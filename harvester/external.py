# SPDX-License-Identifier: NOASSERTION
# Copyright (c) 2026 NordicIntel
"""Retrieve dataset-owned external metadata without downloading research files."""

import asyncio
import copy
import hashlib
import json
import re
from collections.abc import Iterator
from html.parser import HTMLParser
from typing import Any
from urllib.parse import unquote, urljoin, urlsplit

import httpx
from rdflib import Graph
from rdflib.compare import to_canonical_graph

PERMANENT = {401, 403, 404, 410}
OWN_HOSTS = {"researchdata.se", "www.researchdata.se", "api.researchdata.se"}
SKIP_FIELDS = {
    "creator",
    "creators",
    "publisher",
    "contactpoint",
    "contributor",
    "citation",
    "references",
    "relatedidentifier",
    "related_identifiers",
    "previousversion",
    "parent",
    "history",
}


def _local(value: str) -> str:
    return value.rsplit("}", 1)[-1].rsplit("/", 1)[-1].rsplit("#", 1)[-1]


def _url(value: Any) -> str | None:
    if isinstance(value, dict):
        value = value.get("@id", value.get("@value", value.get("value")))
    if not isinstance(value, str):
        return None
    value = value.strip()
    if value.lower().startswith("doi:"):
        value = value[4:]
    if re.match(r"^10\.\d{4,9}/", value):
        value = "https://doi.org/" + value
    return value if urlsplit(value).scheme in {"http", "https"} else None


def _resource_url(value: str) -> str:
    """Use verified metadata endpoints rather than repository HTML when possible."""
    parsed = urlsplit(value)
    host, path = (parsed.hostname or "").lower(), unquote(parsed.path)
    if host in {"doi.org", "dx.doi.org"}:
        if match := re.fullmatch(r"/10\.5281/zenodo\.(\d+)", path, re.I):
            return f"https://zenodo.org/api/records/{match[1]}"
        if match := re.fullmatch(r"/10\.71775/kth\.([\w-]+)", path, re.I):
            return f"https://datarepository.kth.se/api/records/{match[1]}"
    if host in {"zenodo.org", "www.zenodo.org", "datarepository.kth.se"}:
        if match := re.fullmatch(r"/(?:api/)?records?/([\w-]+)/?", path):
            return f"https://{host.removeprefix('www.')}/api/records/{match[1]}"
    return value


def _external(value: str) -> bool:
    parsed = urlsplit(value)
    if re.search(
        r"\.(?:zip|gz|tar|csv|tsv|xlsx?|parquet|nc|h5|tiff?|pdf)$", parsed.path, re.I
    ) or re.search(r"/(?:file|files)/.+/(?:content|download)$", parsed.path):
        return False
    if parsed.hostname in OWN_HOSTS:
        return False
    # These source-owned DOI pages repeat the three harvested representations.
    return not (
        parsed.hostname in {"doi.org", "dx.doi.org"}
        and re.match(r"/10\.(5878|5879)/", parsed.path)
    )


class _LandingParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.documents: list[Any] = []
        self.meta: list[dict[str, str | None]] = []
        self.alternates: list[str] = []
        self.buffer: list[str] | None = None

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attributes = dict(attrs)
        if (
            tag == "script"
            and (attributes.get("type") or "").lower() == "application/ld+json"
        ):
            self.buffer = []
        if tag == "meta":
            self.meta.append(attributes)
        if (
            tag == "link"
            and "alternate" in (attributes.get("rel") or "").split()
            and attributes.get("type")
            in {
                "application/ld+json",
                "application/json",
                "application/rdf+xml",
                "text/turtle",
            }
        ):
            if attributes.get("href"):
                self.alternates.append(attributes["href"])

    def handle_data(self, data: str) -> None:
        if self.buffer is not None:
            self.buffer.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag == "script" and self.buffer is not None:
            self.documents.append(json.loads("".join(self.buffer)))
            self.buffer = None


def _documents(value: Any) -> Iterator[dict[str, Any]]:
    if isinstance(value, dict):
        yield value
        for key, child in value.items():
            if key.casefold() in SKIP_FIELDS:
                continue
            if (
                key == "text"
                and isinstance(child, str)
                and _local(str(value.get("tag", ""))) == "jsonLdString"
            ):
                yield from _documents(json.loads(child))
            else:
                yield from _documents(child)
    elif isinstance(value, list):
        for child in value:
            yield from _documents(child)


def _file_metadata(document: Any) -> list[dict]:
    files: dict[str, dict] = {}
    for node in _documents(document):
        url = _url(node.get("contentUrl", node.get("downloadURL")))
        if url:
            files[url] = {
                "url": url,
                "name": node.get("name"),
                "size": node.get("contentSize", node.get("byteSize")),
                "sha256": node.get("sha256"),
                "encoding_format": node.get("encodingFormat"),
                "license": node.get("license"),
                "access_conditions": node.get("conditionsOfAccess"),
                "raw": node,
            }
        repository_files = node.get("files")
        entries = (
            repository_files
            if isinstance(repository_files, list)
            else (
                repository_files.get("entries", {}).values()
                if isinstance(repository_files, dict)
                else []
            )
        )
        for entry in entries:
            links = entry.get("links", {})
            url = _url(links.get("content", links.get("self")))
            if url:
                files[url] = {
                    "url": url,
                    "name": entry.get("key"),
                    "size": entry.get("size"),
                    "checksum": entry.get("checksum"),
                    "encoding_format": entry.get("mimetype"),
                    "raw": entry,
                }
        if node.get("fileName") and (url := _url(node.get("accessUrl"))):
            files[url] = {
                "url": url,
                "name": node["fileName"],
                "size": node.get("size"),
                "checksum": node.get("hash"),
                "raw": node,
            }
    return list(files.values())


def _research_resources(record: dict) -> set[str]:
    urls = set()
    for evidence in record.get("identities", []):
        if evidence.get("relation") in {"dataset", "citation", "related", "history"}:
            continue
        if url := _url(evidence.get("value")):
            urls.add(url)
    for node in _documents(record.get("representations", {})):
        kind = node.get("@type", "")
        if "Dataset" in str(kind):
            for field in ("url", "sameAs", "mainEntityOfPage"):
                values = node.get(field, [])
                for value in values if isinstance(values, list) else [values]:
                    if url := _url(
                        value.get("@id") if isinstance(value, dict) else value
                    ):
                        urls.add(url)
        if "DataDownload" in str(kind) or "Distribution" in str(kind):
            for field in ("accessURL", "accessUrl"):
                if url := _url(node.get(field)):
                    urls.add(url)
    return {_resource_url(url) for url in urls if _external(url)}


def _dataportalen_resources(record: dict) -> tuple[set[str], list[dict]]:
    urls, files = set(), []
    graph = record.get("metadata", {})
    nodes, root = graph.get("nodes", {}), graph.get("root", record.get("uri"))
    owned = {root}
    for predicate, terms in nodes.get(root, {}).items():
        if _local(predicate) == "distribution":
            owned.update(term["value"] for term in terms)
    for subject in owned:
        for predicate, terms in nodes.get(subject, {}).items():
            field = _local(predicate)
            for term in terms:
                url = _url(term.get("value"))
                if url and field == "downloadURL":
                    files.append({"url": url, "raw": nodes[subject]})
                elif url and field in {"landingPage", "accessURL"} and _external(url):
                    urls.add(_resource_url(url))
    return urls, files


def _parse_response(response: httpx.Response) -> tuple[Any, list[str]]:
    content_type = response.headers.get("content-type", "").split(";", 1)[0]
    if "json" in content_type:
        return response.json(), []
    if content_type in {"application/rdf+xml", "text/turtle", "application/n-triples"}:
        graph = Graph().parse(
            data=response.text,
            format={
                "application/rdf+xml": "xml",
                "text/turtle": "turtle",
                "application/n-triples": "nt",
            }[content_type],
        )
        return json.loads(to_canonical_graph(graph).serialize(format="json-ld")), []
    parser = _LandingParser()
    parser.feed(response.text)
    return {"json_ld": parser.documents, "meta": parser.meta}, [
        urljoin(str(response.url), url) for url in parser.alternates
    ]


async def enrich_records(
    transport: Any,
    research: dict,
    dataportalen: dict,
    links: list,
    previous: list[dict],
) -> tuple[list[dict], list[dict]]:
    """Revalidate each linked metadata resource and retain its complete parsed payload.

    Permanent access failures are explicit outcomes. Retryable network and source
    failures propagate so callers cannot commit a falsely complete harvest.
    """
    previous_by_url = {entry["url"]: entry for entry in previous}
    owners: dict[str, set[str]] = {}
    files: dict[tuple[str, str], dict] = {}
    tasks: dict[str, asyncio.Task] = {}

    def add_files(identifier: str, entries: list[dict]) -> None:
        for entry in entries:
            files[(identifier, entry["url"])] = {
                "identifier": identifier,
                "url": entry["url"],
                "metadata": copy.deepcopy(entry),
                "status": "discovered",
            }

    for identifier, record in research.items():
        add_files(identifier, _file_metadata(record.get("representations", {})))
        for url in _research_resources(record):
            owners.setdefault(url, set()).add(identifier)
    for link in links:
        identifier = link.get("identifier")
        if identifier not in research or link.get("uri") not in dataportalen:
            continue
        urls, entries = _dataportalen_resources(dataportalen[link["uri"]])
        add_files(identifier, entries)
        for url in urls:
            owners.setdefault(url, set()).add(identifier)

    async def fetch(url: str) -> tuple[dict, list[str]]:
        old = previous_by_url.get(url, {})
        validators = old.get("validators", {})
        headers = {"Accept": "application/json, application/ld+json, text/html;q=0.8"}
        if validators.get("etag"):
            headers["If-None-Match"] = validators["etag"]
        if validators.get("last_modified"):
            headers["If-Modified-Since"] = validators["last_modified"]
        response = await transport.request(
            "GET", url, headers=headers, allowed_statuses={304, *PERMANENT}
        )
        if response.status_code == 304:
            if not old or old.get("status") != "available":
                raise ValueError(
                    f"Received HTTP 304 without reusable metadata for {url}."
                )
            return copy.deepcopy(old), list(old.get("related_metadata", []))
        if response.status_code in PERMANENT:
            return {
                "url": url,
                "final_url": str(response.url),
                "status": "restricted"
                if response.status_code in {401, 403}
                else "unavailable",
                "http_status": response.status_code,
                "metadata": None,
                "validators": {},
            }, []
        response.raise_for_status()
        if urlsplit(str(response.url)).hostname in OWN_HOSTS:
            return {
                "url": url,
                "final_url": str(response.url),
                "status": "represented_by_researchdata",
                "metadata": None,
                "validators": {},
            }, []
        digest = hashlib.sha256(response.content).hexdigest()
        if old.get("sha256") == digest and old.get("status") == "available":
            return copy.deepcopy(old), list(old.get("related_metadata", []))
        metadata, related = _parse_response(response)
        endpoint = _resource_url(str(response.url))
        if endpoint != str(response.url):
            related.append(endpoint)
        related = sorted(set(value for value in related if value != url))
        entry = {
            "url": url,
            "final_url": str(response.url),
            "status": "available",
            "metadata": metadata,
            "metadata_status": (
                "no_structured_metadata"
                if isinstance(metadata, dict)
                and set(metadata) == {"json_ld", "meta"}
                and not metadata["json_ld"]
                and not metadata["meta"]
                else "available"
            ),
            "validators": {
                key: response.headers[header]
                for key, header in (
                    ("etag", "etag"),
                    ("last_modified", "last-modified"),
                )
                if header in response.headers
            },
            "related_metadata": related,
            "sha256": digest,
        }
        return entry, related

    external: dict[str, dict] = {}
    pending = set(owners)
    while pending:
        batch = sorted(pending)
        pending = set()
        for url in batch:
            tasks.setdefault(url, asyncio.create_task(fetch(url)))
        for url, result in zip(
            batch, await asyncio.gather(*(tasks[url] for url in batch)), strict=True
        ):
            entry, related = result
            external[url] = entry
            for target in related:
                owners.setdefault(target, set()).update(owners[url])
                if target not in external and target not in batch:
                    pending.add(target)
    # Propagate ownership across shared resources after all dependencies resolve.
    changed = True
    while changed:
        changed = False
        for url, entry in external.items():
            for target in entry.get("related_metadata", []):
                before = len(owners[target])
                owners[target].update(owners[url])
                changed |= len(owners[target]) != before
    for url, entry in external.items():
        entry["identifiers"] = sorted(owners[url])
        if entry["status"] == "available":
            for identifier in entry["identifiers"]:
                add_files(identifier, _file_metadata(entry["metadata"]))
    return [external[url] for url in sorted(external)], [
        files[key] for key in sorted(files)
    ]
