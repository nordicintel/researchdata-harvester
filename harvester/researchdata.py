# SPDX-License-Identifier: NOASSERTION
# Copyright (c) 2026 NordicIntel
"""Resolve current dataset-owned metadata and verify bounded tabular resources."""

import asyncio
import csv
import io
import json
import logging
import re
import tempfile
import zipfile
from collections.abc import Callable
from datetime import UTC, datetime
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, cast
from urllib.parse import urljoin, urlsplit

import duckdb
import httpx
from defusedxml.ElementTree import fromstring
from openpyxl.utils.exceptions import InvalidFileException
from pyld.jsonld import JsonLdError
from pyreadstat import ReadstatError
from xlrd.biffh import XLRDError

from harvester.dataportalen import DCT
from harvester.runtime import Config, Http, safe_url
from harvester.vocabulary import IDENTITY_PREDICATES, CacheFetchError, RdfCache, SemanticResolver

logger = logging.getLogger(__name__)
FILE_SUFFIXES = {".csv", ".tsv", ".xlsx", ".xls", ".sav", ".dta", ".sas7bdat", ".parquet"}
SKIP = {
    "citation",
    "references",
    "isBasedOn",
    "isVersionOf",
    "hasVersion",
    "related_identifiers",
    "relatedIdentifiers",
    "versions",
}


def retry_reason(reason: str | None) -> bool:
    return reason in {
        "HTTP 429",
        "HTTP 500",
        "HTTP 502",
        "HTTP 503",
        "HTTP 504",
        "ReadTimeout",
        "ConnectTimeout",
        "ConnectError",
        "ReadError",
        "RemoteProtocolError",
        "PoolTimeout",
    }


def now() -> str:
    return datetime.now(UTC).isoformat()


def local(name: str) -> str:
    return name.rsplit("#", 1)[-1].rsplit("/", 1)[-1].rsplit("}", 1)[-1]


def identity(value: str) -> str:
    value = value.strip()
    match = re.fullmatch(r"(?:https?://(?:dx\.)?doi.org/|doi:\s*)?(10\.\d{4,9}/\S+)", value, re.I)
    return match[1].lower() if match else value


def objects(value: Any) -> list[dict[str, Any]]:
    if isinstance(value, dict):
        found = [value]
        for key, child in value.items():
            if local(key) not in SKIP:
                found.extend(objects(child))
        return found
    if isinstance(value, list):
        return [node for child in value for node in objects(child)]
    return []


def scalar(value: Any) -> list[str]:
    if isinstance(value, str):
        return [value]
    if isinstance(value, list):
        return [item for child in value for item in scalar(child)]
    if isinstance(value, dict):
        return scalar(value.get("@id", value.get("@value", value.get("value", []))))
    return []


def urls(document: Any, fields: set[str]) -> set[str]:
    return {
        url
        for node in objects(document)
        for key, value in node.items()
        if local(key) in fields
        for url in scalar(value)
        if url.startswith(("https://", "http://"))
    }


class Landing(HTMLParser):
    """Read structured metadata and explicitly advertised representations only."""

    def __init__(self) -> None:
        super().__init__()
        self.documents: list[Any] = []
        self.meta: list[dict[str, str]] = []
        self.links: list[str] = []
        self.buffer: list[str] | None = None
        self.acknowledgement: str | None = None

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        fields = {key: value or "" for key, value in attrs}
        if tag == "script" and fields.get("type") == "application/ld+json":
            self.buffer = []
        if tag == "meta" and "content" in fields:
            self.meta.append(fields)
        if tag == "link" and (
            fields.get("rel") == "describedby"
            or (
                fields.get("rel") == "alternate"
                and any(kind in fields.get("type", "") for kind in ("json", "xml", "rdf", "turtle"))
            )
        ):
            self.links.append(fields.get("href", ""))
        if tag == "a" and fields.get("id") == "downloadBtn" and fields.get("href", "").startswith("/licence_accept?"):
            self.acknowledgement = fields["href"]
        if tag == "a" and "/export/" in fields.get("href", ""):
            self.links.append(fields["href"])

    def handle_data(self, data: str) -> None:
        if self.buffer is not None:
            self.buffer.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag == "script" and self.buffer is not None:
            self.documents.append(json.loads("".join(self.buffer)))
            self.buffer = None


def xml_json(element: Any) -> dict[str, Any]:
    return {
        "tag": element.tag,
        "attributes": dict(element.attrib),
        "text": element.text,
        "tail": element.tail,
        "children": [xml_json(child) for child in element],
    }


def decode(response: httpx.Response) -> dict[str, Any]:
    content_type = response.headers.get("content-type", "").lower()
    if "json" in content_type:
        document = response.json()
        is_observation = isinstance(document, list) and not any(
            isinstance(node, dict) and "@type" in node for node in document[:10]
        )
        is_observation = is_observation or (
            isinstance(document, dict)
            and any(isinstance(document.get(key), list) for key in ("observations", "rows", "data"))
            and not any(key in document for key in ("metadata", "@context", "@type"))
        )
        return {"document": {"downloadURL": str(response.url)} if is_observation else document, "links": []}
    if any(
        kind in content_type
        for kind in ("text/csv", "tab-separated", "octet-stream", "spreadsheet", "excel", "parquet")
    ):
        return {"document": {"downloadURL": str(response.url)}, "links": []}
    if "xml" in content_type or response.text.lstrip().startswith("<?xml"):
        root = fromstring(response.content)
        errors = [node for node in root.iter() if local(node.tag) == "error"]
        if errors:
            raise ValueError("OAI metadata error: " + str(errors[0].attrib.get("code", "unknown")))
        documents = [json.loads(node.text) for node in root.iter() if local(node.tag) == "jsonLdString" and node.text]
        return {"document": xml_json(root), "json_ld": documents, "links": []}
    parser = Landing()
    parser.feed(response.text)
    return {
        "document": {"json_ld": parser.documents, "meta": parser.meta},
        "links": [urljoin(str(response.url), link) for link in parser.links if link],
    }


def repository_url(url: str) -> str:
    parsed = urlsplit(url)
    if parsed.hostname in {"zenodo.org", "www.zenodo.org", "datarepository.kth.se"}:
        match = re.fullmatch(r"/(?:records?|record)/(\w[\w.-]*)/?", parsed.path)
        if match:
            return f"https://{parsed.hostname}/api/records/{match[1]}"
    match = re.fullmatch(r"/10\.5281/zenodo\.(\d+)", parsed.path, re.I)
    if parsed.hostname in {"doi.org", "dx.doi.org"} and match:
        return f"https://zenodo.org/api/records/{match[1]}"
    match = re.fullmatch(r"/10\.71775/kth\.([\w-]+)", parsed.path, re.I)
    if parsed.hostname in {"doi.org", "dx.doi.org"} and match:
        return f"https://datarepository.kth.se/api/records/{match[1]}"
    return url


def file_urls(document: Any) -> set[str]:
    result = urls(document, {"contentUrl", "downloadURL", "downloadUrl"})
    for node in objects(document):
        files = node.get("files", [])
        if isinstance(files, dict):
            files = list(files.get("entries", {}).values())
        if isinstance(files, list):
            for item in files:
                if isinstance(item, dict):
                    links = item.get("links", {})
                    value = links.get("content", links.get("self"))
                    if isinstance(value, str):
                        result.add(value)
        if node.get("fileName") and isinstance(node.get("accessUrl"), str):
            result.add(node["accessUrl"])
    return {value for value in result if value.startswith(("https://", "http://"))}


def table_sample(data: bytes, url: str, media: str, config: Config) -> tuple[str, list[str], list[list[Any]]]:
    suffix = Path(urlsplit(url).path.removesuffix(".gz")).suffix.lower()
    count = config.verification_rows
    if suffix == ".xls" and not data.startswith(b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1"):
        return table_sample(data, "advertised-table.tsv", "text/tab-separated-values", config)
    if suffix in {".csv", ".tsv"} or media in {"text/csv", "text/tab-separated-values"}:
        try:
            text = data.decode("utf-16" if data.startswith((b"\xff\xfe", b"\xfe\xff")) else "utf-8-sig")
        except UnicodeDecodeError:
            text = data.decode("cp1252")
        if text.lstrip().startswith(("<", "{")):
            raise ValueError("Response is not a delimited observation table")
        if "\n####\n" in text.replace("\r\n", "\n"):
            text = text.replace("\r\n", "\n").split("\n####\n", 1)[1]
        dialect = csv.Sniffer().sniff("\n".join(text.splitlines()[:6]), delimiters=",;\t|")
        reader = csv.reader(io.StringIO(text), dialect)
        header = next(reader, [])
        rows = []
        for row in reader:
            if row and len(row) == len(header):
                rows.append(row)
            if len(rows) >= count:
                break
        if len(header) < 1 or not all(header) or len(set(header)) != len(header):
            raise ValueError("Missing or duplicate table column names")
        if {name.lower() for name in header} <= {"id", "title", "description", "url", "format", "name"}:
            raise ValueError("Catalog-style columns do not establish observations")
        return "tsv" if dialect.delimiter == "\t" else "csv", header, rows
    if suffix == ".json" or "json" in media:
        value = json.loads(data)
        if isinstance(value, dict):
            if "@context" in value or value.get("@type") or "distribution" in value or "metadata" in value:
                raise ValueError("Metadata JSON is not an observation table")
            value = value.get("observations", value.get("data", value.get("rows")))
        if not isinstance(value, list) or not value or not all(isinstance(row, dict) for row in value[:count]):
            raise ValueError("JSON has no explicit observation rows")
        header = list(value[0])
        if not header or set(header) <= {"id", "title", "description", "url", "name", "format"}:
            raise ValueError("JSON catalog entries are not observations")
        if any(isinstance(v, (dict, list)) for row in value[:count] for v in row.values()):
            raise ValueError("Nested JSON requires a source-specific table description")
        return "json", header, [[row.get(key) for key in header] for row in value[:count]]
    if suffix in {".xlsx", ".xls", ".sav", ".dta", ".sas7bdat", ".parquet"}:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / ("sample" + suffix)
            path.write_bytes(data)
            if suffix == ".xlsx":
                import openpyxl

                with zipfile.ZipFile(path) as archive:
                    if sum(item.file_size for item in archive.infolist()) > config.verification_bytes:
                        raise ValueError("Workbook decompressed_byte_limit")
                workbook = openpyxl.load_workbook(path, read_only=True, data_only=True)
                try:
                    for sheet in workbook:
                        sheet_rows = list(sheet.iter_rows(max_row=count + 1, values_only=True))
                        if len(sheet_rows) > 1:
                            return "xlsx", [str(x) for x in sheet_rows[0]], [list(row) for row in sheet_rows[1:]]
                finally:
                    workbook.close()
            elif suffix == ".xls":
                import xlrd

                workbook_xls = xlrd.open_workbook(str(path), on_demand=True)
                try:
                    for sheet_xls in workbook_xls.sheets():
                        if sheet_xls.nrows > 1:
                            return (
                                "xls",
                                [str(x) for x in sheet_xls.row_values(0)],
                                [list(sheet_xls.row_values(i)) for i in range(1, min(sheet_xls.nrows, count + 1))],
                            )
                finally:
                    workbook_xls.release_resources()
            elif suffix == ".parquet":
                with duckdb.connect() as connection:
                    result = connection.execute("SELECT * FROM read_parquet(?) LIMIT ?", [str(path), count])
                    return (
                        "parquet",
                        [column[0] for column in result.description],
                        [list(row) for row in result.fetchall()],
                    )
            else:
                import pyreadstat

                read = cast(
                    Callable[..., Any],
                    {
                        ".sav": pyreadstat.read_sav,
                        ".dta": pyreadstat.read_dta,
                        ".sas7bdat": pyreadstat.read_sas7bdat,
                    }[suffix],
                )
                frame, meta = read(str(path), row_limit=count, output_format="dict")
                names = list(frame)
                return suffix[1:], names, [list(row) for row in zip(*(frame[name] for name in names), strict=True)]
    raise ValueError("Unsupported or inconclusive tabular format")


def column_type(values: list[Any]) -> str:
    """Infer a conservative type from inspected nonempty values, without retaining them."""
    values = [value for value in values if value is not None and value != ""]
    if not values:
        return "unknown"
    if all(isinstance(value, bool) for value in values):
        return "boolean"
    if all(re.fullmatch(r"[+-]?\d+", str(value)) for value in values):
        return "integer"
    try:
        for value in values:
            float(value)
        return "number"
    except (ValueError, TypeError):
        return "string"


async def verify(http: Http, url: str, config: Config, *, filename: str | None = None) -> dict[str, Any]:
    """Return evidence, never observation values, for one bounded resource."""
    result: dict[str, Any] = {
        "url": url,
        "source_url": url,
        "entity_id": None,
        "distribution_id": None,
        "filename": filename or Path(urlsplit(url).path).name,
        "outcome": "unresolved",
        "reason": None,
        "checked_at": now(),
        "verification": None,
    }
    if Path(filename or urlsplit(url).path).suffix.lower() in {
        ".zip",
        ".tar",
        ".rar",
        ".7z",
        ".pdf",
        ".jpg",
        ".png",
        ".tif",
        ".shp",
    }:
        result["reason"] = "Unsupported archive, document or geospatial resource"
        return result
    try:
        data, resolved, media, transferred = await http.sample(url)
        acknowledgement = None
        if "html" in media and urlsplit(resolved).hostname in {"data.fieldsites.se", "data.icos-cp.eu"}:
            landing = Landing()
            landing.feed(data.decode("utf-8"))
            if landing.acknowledgement:
                acknowledgement = urljoin(resolved, landing.acknowledgement)
                data, resolved, media, extra = await http.sample(acknowledgement)
                transferred += extra
                if transferred > config.verification_bytes:
                    raise ValueError("transferred_byte_limit")
        format_name, columns, rows = await asyncio.to_thread(table_sample, data, filename or resolved, media, config)
        if not rows or not columns:
            raise ValueError("Table contains no observations")
        result.update(
            outcome="verified",
            verification={
                "verified_at": result["checked_at"],
                "resolved_url": resolved,
                "format": format_name,
                "media_type": media,
                "access_method": "licence_acknowledgement" if acknowledgement else "anonymous",
                "sampled_rows": len(rows),
                "columns": [
                    {"name": name, "data_type": column_type([row[index] for row in rows])}
                    for index, name in enumerate(columns)
                ],
                "transferred_bytes": transferred,
                "decompressed_bytes": len(data),
                "license_acknowledgement_url": acknowledgement,
            },
        )
    except httpx.HTTPError as error:
        result.update(
            outcome="unavailable",
            reason=f"HTTP {error.response.status_code}"
            if isinstance(error, httpx.HTTPStatusError)
            else type(error).__name__,
        )
    except (
        ValueError,
        UnicodeError,
        csv.Error,
        zipfile.BadZipFile,
        XLRDError,
        InvalidFileException,
        ReadstatError,
        duckdb.Error,
    ) as error:
        result["reason"] = str(error)
    logger.debug("verification url=%s outcome=%s reason=%s", safe_url(url), result["outcome"], result["reason"])
    return result


async def harvest(
    http: Http,
    config: Config,
    record: dict[str, Any],
    cache: RdfCache,
    cache_path: Path,
    updates_path: Path,
    *,
    full: bool = False,
    resolver: SemanticResolver | None = None,
) -> dict[str, Any]:
    """Resolve advertised metadata and files for one current registry dataset."""
    documents = list(record["documents"])
    roots = record["metadata"]
    uri = record["dataset_uri"]
    pending = set().union(*(urls(doc["metadata"], {"landingPage", "accessURL"}) for doc in documents))
    asserted = {identity(uri)}
    for predicate in {DCT + "identifier", *IDENTITY_PREDICATES}:
        asserted.update(identity(term["value"]) for term in roots.get(uri, {}).get(predicate, []))
    pending.update("https://doi.org/" + value for value in asserted if re.fullmatch(r"10\.\d{4,9}/\S+", value))
    # A description citation needs matching identity evidence, not just a dataset label.
    for text in (term["value"] for term in roots.get(uri, {}).get(DCT + "description", [])):
        for line in text.splitlines():
            if "[Data set]" in line or "[Dataset]" in line:
                pending.update(
                    url.rstrip(".,)")
                    for url in re.findall(r"https?://[^\s<>]+", line)
                    if identity(url.rstrip(".,)")) in asserted
                )
    files = set().union(*(file_urls(doc["metadata"]) for doc in documents))
    for term in roots.get(uri, {}).get(DCT + "identifier", []):
        if term["value"].startswith(("https://doi.org/", "http://doi.org/")):
            pending.add(term["value"])
    seen: set[str] = set()
    sources: list[dict[str, Any]] = []
    associations: list[dict[str, Any]] = []
    retry = False
    while pending:
        original = sorted(pending)[0]
        pending.remove(original)
        target = repository_url(original)
        if target in seen:
            continue
        seen.add(target)
        if Path(urlsplit(target).path.removesuffix(".gz")).suffix.lower() in FILE_SUFFIXES:
            files.add(target)
            continue
        entry = await cache.fetch(
            target,
            http,
            decoder=decode,
            accept="application/ld+json, application/json, text/html;q=0.9",
            required=target.startswith(config.oai_url)
            or (urlsplit(target).hostname == "researchdata.se" and "/export/" in target),
        )
        sources.append({"url": target, "outcome": entry.outcome, "stale": entry.stale, "reason": entry.error})
        if entry.outcome != "available":
            retry = retry or retry_reason(entry.error)
        if not isinstance(entry.data, dict):
            continue
        decoded = cast(dict[str, Any], entry.data)
        source_url = entry.resolved_url or target
        document = decoded.get("document", {})
        documents.append({"url": target, "metadata": document, "json": True})
        files.update(file_urls(document))
        redirected = repository_url(source_url)
        if redirected != source_url:
            pending.add(redirected)
        pending.update(str(link) for link in decoded.get("links", []) if str(link) != source_url)
        # Identity links on the dataset itself are allowed, nested citations are not.
        for node in objects(document):
            if "Dataset" in str(node.get("@type", "")):
                pending.update(urls({key: node[key] for key in ("sameAs", "url") if key in node}, {"sameAs", "url"}))
        if urlsplit(source_url).hostname in {"researchdata.se", "www.researchdata.se"} and "/export/" not in source_url:
            html_key = source_url + "#landing-html"
            html_entry = await cache.fetch(
                html_key, http, source_url=source_url, decoder=decode, accept="text/html", required=True
            )
            seen.add(html_key)
            if isinstance(html_entry.data, dict):
                html_document = cast(dict[str, Any], html_entry.data)
                documents.append({"url": source_url, "metadata": html_document["document"], "json": True})
                for link in html_document.get("links", []):
                    match = re.fullmatch(r"/catalogue/dataset/([^/]+)/([^/]+)/export/([^/]+)", urlsplit(link).path)
                    if match:
                        identifier = f"oai:researchdata.se:{match[1]}/{match[2]}"
                        if match[3] in {"datacite", "dcat", "json_ld"}:
                            target_record = str(
                                http.build_request(
                                    "GET",
                                    config.oai_url,
                                    params={"verb": "GetRecord", "identifier": identifier, "metadataPrefix": match[3]},
                                ).url
                            )
                            pending.add(target_record)
                        else:
                            pending.add(link)
                files.update(file_urls(html_document["document"]))
            ids = re.findall(r"oai:[^\s\"<>]+", json.dumps(document))
            if not ids:
                ids = [source_url]
            for identifier in sorted(set(ids)):
                associations.append(
                    {
                        "identifier": identifier,
                        "landing_page": source_url,
                        "rule": "explicit_landing_page",
                        "evidence": [
                            {
                                "value": original,
                                "value_type": "uri",
                                "language": None,
                                "datatype": None,
                                "source_url": record["metadata_url"],
                                "entity_id": uri,
                                "graph_id": None,
                                "property": "http://www.w3.org/ns/dcat#landingPage",
                                "path": None,
                            }
                        ],
                    }
                )
        structured = decoded.get("json_ld", [])
        if isinstance(document, dict):
            structured = [*structured, *document.get("json_ld", [])] if isinstance(structured, list) else []
            if any(key in document for key in ("@context", "$schema", "openapi")):
                structured.append(document)
        for item in structured:
            try:
                if resolver is not None:
                    resolved = await resolver.resolve(item, source_url=source_url, required=False)
                else:
                    async with SemanticResolver(
                        cache_path, http, updates_path, cache=cache, full=full
                    ) as local_resolver:
                        resolved = await local_resolver.resolve(item, source_url=source_url)
                if resolved.schemas:
                    documents.append(
                        {
                            "url": source_url,
                            "metadata": {"schemas": resolved.schemas, "references": resolved.schema_references},
                            "json": True,
                        }
                    )
                documents.append({"url": source_url, "metadata": item, "properties": resolved.properties, "json": True})
            except (ValueError, CacheFetchError, JsonLdError) as error:
                sources.append(
                    {"url": source_url, "outcome": "unavailable", "stale": False, "reason": type(error).__name__}
                )
                retry = retry or any(
                    marker in str(error)
                    for marker in (
                        "Timeout",
                        "ConnectError",
                        "HTTP 429",
                        "HTTP 500",
                        "HTTP 502",
                        "HTTP 503",
                        "HTTP 504",
                    )
                )
    names: dict[str, str] = {}
    for document in documents:
        for node in objects(document["metadata"]):
            name = node.get("fileName", node.get("key", node.get("name")))
            if isinstance(name, str):
                for address in urls(node, {"accessUrl", "contentUrl", "downloadURL", "content"}):
                    names[address] = name
    resources = []
    eligible = False
    for url in sorted(
        files,
        key=lambda value: (Path(names.get(value, urlsplit(value).path)).suffix.lower() not in {".csv", ".tsv"}, value),
    ):
        if eligible:
            resource = {
                "url": url,
                "source_url": None,
                "entity_id": None,
                "distribution_id": None,
                "filename": names.get(url, Path(urlsplit(url).path).name),
                "outcome": "unresolved",
                "reason": "Not sampled; another resource established eligibility",
                "checked_at": None,
                "verification": None,
            }
        else:
            resource = await verify(http, url, config, filename=names.get(url))
            eligible = resource["outcome"] == "verified"
        resource["source_url"] = next(
            (document["url"] for document in documents if url in file_urls(document["metadata"])),
            record["metadata_url"],
        )
        resources.append(resource)
    eligible = any(resource["outcome"] == "verified" for resource in resources)
    retry = retry or any(
        resource["outcome"] == "unavailable" and retry_reason(resource["reason"]) for resource in resources
    )
    return {
        "documents": documents,
        "resources": resources,
        "researchdata": associations,
        "sources": sources,
        "outcome": "eligible" if eligible else ("unavailable" if retry else "unresolved"),
        "retry": retry,
        "reason": None if eligible else "No verified tabular resource",
        "cache_references": sorted(seen | set(record.get("cache_references", []))),
    }
