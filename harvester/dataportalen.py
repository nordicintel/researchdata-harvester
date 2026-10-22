# SPDX-License-Identifier: NOASSERTION
# Copyright (c) 2026 NordicIntel
"""Scoped EntryStore inventory and complete dataset-owned metadata retrieval."""

import hashlib
import logging
from datetime import datetime
from typing import Any

from harvester.runtime import Config, Http, fingerprint
from harvester.vocabulary import NAMESPACES, RdfCache, normalize_rdf

logger = logging.getLogger(__name__)
DCT, DCAT, RDF, ES = (NAMESPACES[key] for key in ("dct", "dcat", "rdf", "es"))
NON_PUBLIC = "http://publications.europa.eu/resource/authority/access-right/NON_PUBLIC"
LINKS = {DCT + key for key in ("publisher", "creator", "contributor", "spatial", "temporal", "provenance")}
LINKS |= {DCAT + key for key in ("distribution", "contactPoint", "accessService")}
LINKS |= {NAMESPACES["prov"] + key for key in ("qualifiedAttribution", "agent")}


def escape(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"').replace(":", "\\:")


def query(config: Config, *, candidates: bool = True) -> str:
    result = f"public:true AND rdfType:{escape(DCAT + 'Dataset')} AND context:{escape(config.context_url)}"
    if candidates:
        field = hashlib.md5((DCT + "accessRights").encode()).hexdigest()[:8]
        result += f" AND NOT metadata.predicate.uri.{field}:{escape(NON_PUBLIC)}"
    return result


def values(graph: dict[str, Any], subject: str, predicate: str) -> list[str]:
    return [str(term["value"]) for term in graph.get(subject, {}).get(predicate, [])]


def entry_record(entry: dict[str, Any]) -> dict[str, Any]:
    info = entry["info"]
    if len(info) != 1:
        raise ValueError("Registry entry must have exactly one info subject")
    entry_uri = next(iter(info))
    uri = values(info, entry_uri, ES + "resource")[0]
    metadata_url = values(info, entry_uri, ES + "metadata")[0]
    modified = values(info, entry_uri, DCT + "modified")[0]
    timestamp = datetime.fromisoformat(modified.replace("Z", "+00:00"))
    if timestamp.tzinfo is None:
        raise ValueError(f"Registry modified lacks timezone: {entry_uri}")
    graph = entry["metadata"]
    if DCAT + "Dataset" not in values(graph, uri, RDF + "type"):
        raise ValueError(f"Registry entry is not a dataset: {uri}")
    return {
        "dataset_uri": uri,
        "registry_entry_uri": entry_uri,
        "metadata_url": metadata_url,
        "registry_modified": timestamp.isoformat(),
        "registry_modified_value": modified,
        "metadata": graph,
        "fingerprint": fingerprint(normalize_rdf(graph, metadata_url)),
        "access_rights": values(graph, uri, DCT + "accessRights"),
        "outcome": "pending",
        "retry": False,
    }


async def search(http: Http, config: Config, expression: str, *, limit: int | None = None) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    expected: int | None = None
    offset = 0
    seen: set[str] = set()
    last = ""
    while True:
        page_size = min(config.page_size, limit - len(entries)) if limit else config.page_size
        response = await http.get(
            config.search_url,
            params={"type": "solr", "query": expression, "limit": page_size, "offset": offset, "sort": "uri asc"},
        )
        response.raise_for_status()
        body = response.json()
        children = body["resource"]["children"]
        total = body["results"]
        if not isinstance(total, int) or not isinstance(children, list) or body.get("offset", offset) != offset:
            raise ValueError("Malformed inventory page")
        if expected is not None and total != expected:
            raise ValueError("Inventory result count changed during pagination")
        expected = total
        for child in children:
            key = next(iter(child["info"]))
            if key in seen or key <= last:
                raise ValueError("Repeated or out-of-order inventory page")
            seen.add(key)
            last = key
            entries.append(child)
        logger.info("Inventory page offset=%d returned=%d total=%d", offset, len(children), total)
        offset += page_size
        if limit and len(entries) >= limit:
            return entries[:limit]
        if offset >= total:
            if len(entries) != total:
                raise ValueError("Incomplete inventory: count does not match returned entries")
            return entries
        if not children:
            raise ValueError("Incomplete inventory: empty intermediate page")


async def inventory(
    http: Http,
    config: Config,
    previous: dict[str, Any],
    *,
    full: bool = False,
    dataset_uris: list[str] | None = None,
    limit: int | None = None,
) -> dict[str, Any]:
    """Return a complete reconciliation or an explicitly non-publishable sample."""
    old = previous.get("entries", previous)
    scoped = bool(dataset_uris or limit)
    if dataset_uris:
        records: list[dict[str, Any]] = []
        for uri in sorted(set(dataset_uris)):
            found = await search(http, config, query(config) + f' AND resource:"{escape(uri)}"')
            if not found:
                raise ValueError(f"Selected URI is missing or excluded: {uri}")
            records.extend(found)
    else:
        records = await search(http, config, query(config), limit=limit)
    entries = {item["dataset_uri"]: item for item in map(entry_record, records)}
    if len(entries) != len(records):
        raise ValueError("Duplicate dataset identities in registry inventory")
    members = set(entries)
    if not scoped:
        response = await http.get(config.catalog_metadata_url, headers={"Accept": "application/json"})
        response.raise_for_status()
        catalog = response.json()
        if DCAT + "Catalog" not in values(catalog, config.catalog_uri, RDF + "type"):
            raise ValueError("Unexpected catalog identity or type")
        if DCAT + "dataset" not in catalog[config.catalog_uri]:
            raise ValueError("Catalog membership is missing")
        members = set(values(catalog, config.catalog_uri, DCAT + "dataset"))
        if not set(entries) <= members:
            raise ValueError("Search contains datasets absent from explicit membership")
        missing = members - set(entries)
        # One complementary filtered scan establishes initial exclusions without thousands of individual calls.
        uncertain = {uri for uri in missing if old.get(uri, {}).get("reason") != "NON_PUBLIC"}
        if uncertain:
            field = hashlib.md5((DCT + "accessRights").encode()).hexdigest()[:8]
            excluded = await search(
                http,
                config,
                query(config, candidates=False) + f" AND metadata.predicate.uri.{field}:{escape(NON_PUBLIC)}",
            )
            for item in map(entry_record, excluded):
                if item["dataset_uri"] in missing:
                    item.update(outcome="excluded", reason="NON_PUBLIC")
                    entries[item["dataset_uri"]] = item
            if uncertain - set(entries):
                raise ValueError("Incomplete inventory: catalog members are missing without confirmed exclusion")
        for uri in missing - set(entries):
            entries[uri] = old[uri]
    selected: list[str] = []
    for uri, item in entries.items():
        if item["outcome"] == "excluded":
            continue
        before = old.get(uri, {})
        changed = (
            full
            or not before
            or before.get("retry", False)
            or before.get("outcome") == "pending"
            or any(
                item.get(key) != before.get(key)
                for key in ("registry_modified_value", "registry_entry_uri", "fingerprint")
            )
        )
        if changed:
            selected.append(uri)
        else:
            entries[uri] = {**before, "metadata": item["metadata"]}
    removed = sorted(set(old) - members) if not scoped else []
    logger.info(
        "Inventory entries=%d selected=%d removed=%d scoped=%s", len(entries), len(selected), len(removed), scoped
    )
    return {"entries": entries, "selected": sorted(selected), "removed": removed, "complete": not scoped, "full": full}


async def metadata(http: Http, config: Config, record: dict[str, Any], cache: RdfCache) -> dict[str, Any]:
    """Retrieve descriptive entity closure; cache exact lookups shared by records."""
    documents = [{"url": record["metadata_url"], "metadata": record["metadata"]}]
    visited = {record["dataset_uri"]}
    index = 0
    while index < len(documents):
        graph = documents[index]["metadata"]
        index += 1
        for properties in list(graph.values()):
            for predicate, terms in properties.items():
                if predicate not in LINKS:
                    continue
                for term in terms:
                    uri = term["value"]
                    if term["type"] != "uri" or uri in visited or uri in graph:
                        continue
                    visited.add(uri)
                    expression = f'public:true AND resource:"{escape(uri)}"'
                    source_url = str(
                        http.build_request(
                            "GET",
                            config.search_url,
                            params={"type": "solr", "query": expression, "limit": 100, "offset": 0, "sort": "uri asc"},
                        ).url
                    )

                    def decode(response: Any) -> dict[str, Any]:
                        body = response.json()
                        children = body["resource"]["children"]
                        if body["results"] != len(children):
                            raise ValueError("Related entity lookup is incomplete")
                        merged: dict[str, Any] = {}
                        for child in children:
                            for subject, predicates in child["metadata"].items():
                                target = merged.setdefault(subject, {})
                                for pred, objects in predicates.items():
                                    target.setdefault(pred, []).extend(objects)
                        return merged

                    entry = await cache.fetch(uri, http, source_url=source_url, required=True, decoder=decode)
                    if entry.data:
                        documents.append({"url": source_url, "metadata": entry.data})
    for document in documents:
        document["properties"] = normalize_rdf(document["metadata"], document["url"])
    return {**record, "documents": documents, "cache_references": sorted(visited)}
