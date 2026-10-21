# SPDX-License-Identifier: NOASSERTION
# Copyright (c) 2026 NordicIntel
"""Harvest three complete metadata streams and reconcile current catalog membership."""

from __future__ import annotations

import json
import re
from copy import deepcopy
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any
from urllib.parse import urljoin, urlsplit

from defusedxml.ElementTree import fromstring

from harvester.metadata import (
    JSONLDParser,
    extract_evidence,
    metadata_summary,
    walk_xml,
)
from harvester.oai import OAIClient, OAIError, Record, local_name

if TYPE_CHECKING:
    from harvester.config import Config
    from harvester.transport import Transport

FORMATS = ("datacite", "dcat", "json_ld")
OAI_PREFIX = "oai:researchdata.se:"


def dataset_id(identifier: str) -> str:
    """Return the dataset base without changing the exact current identifier."""
    suffix = identifier.removeprefix(OAI_PREFIX)
    return suffix.rsplit("/", 1)[0] if "/" in suffix else suffix


def make_record(
    identifier: str,
    datestamp: str,
    sets: list[str],
    representations: dict[str, dict[str, Any] | None],
) -> dict[str, Any]:
    """Build a JSON record without treating cited works as dataset identity."""
    trees = deepcopy(representations)
    for tree in trees.values():
        if tree is not None:
            for path, node in walk_xml(tree):
                if (
                    path[-1] == "jsonLdString"
                    and node.get("text")
                    and "json" not in node
                ):
                    node["json"] = json.loads(node["text"])
    identities = []
    for prefix, tree in trees.items():
        for item in extract_evidence(tree):
            evidence = dict(item, format=prefix)
            if evidence not in identities:
                identities.append(evidence)
    return {
        "identifier": identifier,
        "dataset_id": dataset_id(identifier),
        "datestamp": datestamp,
        "sets": sorted(set(sets)),
        "representations": trees,
        "identities": identities,
        "summary": metadata_summary(list(trees.values())),
    }


async def _collect(oai: OAIClient, verb: str, **options: Any) -> list[Any]:
    """Restart expired cursors from scratch; never combine different inventories."""
    for attempt in range(3):
        items = []
        expected = None
        try:
            async for page in oai.iter_pages(verb, **options):
                items.extend(page.items)
                if page.total is not None:
                    if expected is not None and expected != page.total:
                        raise ValueError(
                            "OAI inventory size changed during pagination; retry the harvest."
                        )
                    expected = page.total
            identifiers = [
                item.identifier if isinstance(item, Record) else _header(item)[0]
                for item in items
            ]
            if len(set(identifiers)) != len(identifiers):
                raise ValueError(
                    "OAI scan repeated a record; retry the harvest against a consistent source."
                )
            if expected is not None and expected != len(items):
                raise ValueError(
                    f"OAI scan returned {len(items)} records but advertised {expected}; scan is incomplete."
                )
            return items
        except OAIError as error:
            if error.code != "badResumptionToken" or attempt == 2:
                raise
    raise AssertionError("Unreachable OAI restart state.")


def _header(tree: dict[str, Any]) -> tuple[str, str, list[str], bool]:
    children = tree.get("children", [])
    fields = {
        local_name(child["tag"]): (child.get("text") or "").strip()
        for child in children
    }
    if not fields.get("identifier") or not fields.get("datestamp"):
        raise ValueError(
            "OAI inventory has an incomplete header; membership cannot be reconciled."
        )
    return (
        fields["identifier"],
        fields["datestamp"],
        [
            child.get("text") or ""
            for child in children
            if local_name(child["tag"]) == "setSpec"
        ],
        tree.get("attributes", {}).get("status") == "deleted",
    )


async def _sitemap(
    transport: Transport, url: str
) -> tuple[dict[str, str], dict[str, int]]:
    queue, seen, entries = [url], set(), {}
    counts = {"dataset": 0, "collection": 0, "other": 0}
    locations = set()
    while queue:
        current = queue.pop()
        if current in seen:
            continue
        seen.add(current)
        if len(seen) > 10000:
            raise ValueError(
                "Sitemap exceeds 10,000 documents; membership is incomplete."
            )
        response = await transport.request("GET", current)
        root = fromstring(response.content)
        kind = local_name(root.tag)
        if kind not in {"sitemapindex", "urlset"}:
            raise ValueError(
                "Expected sitemapindex or urlset; membership is incomplete."
            )
        for item in root:
            location = next(
                (child.text for child in item if local_name(child.tag) == "loc"), None
            )
            if not location or urlsplit(location).scheme not in {"http", "https"}:
                raise ValueError(
                    "Sitemap entry lacks an HTTP location; membership is incomplete."
                )
            if kind == "sitemapindex":
                if urlsplit(location).netloc != urlsplit(url).netloc:
                    raise ValueError(
                        "Cross-host sitemap index requires explicit source configuration."
                    )
                queue.append(location)
            elif location not in locations:
                locations.add(location)
                path = urlsplit(location).path.rstrip("/")
                category = (
                    "dataset"
                    if "/dataset/" in path
                    else "collection"
                    if "/collection/" in path
                    else "other"
                )
                counts[category] += 1
                if category == "dataset":
                    identifier = path.rsplit("/", 1)[-1]
                    entries[identifier] = location
    return entries, counts


async def _landing_identifier(transport: Transport, url: str, base: str) -> str:
    response = await transport.request("GET", url)
    parser = JSONLDParser()
    parser.feed(response.text)
    identifiers = set(
        re.findall(
            re.escape(OAI_PREFIX + base) + r"/[A-Za-z0-9._-]+",
            json.dumps(parser.documents),
        )
    )
    for href in parser.links:
        target = urlsplit(urljoin(str(response.url), href))
        if target.hostname != urlsplit(url).hostname:
            continue
        match = re.fullmatch(
            r"/catalogue/dataset/"
            + re.escape(base)
            + r"/([A-Za-z0-9._-]+)/export/[A-Za-z0-9_]+/?",
            target.path,
        )
        if match:
            identifiers.add(OAI_PREFIX + base + "/" + match[1])
    if len(identifiers) != 1:
        raise ValueError(
            f"Sitemap dataset {base} has no unambiguous current OAI identifier; membership is incomplete."
        )
    return identifiers.pop()


async def harvest_researchdata(
    transport: Transport,
    config: Config,
    previous: dict[str, dict[str, Any]],
    boundary: str | None,
    *,
    full: bool = False,
) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    """Return complete current records only after both membership scans succeed.

    The boundary is the previous successful sync date. A one-day overlap covers
    source date granularity. The caller advances its boundary only after commit.
    No previous record is modified, including when any source request fails.
    """
    oai = OAIClient(transport, str(config.oai_url))
    from_date = (
        (datetime.fromisoformat(boundary).date() - timedelta(days=1)).isoformat()
        if boundary and not full
        else None
    )
    streams: dict[str, dict[str, Record]] = {}
    for prefix in FORMATS:
        rows = await _collect(
            oai, "ListRecords", metadata_prefix=prefix, from_=from_date
        )
        streams[prefix] = {row.identifier: row for row in rows if not row.deleted}
    headers = [
        _header(tree)
        for tree in await _collect(oai, "ListIdentifiers", metadata_prefix="oai_dc")
    ]
    inventory = {
        identifier: (stamp, sets)
        for identifier, stamp, sets, deleted in headers
        if not deleted
    }
    sitemap, sitemap_counts = await _sitemap(transport, str(config.sitemap_url))
    by_base = {dataset_id(identifier) for identifier in inventory}
    missing = sorted(set(sitemap) - by_base)
    extra = sorted(by_base - set(sitemap))
    recovered = []
    for base in missing:
        identifier = await _landing_identifier(transport, sitemap[base], base)
        record = await oai.get_record(identifier, "datacite")
        if record.deleted:
            raise ValueError(
                f"Sitemap dataset {base} resolves to a deleted OAI record; retry the harvest."
            )
        inventory[identifier] = (record.datestamp, record.sets)
        streams["datacite"][identifier] = record
        recovered.append(identifier)
    if not inventory and (previous or sitemap):
        raise ValueError(
            "Current OAI inventory is unexpectedly empty; refusing to infer removals."
        )
    records = {}
    unavailable = []
    reused = 0
    fetched = 0
    for identifier, (stamp, sets) in sorted(inventory.items()):
        changed = (
            full
            or identifier not in previous
            or previous[identifier].get("datestamp") != stamp
            or any(identifier in stream for stream in streams.values())
        )
        if not changed and all(
            previous[identifier].get("representations", {}).get(prefix) is not None
            for prefix in FORMATS
        ):
            records[identifier] = deepcopy(previous[identifier])
            reused += 1
            continue
        representations = {}
        dates = [stamp]
        record_sets = set(sets)
        for prefix in FORMATS:
            record = streams[prefix].get(identifier)
            if record is None:
                try:
                    record = await oai.get_record(identifier, prefix)
                    fetched += 1
                except OAIError as error:
                    if error.code != "cannotDisseminateFormat":
                        raise
                    representations[prefix] = None
                    unavailable.append(
                        {
                            "identifier": identifier,
                            "format": prefix,
                            "status": "unavailable",
                            "code": error.code,
                        }
                    )
                    continue
            if record.deleted:
                raise ValueError(
                    f"Current inventory record {identifier} was deleted during metadata retrieval; retry the harvest."
                )
            representations[prefix] = record.metadata
            dates.append(record.datestamp)
            record_sets.update(record.sets)
        records[identifier] = make_record(
            identifier, max(dates), sorted(record_sets), representations
        )
    return records, {
        "records": len(records),
        "reused": reused,
        "get_records": fetched,
        "removed": sorted(set(previous) - set(records)),
        "streams": {prefix: len(rows) for prefix, rows in streams.items()},
        "sitemap": sitemap_counts,
        "missing_from_oai": missing,
        "recovered": recovered,
        "missing_from_sitemap": extra,
        "source_outcomes": unavailable,
        "from_date": from_date,
    }
