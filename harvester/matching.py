# SPDX-License-Identifier: NOASSERTION
# Copyright (c) 2026 NordicIntel
"""Pair anonymous source records with verified catalog representations."""

import hashlib
import json
import re
from collections import defaultdict
from dataclasses import asdict, dataclass
from typing import Any
from urllib.parse import urlsplit

from harvester.dataportalen import identity_index, match_identities, metadata_to_triples
from harvester.metadata import (
    json_entities,
    jsonld_documents,
    normalize_identity,
    scalar_values,
    values,
)

DCT = "http://purl.org/dc/terms/"
DCAT = "http://www.w3.org/ns/dcat#"
RDF = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
XML = "http://www.w3.org/XML/1998/namespace"
DATACITE = "http://datacite.org/schema/kernel-4"
MATCHING_POLICY = "anonymous-catalog-representation-v2"


@dataclass(frozen=True, order=True)
class Fact:
    """A lexical RDF statement without a subject or rewritten node identity."""

    predicate: str
    term_type: str
    value: str
    language: str = ""
    datatype: str = ""


@dataclass(frozen=True)
class SourceMetadata:
    identifier: str
    content_hash: str
    facts: frozenset[Fact]
    periods: frozenset[frozenset[Fact]]
    primary_urls: tuple[tuple[str, str], ...]

    def field(self, predicate: str) -> frozenset[Fact]:
        return frozenset(fact for fact in self.facts if fact.predicate == predicate)


def xml_fact(node: dict[str, Any], language: str = "") -> Fact | None:
    if node["children"]:
        return None
    attributes = node["attributes"]
    if "{" + RDF + "}nodeID" in attributes:
        return None
    resource = attributes.get("{" + RDF + "}resource")
    value = resource if resource is not None else node["text"]
    if not value:
        return None
    predicate = node["tag"].removeprefix("{").replace("}", "", 1)
    return Fact(
        predicate,
        "uri" if resource is not None else "literal",
        value,
        "" if resource is not None else attributes.get("{" + XML + "}lang", language),
        "" if resource is not None else attributes.get("{" + RDF + "}datatype", ""),
    )


def triple_fact(triple: dict[str, Any]) -> Fact | None:
    obj = triple["o"]
    if obj["type"] not in {"uri", "literal"} or not obj["value"]:
        return None
    return Fact(
        triple["p"]["value"],
        obj["type"],
        obj["value"],
        obj.get("xml:lang", ""),
        obj.get("datatype", ""),
    )


def primary_urls(
    representations: dict[str, dict[str, Any]],
) -> tuple[tuple[str, str], ...]:
    """Read only the source dataset's own identifiers, never related works."""
    result: set[tuple[str, str]] = set()
    anchors: set[str] = set()
    datacite = representations.get("datacite")
    if datacite and datacite["tag"] == "{" + DATACITE + "}resource":
        for child in datacite["children"]:
            if child["tag"] == "{" + DATACITE + "}identifier" and child["text"]:
                result.add((child["text"], "datacite/resource/identifier"))
                if normalized := normalize_identity(child["text"]):
                    anchors.add(normalized)
    datasets = [
        entity
        for document in jsonld_documents(representations.get("json_ld"))
        for entity in json_entities(document)
        if set(values(entity.get("@type", [])))
        & {"Dataset", "https://schema.org/Dataset", "http://schema.org/Dataset"}
    ]
    if anchors:
        owners = [
            entity
            for entity in datasets
            if anchors
            & {
                normalize_identity(value)
                for key in ("identifier", "@id")
                for value in scalar_values(entity.get(key, []))
            }
        ]
    else:
        owners = datasets if len(datasets) == 1 else []
    if len(owners) == 1:
        result.update(
            (value, "json_ld/Dataset/identifier")
            for value in scalar_values(owners[0].get("identifier", []))
        )
    return tuple(
        sorted(
            (url, field)
            for url, field in result
            if urlsplit(url).scheme in {"http", "https"}
            and urlsplit(url).hostname
            and not any(character.isspace() for character in url)
        )
    )


class MetadataIndex:
    """Index anonymous current DCAT records once by exact language-tagged titles."""

    def __init__(self, records: dict[str, dict[str, Any]]):
        self.by_title: dict[Fact, list[SourceMetadata]] = defaultdict(list)
        for identifier, record in sorted(records.items()):
            representations = record.get("representations", {})
            tree = representations.get("dcat")
            if not tree or tree["tag"] != "{" + RDF + "}RDF":
                continue
            datasets = [
                node
                for node in tree["children"]
                if node["tag"] == "{" + DCAT + "}Dataset"
            ]
            if len(datasets) != 1 or any(
                "{" + RDF + "}" + name in datasets[0]["attributes"]
                for name in ("about", "ID")
            ):
                continue
            dataset = datasets[0]
            language = dataset["attributes"].get(
                "{" + XML + "}lang", tree["attributes"].get("{" + XML + "}lang", "")
            )
            facts = frozenset(
                fact
                for node in dataset["children"]
                if (fact := xml_fact(node, language)) and fact.predicate != RDF + "type"
            )
            periods = frozenset(
                frozenset(
                    fact
                    for node in period["children"]
                    if (fact := xml_fact(node, language))
                )
                for relation in dataset["children"]
                if relation["tag"] == "{" + DCT + "}temporal"
                for period in relation["children"]
                if period["tag"] == "{" + DCT + "}PeriodOfTime"
            )
            source = SourceMetadata(
                identifier,
                fingerprint(tree),
                facts,
                periods,
                primary_urls(representations),
            )
            if not source.field(DCT + "description") or not any(
                fact.term_type == "uri" for fact in source.field(DCT + "publisher")
            ):
                continue
            for title in source.field(DCT + "title"):
                self.by_title[title].append(source)

    def match(
        self, uri: str, triples: list[dict[str, Any]], catalog: dict[str, Any]
    ) -> list[dict[str, Any]]:
        """Return representation evidence only for explicitly proven catalog members."""
        if uri not in catalog["members"]:
            return []
        root = [
            triple for triple in triples if triple["s"] == {"type": "uri", "value": uri}
        ]
        facts = frozenset(
            fact
            for triple in root
            if (fact := triple_fact(triple)) and fact.predicate != RDF + "type"
        )
        periods = frozenset(
            frozenset(
                fact
                for item in triples
                if item["s"] == triple["o"]
                and (fact := triple_fact(item))
                and fact.predicate != RDF + "type"
            )
            for triple in root
            if triple["p"]["value"] == DCT + "temporal"
        )
        candidates = {
            source.identifier: source
            for title in facts
            if title.predicate == DCT + "title"
            for source in self.by_title.get(title, [])
        }
        matches = []
        support: dict[tuple[Any, ...], list[dict[str, Any]]] = defaultdict(list)
        for source in candidates.values():
            dates = (
                source.field(DCT + "issued")
                | source.field(DCT + "modified")
                | source.field(DCT + "date")
            )
            if dates and source.facts <= facts and source.periods <= periods:
                rule = "exact_dcat_fields"
                compared = {}
                key = (rule, source.facts, source.periods)
            else:
                citation = (
                    self_citation(source, facts)
                    if dates
                    and all(
                        fact in facts
                        for fact in source.facts
                        if fact.predicate != DCT + "description"
                    )
                    and source.periods <= periods
                    else None
                )
                if citation is None:
                    continue
                rule = "corroborated_self_citation"
                compared = citation
                key = (
                    rule,
                    frozenset(
                        fact
                        for fact in source.facts
                        if fact.predicate != DCT + "description"
                    ),
                    source.periods,
                    citation["primary_url"],
                )
            compared["facts"] = [
                asdict(fact)
                for fact in sorted(source.facts)
                if rule == "exact_dcat_fields" or fact.predicate != DCT + "description"
            ]
            compared["periods"] = [
                [asdict(fact) for fact in sorted(period)]
                for period in sorted(source.periods, key=lambda period: sorted(period))
            ]
            match = {
                "identifier": source.identifier,
                "rule": rule,
                "relation": "catalog_representation",
                "status": "matched",
                "evidence": {
                    "matching_policy": MATCHING_POLICY,
                    "source_identifier": source.identifier,
                    "source_dcat_content_hash": source.content_hash,
                    "catalog": {
                        key: value for key, value in catalog.items() if key != "members"
                    },
                    "dataportalen_uri": uri,
                    **compared,
                },
            }
            matches.append(match)
            support[key].append(match)
        for group in support.values():
            if len(group) > 1:
                for match in group:
                    match["status"] = "ambiguous"
        return sorted(matches, key=lambda match: match["identifier"])


def self_citation(
    source: SourceMetadata, facts: frozenset[Fact]
) -> dict[str, Any] | None:
    """Require the exact title before an own primary URL in one description paragraph."""
    publishers = frozenset(
        fact for fact in facts if fact.predicate == DCT + "publisher"
    )
    if source.field(DCT + "publisher") != publishers:
        return None
    titles = source.field(DCT + "title") & facts
    for description in sorted(
        fact for fact in facts if fact.predicate == DCT + "description"
    ):
        for paragraph in re.split(r"\n\s*\n|\r\n\s*\r\n", description.value):
            for url, field in source.primary_urls:
                # URL tokens retain paths, queries and versions; punctuation can delimit prose.
                pattern = (
                    r"(?<![\w/.:?&#=%+~-])"
                    + re.escape(url)
                    + r"(?=$|[\s<>\"']|[.,;)](?:\s|$))"
                )
                occurrence = re.search(pattern, paragraph)
                if occurrence is None:
                    continue
                for title in sorted(titles):
                    if title.value not in paragraph[: occurrence.start()]:
                        continue
                    return {
                        "primary_url": url,
                        "primary_url_field": field,
                        "self_citation_text": paragraph,
                        "description": asdict(description),
                        "title": asdict(title),
                        "publishers": [asdict(fact) for fact in sorted(publishers)],
                    }
    return None


def fingerprint(value: Any) -> str:
    """Hash JSON evidence with stable key ordering and exact lexical values."""
    return hashlib.sha256(
        json.dumps(
            value, sort_keys=True, ensure_ascii=False, separators=(",", ":")
        ).encode()
    ).hexdigest()


def match_records(
    research: dict[str, dict[str, Any]],
    dataportalen: dict[str, dict[str, Any]],
    previous_links: list[dict[str, Any]],
    previous_matches: dict[str, list[str]],
    previous_fingerprints: dict[str, Any],
    catalog: dict[str, Any],
) -> tuple[list[dict[str, Any]], dict[str, list[str]], dict[str, Any]]:
    """Match direct evidence first and reuse unchanged confirmed compound links.

    ``matches`` maps exact Researchdata identifiers to sorted Dataportalen URIs
    for confirmed indirect links only. ``fingerprints`` contains ``policy``,
    per-URI ``candidates`` hashes and ``dataportalen`` hashes. Candidate hashes
    cover every exact-title peer and its source evidence, so relevant additions,
    changes and removals rerun matching and ambiguity checks without invalidating
    unrelated title groups.
    Only links present in both the confirmed map and previous links are reusable.
    Ambiguous links remain observable but never enter the confirmed map.
    """
    identities = {key: record.get("identities", []) for key, record in research.items()}
    index = identity_index(identities)
    metadata_index = MetadataIndex(research)
    record_hashes = {
        key: fingerprint(
            {
                "identities": record.get("identities", []),
                "representations": record.get("representations", {}),
            }
        )
        for key, record in sorted(research.items())
    }
    candidates: dict[str, str] = {}
    for uri, record in sorted(dataportalen.items()):
        fields = record["metadata"]["nodes"].get(uri, {})
        identifiers = {
            source.identifier
            for term in fields.get(DCT + "title", [])
            if (fact := triple_fact({"p": {"value": DCT + "title"}, "o": term}))
            for source in metadata_index.by_title.get(fact, [])
        }
        candidates[uri] = fingerprint(
            {key: record_hashes[key] for key in sorted(identifiers)}
        )
    fingerprints = {
        "policy": MATCHING_POLICY,
        "candidates": candidates,
        "catalog": fingerprint(
            {key: catalog.get(key) for key in ("source", "catalog")}
        ),
        "dataportalen": {
            uri: fingerprint(
                {
                    "identities": record.get("identities", []),
                    "metadata": record["metadata"],
                }
            )
            for uri, record in sorted(dataportalen.items())
        },
    }
    reusable_policy = (
        previous_fingerprints.get("policy") == MATCHING_POLICY
        and previous_fingerprints.get("catalog") == fingerprints["catalog"]
    )
    by_uri: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for link in previous_links:
        identifier, uri = link.get("identifier"), link.get("uri")
        if (
            identifier in research
            and uri in dataportalen
            and link.get("status") in {"matched", "ambiguous"}
            and link.get("rule") in {"exact_dcat_fields", "corroborated_self_citation"}
            and (
                link.get("status") == "ambiguous"
                or uri in previous_matches.get(identifier, [])
            )
        ):
            by_uri[uri].append(link)
    links: list[dict[str, Any]] = []
    matches: dict[str, list[str]] = defaultdict(list)
    members = set(catalog["members"])
    for uri, record in sorted(dataportalen.items()):
        if uri not in members:
            continue
        direct = match_identities(identities, record.get("identities", []), index=index)
        if direct:
            selected = direct
        elif (
            reusable_policy
            and previous_fingerprints.get("candidates", {}).get(uri) == candidates[uri]
            and previous_fingerprints.get("dataportalen", {}).get(uri)
            == fingerprints["dataportalen"][uri]
            and any(link["status"] == "matched" for link in by_uri[uri])
        ):
            selected = by_uri[uri]
        else:
            selected = metadata_index.match(
                uri, metadata_to_triples(record["metadata"]), catalog
            )
        for match in selected:
            link = dict(match, uri=uri)
            if link["rule"] != "exact_identity":
                link["evidence"] = dict(
                    link["evidence"],
                    catalog={
                        key: value for key, value in catalog.items() if key != "members"
                    },
                )
            links.append(link)
            if link["rule"] != "exact_identity" and link["status"] == "matched":
                matches[link["identifier"]].append(uri)
    return (
        sorted(links, key=lambda link: (link["identifier"], link["uri"], link["rule"])),
        {key: sorted(set(uris)) for key, uris in sorted(matches.items())},
        fingerprints,
    )
