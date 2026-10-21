# SPDX-License-Identifier: NOASSERTION
# Copyright (c) 2026 NordicIntel
"""Extract explicit identity and file evidence without using citations as identity."""

import json
import re
from collections.abc import Iterator
from html.parser import HTMLParser
from typing import Any
from urllib.parse import unquote, urlsplit, urlunsplit

from harvester.oai import local_name

DOI = re.compile(r"^10\.\d{4,9}/\S+$", re.I)
IDENTITY_NAMES = {
    "identifier",
    "alternateIdentifier",
    "sameAs",
    "landingPage",
    "url",
    "@id",
    "mainEntityOfPage",
}


def normalize_identity(value: str) -> str | None:
    value = value.strip()
    doi_value = re.sub(r"^doi:\s*", "", value, flags=re.I)
    if re.match(r"^https?://(?:dx\.)?doi\.org/", doi_value, re.I):
        doi_value = re.sub(r"^https?://(?:dx\.)?doi\.org/", "", doi_value, flags=re.I)
        doi_value = unquote(doi_value)
    if DOI.fullmatch(doi_value):
        return "doi:" + doi_value.casefold()
    parsed = urlsplit(value)
    if parsed.scheme.lower() in {"http", "https"} and parsed.hostname:
        return urlunsplit(
            (
                parsed.scheme.lower(),
                parsed.netloc.lower(),
                parsed.path,
                parsed.query,
                parsed.fragment,
            )
        )
    return value if value and not any(char.isspace() for char in value) else None


def walk_xml(
    tree: dict[str, Any], path: tuple[str, ...] = ()
) -> Iterator[tuple[tuple[str, ...], dict[str, Any]]]:
    current = (*path, local_name(tree["tag"]))
    yield current, tree
    for child in tree.get("children", []):
        yield from walk_xml(child, current)


def jsonld_documents(tree: dict[str, Any] | None) -> list[Any]:
    documents = []
    if tree:
        for path, node in walk_xml(tree):
            if path[-1] == "jsonLdString" and node.get("text"):
                documents.append(
                    node["json"] if "json" in node else json.loads(node["text"])
                )
    return documents


def values(value: Any) -> list[Any]:
    return value if isinstance(value, list) else [value]


def json_entities(document: Any) -> list[dict[str, Any]]:
    if isinstance(document, list):
        return [entity for item in document for entity in json_entities(item)]
    if isinstance(document, dict):
        graph = document.get("@graph")
        if graph is not None:
            return json_entities(graph)
        return [document]
    return []


def scalar_values(value: Any) -> Iterator[str]:
    for item in values(value):
        if isinstance(item, str):
            yield item
        elif isinstance(item, dict):
            for key in ("@id", "@value", "value", "url"):
                if isinstance(item.get(key), str):
                    yield item[key]


def owned_datasets(document: Any, anchors: set[str]) -> list[dict[str, Any]]:
    entities = [
        entity
        for entity in json_entities(document)
        if not entity.get("@type") or "Dataset" in str(entity["@type"])
    ]
    if len(entities) > 1:
        entities = [
            entity
            for entity in entities
            if anchors
            & {
                normalize_identity(value)
                for field in ("identifier", "@id")
                for value in scalar_values(entity.get(field, []))
            }
        ]
        if len(entities) != 1:
            return []
    elif entities and anchors:
        identifiers = {
            normalize_identity(value)
            for field in ("identifier", "@id", "url")
            for value in scalar_values(entities[0].get(field, []))
        }
        if identifiers and not identifiers & anchors:
            return []
    return entities


def extract_evidence(
    tree: dict[str, Any] | None, *, primary_identifiers: set[str] | None = None
) -> list[dict[str, str]]:
    result: list[dict[str, str]] = []
    if tree is None:
        return result
    for path, node in walk_xml(tree):
        text = (node.get("text") or "").strip()
        if any(
            name.casefold() in {"citation", "reference", "references", "relateditem"}
            for name in path
        ):
            continue
        if path[-1] == "relatedIdentifier":
            attributes = {
                local_name(key): value
                for key, value in node.get("attributes", {}).items()
            }
            if attributes.get("relationType") == "IsVersionOf" and text:
                result.append(
                    {"value": text, "field": "/".join(path), "relation": "dataset"}
                )
            continue
        if any(
            name.casefold()
            in {
                "relatedidentifier",
                "citation",
                "reference",
                "references",
                "creator",
                "contributor",
                "publisher",
                "contactpoint",
                "person",
                "organization",
                "agent",
                "distribution",
                "datadownload",
            }
            for name in path
        ):
            continue
        if (
            path[-1]
            in {"identifier", "alternateIdentifier", "sameAs", "landingPage", "url"}
            and text
        ):
            result.append(
                {"value": text, "field": "/".join(path), "relation": "version"}
            )
        if path[-1] == "Dataset":
            for key, value in node.get("attributes", {}).items():
                if local_name(key) == "about":
                    result.append(
                        {
                            "value": value,
                            "field": "/".join(path) + "/@about",
                            "relation": "version",
                        }
                    )
        if path[-1] in {"landingPage", "identifier", "sameAs"}:
            for key, value in node.get("attributes", {}).items():
                if local_name(key) == "resource":
                    result.append(
                        {"value": value, "field": "/".join(path), "relation": "version"}
                    )
    for document in jsonld_documents(tree):
        for entity in owned_datasets(document, primary_identifiers or set()):
            for key in IDENTITY_NAMES:
                for value in scalar_values(entity.get(key, [])):
                    result.append(
                        {
                            "value": value,
                            "field": "jsonld/" + key,
                            "relation": "version",
                        }
                    )
    return [
        dict(item, normalized=normalized)
        for item in result
        if (normalized := normalize_identity(item["value"]))
    ]


def extract_files(documents: list[Any]) -> list[dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for document in documents:
        for entity in json_entities(document):
            for distribution in values(entity.get("distribution", [])):
                if not isinstance(distribution, dict):
                    continue
                urls = list(
                    scalar_values(
                        distribution.get(
                            "contentUrl",
                            distribution.get(
                                "contentURL", distribution.get("downloadURL", [])
                            ),
                        )
                    )
                )
                for url in urls:
                    if urlsplit(url).scheme not in {"http", "https"}:
                        continue
                    size = distribution.get("contentSize", distribution.get("byteSize"))
                    try:
                        size = int(size) if size is not None else None
                    except (ValueError, TypeError):
                        size = None
                    checksum = distribution.get("sha256")
                    if isinstance(checksum, dict):
                        checksum = checksum.get("@value", checksum.get("value"))
                    result[url] = {
                        "url": url,
                        "name": distribution.get("name"),
                        "size": size,
                        "sha256": checksum,
                        "source": "jsonld/distribution",
                        "encoding_format": distribution.get("encodingFormat"),
                        "license": distribution.get("license", entity.get("license")),
                        "access_conditions": entity.get(
                            "conditionsOfAccess", entity.get("isAccessibleForFree")
                        ),
                        "raw": distribution,
                    }
    return list(result.values())


class JSONLDParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.active = False
        self.buffer: list[str] = []
        self.documents: list[Any] = []
        self.links: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        href = dict(attrs).get("href")
        if tag.lower() == "a" and href:
            self.links.append(href)
        if (
            tag.lower() == "script"
            and dict(attrs).get("type", "").lower() == "application/ld+json"
        ):
            self.active = True
            self.buffer = []

    def handle_data(self, data: str) -> None:
        if self.active:
            self.buffer.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() == "script" and self.active:
            self.active = False
            self.documents.append(json.loads("".join(self.buffer)))


def metadata_summary(
    trees: list[dict[str, Any] | None],
    *,
    primary_identifiers: set[str] | None = None,
) -> dict[str, list[dict[str, str | None]]]:
    """Return source titles, descriptions and publishers without discarding languages."""
    result: dict[str, list[dict[str, str | None]]] = {
        "titles": [],
        "descriptions": [],
        "publishers": [],
    }
    fields = {
        "title": "titles",
        "description": "descriptions",
        "abstract": "descriptions",
        "publisher": "publishers",
    }

    def add(field: str, value: str, language: str | None = None) -> None:
        item = {"value": value, "language": language}
        if value and item not in result[field]:
            result[field].append(item)

    for tree in trees:
        if tree is None:
            continue
        for path, node in walk_xml(tree):
            if any(
                name.casefold()
                in {"citation", "reference", "references", "relateditem"}
                for name in path
            ):
                continue
            if path[-1] in fields:
                add(
                    fields[path[-1]],
                    (node.get("text") or "").strip(),
                    node.get("attributes", {}).get(
                        "{http://www.w3.org/XML/1998/namespace}lang"
                    ),
                )
        for document in jsonld_documents(tree):
            for entity in owned_datasets(document, primary_identifiers or set()):
                for key, target in (
                    ("name", "titles"),
                    ("description", "descriptions"),
                    ("publisher", "publishers"),
                ):
                    for item in values(entity.get(key, [])):
                        if isinstance(item, str):
                            add(target, item)
                        elif isinstance(item, dict):
                            text = item.get("@value", item.get("name"))
                            if isinstance(text, str):
                                add(target, text, item.get("@language"))
    return result
