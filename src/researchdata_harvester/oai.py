# SPDX-License-Identifier: NOASSERTION
# Copyright (c) 2026 NordicIntel
"""Lossless XML trees and the six OAI-PMH verbs with complete page iterators."""

from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Any
from urllib.parse import quote
from xml.etree import ElementTree as ET

from defusedxml.ElementTree import fromstring

from researchdata_harvester.transport import Transport

NS = "{http://www.openarchives.org/OAI/2.0/}"


def local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def xml_tree(element: ET.Element) -> dict[str, Any]:
    """Keep expanded namespace names, ordered children, attributes and mixed text."""
    return {
        "tag": element.tag,
        "attributes": dict(element.attrib),
        "text": element.text,
        "tail": element.tail,
        "children": [xml_tree(child) for child in element],
    }


@dataclass(frozen=True)
class Record:
    identifier: str
    datestamp: str
    sets: list[str]
    deleted: bool
    metadata: dict[str, Any] | None
    raw_xml: str


@dataclass(frozen=True)
class Page:
    items: list[Any]
    token: str | None
    total: int | None
    raw_xml: str


class OAIError(RuntimeError):
    def __init__(self, code: str, message: str):
        self.code = code
        super().__init__(f"OAI-PMH {code}: {message}")


def parse_record(element: ET.Element) -> Record:
    header = element.find(NS + "header")
    if header is None or not header.findtext(NS + "identifier"):
        raise ValueError("OAI record has no identifier header; the scan is incomplete.")
    metadata = element.find(NS + "metadata")
    deleted = header.get("status") == "deleted"
    if not deleted and (metadata is None or not len(metadata)):
        raise ValueError("Live OAI record has no metadata; the scan is incomplete.")
    return Record(
        header.findtext(NS + "identifier", "").strip(),
        header.findtext(NS + "datestamp", "").strip(),
        [item.text or "" for item in header.findall(NS + "setSpec")],
        deleted,
        xml_tree(metadata[0]) if metadata is not None and len(metadata) else None,
        ET.tostring(element, encoding="unicode"),
    )


class OAIClient:
    def __init__(self, transport: Transport, url: str):
        self.transport = transport
        self.url = url

    async def call(self, verb: str, **options: str | None) -> tuple[ET.Element, str]:
        params = {
            "verb": verb,
            **{key: value for key, value in options.items() if value is not None},
        }
        response = await self.transport.request("GET", self.url, params=params)
        root = fromstring(response.content)
        if root.tag != NS + "OAI-PMH":
            raise ValueError("Expected an OAI-PMH envelope; the scan is incomplete.")
        error = root.find(NS + "error")
        if error is not None:
            raise OAIError(error.get("code", "unknown"), error.text or "")
        container = root.find(NS + verb)
        if container is None:
            raise ValueError(
                f"Missing OAI-PMH {verb} container; the scan is incomplete."
            )
        return container, response.text

    async def identify(self) -> dict[str, Any]:
        element, _ = await self.call("Identify")
        return xml_tree(element)

    async def list_metadata_formats(
        self, identifier: str | None = None
    ) -> list[dict[str, Any]]:
        element, _ = await self.call("ListMetadataFormats", identifier=identifier)
        return [xml_tree(item) for item in element.findall(NS + "metadataFormat")]

    async def list_sets(self, *, resumption_token: str | None = None) -> Page:
        return await self._page("ListSets", resumption_token=resumption_token)

    async def list_identifiers(
        self, metadata_prefix: str = "datacite", **options: Any
    ) -> Page:
        return await self._page(
            "ListIdentifiers", metadata_prefix=metadata_prefix, **options
        )

    async def list_records(
        self, metadata_prefix: str = "datacite", **options: Any
    ) -> Page:
        return await self._page(
            "ListRecords", metadata_prefix=metadata_prefix, **options
        )

    async def get_record(
        self, identifier: str, metadata_prefix: str = "datacite"
    ) -> Record:
        element, _ = await self.call(
            "GetRecord", identifier=identifier, metadataPrefix=metadata_prefix
        )
        record = element.find(NS + "record")
        if record is None:
            raise ValueError(f"GetRecord returned no record for {identifier}.")
        result = parse_record(record)
        if result.identifier != identifier:
            raise ValueError(
                f"GetRecord returned {result.identifier} instead of {identifier}; retry against a consistent source."
            )
        return result

    async def _page(
        self,
        verb: str,
        *,
        metadata_prefix: str = "datacite",
        set_spec: str | None = None,
        from_: str | None = None,
        until: str | None = None,
        resumption_token: str | None = None,
    ) -> Page:
        options = (
            {"resumptionToken": resumption_token}
            if resumption_token
            else {
                "metadataPrefix": metadata_prefix,
                "set": set_spec,
                "from": from_,
                "until": until,
            }
        )
        if verb == "ListSets" and not resumption_token:
            options = {}
        try:
            element, raw = await self.call(verb, **options)
        except OAIError as error:
            if error.code in {"noRecordsMatch", "noSetHierarchy"}:
                return Page([], None, 0, "")
            raise
        token_element = element.find(NS + "resumptionToken")
        token = token_element.text if token_element is not None else None
        total = (
            token_element.get("completeListSize") if token_element is not None else None
        )
        if verb == "ListRecords":
            items = [parse_record(item) for item in element.findall(NS + "record")]
        else:
            items = [
                xml_tree(item)
                for item in element.findall(
                    NS + ("header" if verb == "ListIdentifiers" else "set")
                )
            ]
        return Page(
            items,
            token if token and token.strip() else None,
            int(total) if total else None,
            raw,
        )

    async def iter_pages(self, verb: str, **options: Any) -> AsyncIterator[Page]:
        """Yield every page; repeated tokens are protocol failures, never completion."""
        seen: set[str] = set()
        token = options.pop("resumption_token", None)
        if token:
            seen.add(token)
        while True:
            page = await self._page(verb, resumption_token=token, **options)
            if page.token in seen:
                raise ValueError(
                    "OAI-PMH repeated a resumption token; the scan is incomplete."
                )
            yield page
            if page.token is None:
                break
            seen.add(page.token)
            token = page.token

    async def iter_records(
        self, metadata_prefix: str = "datacite", **options: Any
    ) -> AsyncIterator[Record]:
        async for page in self.iter_pages(
            "ListRecords", metadata_prefix=metadata_prefix, **options
        ):
            for record in page.items:
                yield record

    async def iter_identifiers(
        self, metadata_prefix: str = "datacite", **options: Any
    ) -> AsyncIterator[dict[str, Any]]:
        async for page in self.iter_pages(
            "ListIdentifiers", metadata_prefix=metadata_prefix, **options
        ):
            for header in page.items:
                yield header

    async def iter_sets(self) -> AsyncIterator[dict[str, Any]]:
        async for page in self.iter_pages("ListSets"):
            for item in page.items:
                yield item


def build_file_url(dataset_id: str, version: str | int, filename: str = "") -> str:
    """Construct an unverified API URL; callers must obtain filenames from metadata."""
    parts = [dataset_id, str(version), *filename.replace("\\", "/").split("/")]
    if any(
        part in {"", ".", ".."} or "/" in part or "\\" in part for part in parts[:2]
    ):
        raise ValueError("Dataset ID and version must be individual path components.")
    if any(part in {".", ".."} for part in parts[2:]):
        raise ValueError("File paths cannot contain traversal components.")
    base = f"https://api.researchdata.se/dataset/{quote(parts[0], safe='')}/{quote(parts[1], safe='')}/file"
    return base + (
        "/" + "/".join(quote(part, safe="") for part in parts[2:]) if filename else ""
    )
