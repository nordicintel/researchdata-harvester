# SPDX-License-Identifier: NOASSERTION
# Copyright (c) 2026 NordicIntel
"""Protect catalog scope, complete descriptions, and lexical graph fidelity."""

from types import SimpleNamespace
from typing import Any

import httpx
import pytest

from harvester.dataportalen import (
    DataportalenClient,
    harvest_dataportalen,
    metadata_to_triples,
    triples_to_metadata,
)

URI = "https://data.test/a"
CATALOG = "https://researchdata.test/catalog"
DCT = "http://purl.org/dc/terms/"
DCAT = "http://www.w3.org/ns/dcat#"
RDF = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
XSD = "http://www.w3.org/2001/XMLSchema#"


async def test_unexpected_empty_catalog_cannot_remove_existing_records():
    transport = EndpointTransport(
        lambda request: httpx.Response(
            200,
            json={
                CATALOG: {
                    RDF + "type": [{"type": "uri", "value": DCAT + "Catalog"}],
                    DCAT + "dataset": [],
                }
            },
        )
    )
    config = SimpleNamespace(
        sparql_url="https://test/sparql",
        catalog_metadata_url="https://test/catalog",
        catalog_uri=CATALOG,
        identity_batch_size=100,
    )
    try:
        with pytest.raises(ValueError, match="unexpectedly empty"):
            await harvest_dataportalen(transport, config, {URI: {"uri": URI}})
    finally:
        await transport.client.aclose()


class EndpointTransport:
    def __init__(self, handler: Any):
        self.client = httpx.AsyncClient(transport=httpx.MockTransport(handler))

    async def request(self, method: str, url: str, **kwargs: Any) -> httpx.Response:
        response = await self.client.request(method, url, **kwargs)
        response.raise_for_status()
        return response


async def read_graph(content: str) -> dict[str, Any]:
    transport = EndpointTransport(
        lambda request: httpx.Response(
            200, text=content, headers={"Content-Type": "application/n-triples"}
        )
    )
    try:
        return (
            await DataportalenClient(transport, "https://test/sparql").metadata([URI])
        )[URI]
    finally:
        await transport.client.aclose()


async def test_canonical_graph_is_invariant_to_labels_order_and_symmetry():
    content = f"""<{URI}> <{DCT}temporal> _:one .
<{URI}> <{DCT}temporal> _:two .
_:one <{DCT}title> "same"@sv .
_:two <{DCT}title> "same"@sv .
_:one <{DCT}relation> _:two .
_:two <{DCT}relation> _:one ."""
    renamed = content.replace("_:one", "_:other").replace("_:two", "_:different")
    first = await read_graph(content)
    assert first == await read_graph("\n".join(reversed(renamed.splitlines())))
    assert triples_to_metadata(metadata_to_triples(first), URI) == first
    assert len(first["nodes"]) == 3


async def test_graph_preserves_malformed_and_normalizable_lexical_literals():
    graph = await read_graph(f"""<{URI}> <{DCT}issued> "2020"^^<{XSD}dateTime> .
<{URI}> <{DCT}identifier> "01"^^<{XSD}integer> .
<{URI}> <{DCT}identifier> "1"^^<{XSD}integer> .
<{URI}> <{DCT}title> "Name"@sv-SE .
<{URI}> <{DCT}description> "  two  words  "^^<{XSD}token> .""")
    fields = graph["nodes"][URI]
    assert fields[DCT + "issued"] == [
        {
            "type": "literal",
            "value": "2020",
            "datatype": XSD + "dateTime",
            "ill_typed": True,
        }
    ]
    assert {term["value"] for term in fields[DCT + "identifier"]} == {"01", "1"}
    assert fields[DCT + "description"][0]["value"] == "  two  words  "
    assert fields[DCT + "title"][0]["xml:lang"] == "sv-SE"


async def test_missing_blank_node_or_root_fails_instead_of_publishing_partial_metadata():
    with pytest.raises(ValueError, match="blank-node description"):
        await read_graph(f"<{URI}> <{DCT}temporal> _:missing .")
    with pytest.raises(ValueError, match="requested metadata root"):
        await read_graph('<https://other.test/a> <https://test/title> "Other" .')


async def test_inventory_limits_requests_and_refreshes_independent_metadata():
    calls = []
    title = "Before"

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        if request.method == "GET":
            return httpx.Response(
                200,
                json={
                    CATALOG: {
                        RDF + "type": [{"type": "uri", "value": DCAT + "Catalog"}],
                        DCAT + "dataset": [{"type": "uri", "value": URI}],
                    }
                },
            )
        if b"DESCRIBE" in request.content:
            return httpx.Response(
                200,
                text=f'<{URI}> <{DCT}title> "{title}" .',
                headers={"Content-Type": "application/n-triples"},
            )
        return httpx.Response(200, json={"results": {"bindings": []}})

    transport = EndpointTransport(handler)
    config = SimpleNamespace(
        sparql_url="https://test/sparql",
        catalog_metadata_url="https://test/catalog",
        catalog_uri=CATALOG,
        identity_batch_size=10,
    )
    try:
        first, catalog = await harvest_dataportalen(transport, config, {})
        title = "After"
        second, _ = await harvest_dataportalen(transport, config, first)
    finally:
        await transport.client.aclose()
    assert catalog["members"] == list(first) == list(second) == [URI]
    assert first[URI]["metadata"]["nodes"][URI][DCT + "title"][0]["value"] == "Before"
    assert second[URI]["metadata"]["nodes"][URI][DCT + "title"][0]["value"] == "After"
    assert len(calls) == 6
    assert all(
        b"VALUES" in request.content for request in calls if request.method == "POST"
    )


async def test_malformed_inventory_is_rejected():
    transport = EndpointTransport(
        lambda request: httpx.Response(
            200,
            json={
                CATALOG: {RDF + "type": [{"type": "uri", "value": DCAT + "Catalog"}]}
            },
        )
    )
    try:
        with pytest.raises(ValueError):
            await DataportalenClient(
                transport, "https://test/sparql"
            ).catalog_inventory("https://test/catalog", CATALOG)
    finally:
        await transport.client.aclose()
