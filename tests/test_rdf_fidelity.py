# SPDX-License-Identifier: NOASSERTION
# Copyright (c) 2026 NordicIntel
"""Verify RDF lexical fidelity and narrowly scoped conversion diagnostics."""

import logging
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import httpx
import pytest
from rdflib import Literal, URIRef
from rdflib.exceptions import ParserError

from researchdata_harvester import dataportalen
from researchdata_harvester.config import Config
from researchdata_harvester.dataportalen import DataportalenClient
from researchdata_harvester.transport import Transport

URI = "https://dataset.test/item"
XSD = "http://www.w3.org/2001/XMLSchema#"


async def metadata(
    content: str, content_type: str = "application/n-triples"
) -> list[dict[str, Any]]:
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(
                200, text=content, headers={"Content-Type": content_type}
            )
        )
    ) as client:
        api = DataportalenClient(
            Transport(Config(retries=0), client), "https://source.test/sparql"
        )
        return (await api.metadata([URI]))[URI]


async def test_literal_spelling_datatype_and_language_are_preserved(caplog):
    content = f"""<{URI}> <https://test/integer> "01"^^<{XSD}integer> .
<{URI}> <https://test/integer> "1"^^<{XSD}integer> .
<{URI}> <https://test/year> "2020"^^<{XSD}dateTime> .
<{URI}> <https://test/token> "  two  words  "^^<{XSD}token> .
<{URI}> <https://test/string> "a\\tb"^^<{XSD}normalizedString> .
<{URI}> <https://test/label> "Hej"@sv-SE .
<{URI}> <https://test/blank> _:a .
_:a <https://test/label> "Nested"@en ."""
    triples = await metadata(content)
    objects = [triple["o"] for triple in triples]
    assert {
        obj["value"] for obj in objects if obj.get("datatype") == XSD + "integer"
    } == {"01", "1"}
    assert {
        "type": "literal",
        "value": "2020",
        "datatype": XSD + "dateTime",
        "ill_typed": True,
    } in objects
    assert {
        "type": "literal",
        "value": "  two  words  ",
        "datatype": XSD + "token",
    } in objects
    assert {
        "type": "literal",
        "value": "a\tb",
        "datatype": XSD + "normalizedString",
    } in objects
    assert {"type": "literal", "value": "Hej", "xml:lang": "sv-SE"} in objects
    blank = next(obj for obj in objects if obj["type"] == "bnode")
    assert any(
        triple["s"] == blank and triple["o"].get("value") == "Nested"
        for triple in triples
    )
    assert len(caplog.records) == 1
    assert "1 ill-typed literal triples" in caplog.text
    assert all(record.exc_info is None for record in caplog.records)


async def test_invalid_literals_produce_one_summary_without_hiding_other_logs(
    caplog, monkeypatch
):
    original = dataportalen.Literal
    first = True

    def construct_literal(*args: Any, **kwargs: Any) -> Literal:
        nonlocal first
        if first:
            first = False
            logging.getLogger("rdflib.term").warning("Unrelated parser warning")
            with ThreadPoolExecutor(max_workers=1) as executor:
                executor.submit(
                    Literal, "bad", datatype=URIRef(XSD + "dateTime")
                ).result()
        return original(*args, **kwargs)

    monkeypatch.setattr(dataportalen, "Literal", construct_literal)
    triples = await metadata(
        "\n".join(
            f'<{URI}> <https://test/date/{index}> "2020"^^<{XSD}dateTime> .'
            for index in range(100)
        )
    )
    assert len(triples) == 100
    assert all(triple["o"]["ill_typed"] is True for triple in triples)
    assert "100 ill-typed literal triples" in caplog.text
    assert "Unrelated parser warning" in caplog.text
    converter_warnings = [
        record
        for record in caplog.records
        if "Failed to convert Literal" in record.getMessage()
    ]
    assert len(converter_warnings) == 1
    assert converter_warnings[0].thread != dataportalen.get_ident()


async def test_parse_failure_keeps_error_and_restores_converter_logging(caplog):
    with pytest.raises(ParserError):
        await metadata(f'<{URI}> <https://test/p> "unterminated')
    Literal("2020", datatype=URIRef(XSD + "dateTime"))
    assert "Failed to convert Literal" in caplog.text


@pytest.mark.parametrize(
    "content_type", ["text/turtle", "application/rdf+xml", "text/html"]
)
async def test_unrequested_rdf_syntax_is_rejected(content_type):
    with pytest.raises(ValueError, match="preserve RDF lexical values"):
        await metadata("", content_type)
