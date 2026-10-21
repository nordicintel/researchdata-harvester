# SPDX-License-Identifier: NOASSERTION
# Copyright (c) 2026 NordicIntel
"""Verify compound catalog pairing through the real asynchronous coordinator."""

import json
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs
from xml.etree.ElementTree import fromstring
from xml.sax.saxutils import escape

import httpx
import pytest

from researchdata_harvester.config import Config
from researchdata_harvester.harvester import Harvester
from researchdata_harvester.matching import DATACITE, DCAT, DCT, RDF, primary_urls
from researchdata_harvester.oai import parse_record, xml_tree
from researchdata_harvester.store import Store

URI = "https://data.test/a"
CATALOG = "https://researchdata.se/catalogue/"
PUBLISHER = "https://publisher.test/organisation"
PRIMARY_URL = "https://hdl.handle.net/123.1/dataset?v=1"
TITLE = "Mätning åäö, 2024–2025"
DATE = "2026-01-01T00:00:00Z"
XSD = "http://www.w3.org/2001/XMLSchema#"


def source_xml(
    *,
    title: str = TITLE,
    description: str = "Original abstract",
    date: str = DATE,
    publisher: str = PUBLISHER,
    language: str = "en",
    datatype: str = XSD + "dateTime",
    period: str | None = None,
    about: str | None = None,
    rdf_id: str | None = None,
) -> str:
    temporal = (
        f'<dct:temporal><dct:PeriodOfTime><dcat:startDate rdf:datatype="{XSD}date">{period}</dcat:startDate></dct:PeriodOfTime></dct:temporal>'
        if period
        else ""
    )
    identifier_attributes = (
        f' rdf:about="{about}"'
        if about is not None
        else f' rdf:ID="{rdf_id}"'
        if rdf_id is not None
        else ""
    )
    return (
        f'<rdf:RDF xmlns:rdf="{RDF}" xmlns:dcat="{DCAT}" xmlns:dct="{DCT}">'
        + f"<dcat:Dataset{identifier_attributes}>"
        + f'<dct:title xml:lang="{language}">{escape(title)}</dct:title>'
        f'<dct:description xml:lang="{language}">{escape(description)}</dct:description>'
        f'<dct:issued rdf:datatype="{datatype}">{date}</dct:issued>'
        f'<dct:publisher rdf:resource="{publisher}"/>{temporal}'
        "</dcat:Dataset></rdf:RDF>"
    )


def seed_source(
    store: Store,
    run: int,
    identifier: str = "oai:researchdata.se:one/1",
    **options: Any,
) -> None:
    seed_representation(store, run, identifier, "dcat", source_xml(**options))


def record_xml(identifier: str, metadata: str) -> str:
    return (
        '<record xmlns="http://www.openarchives.org/OAI/2.0/">'
        f"<header><identifier>{identifier}</identifier><datestamp>2026-10-21</datestamp></header>"
        f"<metadata>{metadata}</metadata></record>"
    )


def seed_representation(
    store: Store, run: int, identifier: str, prefix: str, metadata: str
) -> None:
    store.put_record(
        parse_record(fromstring(record_xml(identifier, metadata))),
        prefix,
        run,
        "https://source.test/oai",
        catalogue=True,
    )


def external_graph(
    *,
    uri: str = URI,
    title: str = TITLE,
    description: str = "Original abstract",
    date: str = DATE,
    publisher: str = PUBLISHER,
    language: str = "en",
    datatype: str = XSD + "dateTime",
    period: str | None = None,
) -> str:
    graph = (
        f"<{uri}> <{DCT}title> {json.dumps(title)}@{language} .\n"
        f"<{uri}> <{DCT}description> {json.dumps(description)}@{language} .\n"
        f"<{uri}> <{DCT}issued> {json.dumps(date)}^^<{datatype}> .\n"
        f"<{uri}> <{DCT}publisher> <{publisher}> .\n"
    )
    if period:
        graph += f"<{uri}> <{DCT}temporal> _:period .\n_:period <{DCAT}startDate> {json.dumps(period)}^^<{XSD}date> .\n"
    return graph


def catalog_document(members: list[str]) -> dict[str, Any]:
    return {
        CATALOG: {
            RDF + "type": [{"type": "uri", "value": DCAT + "Catalog"}],
            DCAT + "dataset": [{"type": "uri", "value": uri} for uri in members],
        }
    }


class Endpoint:
    def __init__(
        self, graphs: dict[str, str], *, members: list[str] | None = None
    ) -> None:
        self.graphs = graphs
        self.members = list(graphs) if members is None else members
        self.inventory_calls = 0
        self.fail_uri: str | None = None
        self.identity: str | None = None

    def __call__(self, request: httpx.Request) -> httpx.Response:
        if request.method == "GET":
            self.inventory_calls += 1
            return httpx.Response(200, json=catalog_document(self.members))
        query = parse_qs(request.content.decode())["query"][0]
        if "SELECT DISTINCT ?dataset WHERE" in query:
            uris = [] if "FILTER(STR" in query else sorted(self.graphs)
            return httpx.Response(
                200,
                json={
                    "results": {
                        "bindings": [
                            {"dataset": {"type": "uri", "value": uri}} for uri in uris
                        ]
                    }
                },
            )
        if "DESCRIBE" in query:
            selected = [uri for uri in self.graphs if f"<{uri}>" in query]
            if self.fail_uri in selected:
                return httpx.Response(503)
            return httpx.Response(
                200,
                text="\n".join(self.graphs[uri] for uri in selected),
                headers={"Content-Type": "application/n-triples"},
            )
        bindings = []
        if self.identity:
            bindings = [
                {
                    "dataset": {"type": "uri", "value": URI},
                    "field": {"type": "uri", "value": DCT + "identifier"},
                    "value": {"type": "literal", "value": self.identity},
                }
            ]
        return httpx.Response(200, json={"results": {"bindings": bindings}})


@pytest.mark.parametrize("scope", ["researchdata", "all"])
async def test_coordinator_pairs_anonymous_metadata_and_retains_primary_evidence(
    tmp_path: Path, scope: str
) -> None:
    endpoint = Endpoint({URI: external_graph(period="2024-01-01")})
    config = Config(database=tmp_path / "catalog.sqlite", dataportalen_scope=scope)
    async with httpx.AsyncClient(transport=httpx.MockTransport(endpoint)) as client:
        async with Harvester(config, client=client) as harvester:
            run = harvester.store.start_run("full", "2026-10-21")
            with harvester.store.connection:
                seed_source(harvester.store, run, period="2024-01-01")
            stats = await harvester._dataportalen(run)
            assert stats["matched"] == stats["no_identifier"] == 1
            assert stats["unresolved"] == 0
            link = dict(
                harvester.store.connection.execute("SELECT * FROM links").fetchone()
            )
            assert (link["rule"], link["status"], link["relation"]) == (
                "exact_dcat_fields",
                "matched",
                "catalog_representation",
            )
            evidence = json.loads(link["evidence"])
            expected_hash = harvester.store.connection.execute(
                "SELECT content_hash FROM representations WHERE format='dcat'"
            ).fetchone()[0]
            assert evidence["source_dcat_content_hash"] == expected_hash
            assert evidence["source_identifier"] == "oai:researchdata.se:one/1"
            assert evidence["catalog"]["catalog"] == CATALOG
            assert len(evidence["catalog"]["sha256"]) == 64
            assert any(
                fact["value"] == TITLE and fact["language"] == "en"
                for fact in evidence["facts"]
            )
            assert evidence["periods"][0][0]["value"] == "2024-01-01"
            assert endpoint.inventory_calls == 1


@pytest.mark.parametrize(
    "source_options,external_options",
    [
        ({}, {"description": "Different abstract"}),
        ({}, {"date": "2025-01-01T00:00:00Z"}),
        ({"period": "2024-01-01"}, {"period": "2023-01-01"}),
        ({}, {"publisher": "https://publisher.test/other"}),
        ({}, {"language": "sv"}),
        ({"datatype": XSD + "string"}, {}),
        (
            {"description": " leading and trailing "},
            {"description": "leading and trailing"},
        ),
        ({"about": "https://source.test/identified"}, {}),
        ({"about": ""}, {}),
        ({"rdf_id": "identified"}, {}),
        ({"date": ""}, {}),
        ({"description": ""}, {}),
        ({"title": ""}, {}),
        ({"date": "2026-01-01T00:00:00+00:00"}, {}),
        ({}, {"title": "Mätning åäö, 2024-2025"}),
    ],
)
async def test_coordinator_rejects_insufficient_or_changed_metadata(
    tmp_path: Path, source_options: dict[str, str], external_options: dict[str, str]
) -> None:
    endpoint = Endpoint({URI: external_graph(**external_options)})
    async with httpx.AsyncClient(transport=httpx.MockTransport(endpoint)) as client:
        async with Harvester(
            Config(database=tmp_path / "catalog.sqlite"), client=client
        ) as harvester:
            run = harvester.store.start_run("full", "2026-10-21")
            with harvester.store.connection:
                seed_source(harvester.store, run, **source_options)
            stats = await harvester._dataportalen(run)
            assert stats["matched"] == 0 and stats["unresolved"] == 1
            assert (
                harvester.store.connection.execute(
                    "SELECT COUNT(*) FROM links"
                ).fetchone()[0]
                == 0
            )


@pytest.mark.parametrize("primary_format", ["datacite", "json_ld"])
@pytest.mark.parametrize(
    "citation,expected",
    [
        (f"Author (2026). {TITLE} [Data set]. Repository. {PRIMARY_URL}", True),
        (f"Other title [Data set]. {PRIMARY_URL}", False),
        (f"{PRIMARY_URL} {TITLE}", False),
        (f"{TITLE}\n\nOther paragraph {PRIMARY_URL}", False),
        (f"{TITLE} {PRIMARY_URL}extra", False),
        (f"{TITLE} {PRIMARY_URL}&other=2", False),
        (f"{TITLE} https://paper.test/publication", False),
        (f"{TITLE} https://unrelated.test/data", False),
    ],
)
async def test_coordinator_requires_own_primary_url_and_corroborated_self_citation(
    tmp_path: Path, primary_format: str, citation: str, expected: bool
) -> None:
    endpoint = Endpoint({URI: external_graph(description=citation)})
    async with httpx.AsyncClient(transport=httpx.MockTransport(endpoint)) as client:
        async with Harvester(
            Config(database=tmp_path / "catalog.sqlite"), client=client
        ) as harvester:
            run = harvester.store.start_run("full", "2026-10-21")
            with harvester.store.connection:
                seed_source(harvester.store, run)
                if primary_format == "datacite":
                    metadata = f'<resource xmlns="{DATACITE}"><identifier identifierType="Handle">{escape(PRIMARY_URL)}</identifier><relatedIdentifiers><relatedIdentifier>https://paper.test/publication</relatedIdentifier></relatedIdentifiers></resource>'
                else:
                    document = {
                        "@type": "Dataset",
                        "identifier": {"@type": "PropertyValue", "value": PRIMARY_URL},
                        "citation": {
                            "@type": "Dataset",
                            "identifier": "https://paper.test/publication",
                        },
                    }
                    metadata = (
                        f"<jsonLdString>{escape(json.dumps(document))}</jsonLdString>"
                    )
                seed_representation(
                    harvester.store,
                    run,
                    "oai:researchdata.se:one/1",
                    primary_format,
                    metadata,
                )
            stats = await harvester._dataportalen(run)
            assert stats["matched"] == int(expected)
            if expected:
                row = harvester.store.connection.execute(
                    "SELECT rule,evidence FROM links"
                ).fetchone()
                assert row["rule"] == "corroborated_self_citation"
                evidence = json.loads(row["evidence"])
                assert evidence["primary_url"] == PRIMARY_URL
                assert evidence["self_citation_text"] == citation
                assert evidence["facts"]


@pytest.mark.parametrize("distinctive", [True, False])
async def test_coordinator_preserves_distinctive_many_to_one_and_flags_indistinguishable_sources(
    tmp_path: Path, distinctive: bool
) -> None:
    endpoint = Endpoint(
        {URI: external_graph() + external_graph(description="Second abstract")}
    )
    async with httpx.AsyncClient(transport=httpx.MockTransport(endpoint)) as client:
        async with Harvester(
            Config(database=tmp_path / "catalog.sqlite"), client=client
        ) as harvester:
            run = harvester.store.start_run("full", "2026-10-21")
            with harvester.store.connection:
                seed_source(harvester.store, run)
                seed_source(
                    harvester.store,
                    run,
                    "oai:researchdata.se:two/1",
                    description="Second abstract"
                    if distinctive
                    else "Original abstract",
                )
            assert (await harvester._dataportalen(run))["matched"] == 1
            links = harvester.store.connection.execute(
                "SELECT status FROM links"
            ).fetchall()
            assert [row[0] for row in links] == [
                "matched" if distinctive else "ambiguous"
            ] * 2


async def test_global_audit_refresh_retains_metadata_pairing_only_for_trusted_members(
    tmp_path: Path,
) -> None:
    other = "https://data.test/foreign"
    endpoint = Endpoint(
        {URI: external_graph(), other: external_graph(uri=other)}, members=[URI]
    )
    config = Config(database=tmp_path / "catalog.sqlite")
    async with httpx.AsyncClient(transport=httpx.MockTransport(endpoint)) as client:
        async with Harvester(config, client=client) as harvester:
            run = harvester.store.start_run("full", "2026-10-21")
            with harvester.store.connection:
                seed_source(harvester.store, run)
            assert (await harvester._dataportalen(run))["matched"] == 1
            config.dataportalen_scope = "all"
            run = harvester.store.start_run("incremental", "2026-10-21")
            stats = await harvester._dataportalen(run)
            assert stats["scanned"] == 2 and stats["matched"] == 1
            assert [
                row[0]
                for row in harvester.store.connection.execute("SELECT uri FROM links")
            ] == [URI]
            assert endpoint.inventory_calls == 2


async def test_metadata_pairing_resume_keeps_catalog_snapshot_links_and_counts(
    tmp_path: Path,
) -> None:
    second = "https://data.test/b"
    endpoint = Endpoint({URI: external_graph(), second: external_graph(uri=second)})
    endpoint.fail_uri = second
    config = Config(database=tmp_path / "catalog.sqlite", sparql_page_size=1, retries=0)
    async with httpx.AsyncClient(transport=httpx.MockTransport(endpoint)) as client:
        async with Harvester(config, client=client) as harvester:
            run = harvester.store.start_run("full", "2026-10-21")
            with harvester.store.connection:
                seed_source(harvester.store, run)
            with pytest.raises(httpx.HTTPStatusError):
                await harvester._dataportalen(run)
            assert (
                harvester.store.connection.execute(
                    "SELECT COUNT(*) FROM links"
                ).fetchone()[0]
                == 1
            )
        endpoint.fail_uri = None
        endpoint.members = []
        async with Harvester(config, client=client) as harvester:
            stats = await harvester._dataportalen(run)
            assert stats["scanned"] == stats["matched"] == stats["no_identifier"] == 2
            assert stats["unresolved"] == 0
            assert endpoint.inventory_calls == 1
            assert (
                harvester.store.connection.execute(
                    "SELECT COUNT(*) FROM links"
                ).fetchone()[0]
                == 2
            )


async def test_direct_doi_identity_takes_precedence_over_compound_metadata(
    tmp_path: Path,
) -> None:
    endpoint = Endpoint({URI: external_graph()})
    endpoint.identity = "doi:10.1234/EXAMPLE"
    async with httpx.AsyncClient(transport=httpx.MockTransport(endpoint)) as client:
        async with Harvester(
            Config(database=tmp_path / "catalog.sqlite"), client=client
        ) as harvester:
            run = harvester.store.start_run("full", "2026-10-21")
            with harvester.store.connection:
                seed_source(harvester.store, run)
                seed_representation(
                    harvester.store,
                    run,
                    "oai:researchdata.se:one/1",
                    "datacite",
                    f'<resource xmlns="{DATACITE}"><identifier>https://doi.org/10.1234/example</identifier></resource>',
                )
            assert (await harvester._dataportalen(run))["matched"] == 1
            link = harvester.store.connection.execute(
                "SELECT rule,relation,status FROM links"
            ).fetchone()
            assert tuple(link) == ("exact_identity", "version", "matched")


@pytest.mark.parametrize(
    "anchor,cited_owner,expected",
    [
        (None, False, False),
        (None, True, False),
        ("10.1234/OWN", False, False),
        ("10.1234/OWN", True, True),
    ],
)
async def test_coordinator_requires_graph_owner_before_accepting_primary_url(
    tmp_path: Path, anchor: str | None, cited_owner: bool, expected: bool
) -> None:
    own = "https://doi.org/10.1234/own"
    related = "https://doi.org/10.1234/related"
    cited_url = own if cited_owner else related
    endpoint = Endpoint({URI: external_graph(description=f"{TITLE} {cited_url}")})
    document = {
        "@graph": [
            {
                "@type": "Dataset",
                "@id": "https://source.test/own",
                "identifier": own,
                "isBasedOn": {"@id": "https://source.test/related"},
            },
            {
                "@type": "Dataset",
                "@id": "https://source.test/related",
                "identifier": related,
            },
        ]
    }
    async with httpx.AsyncClient(transport=httpx.MockTransport(endpoint)) as client:
        async with Harvester(
            Config(database=tmp_path / "catalog.sqlite"), client=client
        ) as harvester:
            run = harvester.store.start_run("full", "2026-10-21")
            with harvester.store.connection:
                seed_source(harvester.store, run)
                seed_representation(
                    harvester.store,
                    run,
                    "oai:researchdata.se:one/1",
                    "json_ld",
                    "<jsonLdString>" + escape(json.dumps(document)) + "</jsonLdString>",
                )
                if anchor:
                    seed_representation(
                        harvester.store,
                        run,
                        "oai:researchdata.se:one/1",
                        "datacite",
                        f'<resource xmlns="{DATACITE}"><identifier>{anchor}</identifier></resource>',
                    )
            stats = await harvester._dataportalen(run)
            assert stats["matched"] == int(expected)
            if expected:
                evidence = json.loads(
                    harvester.store.connection.execute(
                        "SELECT evidence FROM links"
                    ).fetchone()[0]
                )
                assert evidence["primary_url"] == own
                assert evidence["primary_url_field"] == "json_ld/Dataset/identifier"


@pytest.mark.parametrize("difference", ["date", "period", None])
async def test_coordinator_compares_all_citation_support_before_flagging_ambiguity(
    tmp_path: Path, difference: str | None
) -> None:
    primary = "https://doi.org/10.1234/own"
    first = {"date": DATE, "period": "2024-01-01"}
    second = {
        "date": "2026-02-01T00:00:00Z" if difference == "date" else DATE,
        "period": "2025-01-01" if difference == "period" else "2024-01-01",
    }
    endpoint = Endpoint(
        {
            URI: external_graph(description=f"{TITLE} {primary}", **first)
            + external_graph(description=f"{TITLE} {primary}", **second).replace(
                "_:period", "_:second_period"
            )
        }
    )
    async with httpx.AsyncClient(transport=httpx.MockTransport(endpoint)) as client:
        async with Harvester(
            Config(database=tmp_path / "catalog.sqlite"), client=client
        ) as harvester:
            run = harvester.store.start_run("full", "2026-10-21")
            with harvester.store.connection:
                for identifier, options in [
                    ("oai:researchdata.se:one/1", first),
                    ("oai:researchdata.se:two/1", second),
                ]:
                    seed_source(harvester.store, run, identifier, **options)
                    seed_representation(
                        harvester.store,
                        run,
                        identifier,
                        "json_ld",
                        "<jsonLdString>"
                        + escape(
                            json.dumps({"@type": "Dataset", "identifier": primary})
                        )
                        + "</jsonLdString>",
                    )
            await harvester._dataportalen(run)
            statuses = [
                row[0]
                for row in harvester.store.connection.execute(
                    "SELECT status FROM links"
                )
            ]
            assert statuses == ["matched" if difference else "ambiguous"] * 2


@pytest.mark.parametrize("anchor", [None, "10.1234/own", "10.1234/conflicting"])
def test_primary_urls_preserves_owner_provenance_and_excludes_nested_dataset(
    anchor: str | None,
) -> None:
    document = {
        "@type": "Dataset",
        "@id": "https://doi.org/10.1234/OWN",
        "identifier": PRIMARY_URL,
        "isBasedOn": {"@type": "Dataset", "identifier": "https://related.test/data"},
    }
    representations = {
        "json_ld": xml_tree(
            fromstring(
                "<jsonLdString>" + escape(json.dumps(document)) + "</jsonLdString>"
            )
        )
    }
    if anchor:
        representations["datacite"] = xml_tree(
            fromstring(
                f'<resource xmlns="{DATACITE}"><identifier>{anchor}</identifier></resource>'
            )
        )
    assert primary_urls(representations) == (
        ()
        if anchor == "10.1234/conflicting"
        else ((PRIMARY_URL, "json_ld/Dataset/identifier"),)
    )
    if anchor:
        assert representations["datacite"]["children"][0]["text"] == anchor
