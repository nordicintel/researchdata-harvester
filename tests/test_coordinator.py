# SPDX-License-Identifier: NOASSERTION
# Copyright (c) 2026 NordicIntel
"""Exercise complete coordinator flows and transport failures with HTTP fixtures."""

import hashlib
import json
from urllib.parse import parse_qs
from xml.etree.ElementTree import fromstring

import httpx
import pytest

from researchdata_harvester.config import Config
from researchdata_harvester.dataportalen import DataportalenClient
from researchdata_harvester.downloads import download_file, safe_relative_path
from researchdata_harvester.harvester import Harvester
from researchdata_harvester.oai import parse_record
from researchdata_harvester.transport import Transport

NS = "http://www.openarchives.org/OAI/2.0/"
URI = "https://doi.org/10.1234/SAMPLE"
IDENTIFIER = "oai:researchdata.se:sample/1"


def test_retry_after_does_not_retry_before_the_servers_requested_delay():
    response = httpx.Response(429, headers={"Retry-After": "900"})
    assert Transport.delay(response, 0) == 900


@pytest.mark.parametrize(
    "landing",
    [
        '<script type="application/ld+json">{"@type":"Dataset","identifier":"oai:researchdata.se:sample/1"}</script>',
        '<a href="/catalogue/dataset/sample/1/export/json_ld">Metadata</a>',
    ],
)
async def test_sitemap_recovery_remains_current_without_rewriting_inventory(
    tmp_path, landing
):
    config = Config(database=tmp_path / "catalog.sqlite", formats=["datacite"])

    def handle(request):
        if request.url.params.get("verb") == "GetRecord":
            return httpx.Response(200, text=oai("GetRecord", item("datacite")))
        return httpx.Response(200, text=landing)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        async with Harvester(config, client=client) as harvester:
            run = harvester.store.start_run("full", "2026-10-21")
            connection = harvester.store.connection
            with connection:
                harvester.store.checkpoint("oai:inventory", run, completed=True)
                connection.execute(
                    "INSERT INTO sitemap_entries VALUES(?,?,?,1,?)",
                    (
                        "https://researchdata.se/sv/catalogue/dataset/sample",
                        "dataset",
                        run,
                        "{}",
                    ),
                )
            await harvester._reconcile(run)
            assert (
                connection.execute(
                    "SELECT current_catalog FROM research_records"
                ).fetchone()[0]
                == 1
            )
            assert (
                connection.execute("SELECT present FROM representations").fetchone()[0]
                == 1
            )
            assert (
                connection.execute("SELECT count(*) FROM current_inventory").fetchone()[
                    0
                ]
                == 0
            )
            details = json.loads(
                connection.execute(
                    "SELECT details FROM checkpoints WHERE source='reconciliation'"
                ).fetchone()[0]
            )
            assert details["missing_from_oai"] == ["sample"]


async def test_get_record_rejects_wrong_version(tmp_path):
    config = Config(database=tmp_path / "catalog.sqlite")
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(200, text=oai("GetRecord", item("datacite")))
        )
    ) as client:
        async with Harvester(config, client=client) as harvester:
            with pytest.raises(ValueError, match="instead of"):
                await harvester.oai.get_record("oai:researchdata.se:sample/2")


async def test_download_selection_rejects_unknown_urls_before_downloading(tmp_path):
    requested = []

    def handle(request):
        requested.append(str(request.url))
        return httpx.Response(200, content=b"data")

    config = Config(database=tmp_path / "catalog.sqlite")
    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        async with Harvester(config, client=client) as harvester:
            run = harvester.store.start_run("record", "2026-10-21")
            with harvester.store.connection:
                record = parse_record(
                    fromstring(
                        item("json_ld").replace("<record>", f'<record xmlns="{NS}">', 1)
                    )
                )
                harvester.store.put_record(record, "json_ld", run, "source")
                harvester.store.put_file(
                    IDENTIFIER,
                    {
                        "url": "https://files.test/second.csv",
                        "filename": "second.csv",
                        "size": 4,
                    },
                )
            selected = "https://files.test/download/123"
            with pytest.raises(ValueError, match="not current advertised"):
                await harvester.download(
                    IDENTIFIER,
                    tmp_path / "downloads",
                    urls=[selected, "https://files.test/unknown.csv"],
                )
            assert requested == []
            result = await harvester.download(
                IDENTIFIER, tmp_path / "downloads", urls=[selected]
            )
            assert requested == [selected]
            assert result[0]["status"] == "verified"
            assert not (tmp_path / "downloads" / "second.csv").exists()


def oai(verb, body):
    return f'<OAI-PMH xmlns="{NS}"><{verb}>{body}</{verb}></OAI-PMH>'


def item(prefix):
    if prefix == "json_ld":
        payload = (
            '<jsonLdString xmlns="https://example.org/jsonld"><![CDATA['
            + json.dumps(
                {
                    "@type": "Dataset",
                    "identifier": URI,
                    "name": "Water",
                    "distribution": [
                        {
                            "@type": "DataDownload",
                            "name": "tables/data.csv",
                            "contentUrl": "https://files.test/download/123",
                            "contentSize": "4",
                            "sha256": hashlib.sha256(b"data").hexdigest(),
                        }
                    ],
                }
            )
            + "]]></jsonLdString>"
        )
    else:
        payload = f'<resource xmlns="http://datacite.org/schema/kernel-4"><identifier identifierType="DOI">{URI}</identifier><title xml:lang="en">Water</title></resource>'
    return f"<record><header><identifier>{IDENTIFIER}</identifier><datestamp>2026-10-20</datestamp></header><metadata>{payload}</metadata></record>"


def term(value):
    return {"type": "uri", "value": value}


async def test_full_sync_cross_format_repair_incremental_inventory_and_snapshot(
    tmp_path,
):
    calls = []

    def handle(request):
        calls.append((str(request.url), request.content))
        if request.url.path == "/oai":
            args = dict(request.url.params)
            verb = args["verb"]
            if verb == "Identify":
                body = "<granularity>YYYY-MM-DD</granularity><deletedRecord>no</deletedRecord>"
            elif verb == "ListMetadataFormats":
                body = "".join(
                    f"<metadataFormat><metadataPrefix>{prefix}</metadataPrefix></metadataFormat>"
                    for prefix in ["datacite", "dcat", "json_ld"]
                )
            elif verb == "ListSets":
                body = "<set><setSpec>all</setSpec></set>"
            elif verb == "ListIdentifiers":
                body = f"<header><identifier>{IDENTIFIER}</identifier><datestamp>2026-10-20</datestamp></header>"
            elif verb == "ListRecords":
                body = (
                    ""
                    if args["metadataPrefix"] == "dcat"
                    else item(args["metadataPrefix"])
                )
            elif verb == "GetRecord":
                body = item(args["metadataPrefix"])
            else:
                raise AssertionError(args)
            return httpx.Response(200, text=oai(verb, body))
        if request.url.path == "/sitemap":
            return httpx.Response(
                200,
                text='<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9"><sitemap><loc>https://source.test/child</loc></sitemap></sitemapindex>',
            )
        if request.url.path == "/child":
            return httpx.Response(
                200,
                text="<urlset><url><loc>https://source.test/dataset/sample</loc></url><url><loc>https://source.test/collection/example</loc></url></urlset>",
            )
        if request.url.path == "/catalog":
            return httpx.Response(
                200,
                json={
                    "https://researchdata.se/catalogue/": {
                        "http://www.w3.org/1999/02/22-rdf-syntax-ns#type": [
                            term("http://www.w3.org/ns/dcat#Catalog")
                        ],
                        "http://www.w3.org/ns/dcat#dataset": [term(URI)],
                    }
                },
            )
        if request.url.path == "/sparql":
            query = parse_qs(request.content.decode())["query"][0]
            if "DESCRIBE" in query:
                return httpx.Response(
                    200,
                    text=f'<{URI}> <http://purl.org/dc/terms/title> "Water"@en .\n',
                    headers={"Content-Type": "application/n-triples"},
                )
            if "SELECT DISTINCT ?dataset WHERE" in query:
                rows = [] if "FILTER(STR" in query else [{"dataset": term(URI)}]
            else:
                rows = [
                    {
                        "dataset": term(URI),
                        "field": term("http://purl.org/dc/terms/identifier"),
                        "value": {"type": "literal", "value": "10.1234/SAMPLE"},
                    }
                ]
            return httpx.Response(200, json={"results": {"bindings": rows}})
        raise AssertionError(str(request.url))

    config = Config(
        database=tmp_path / "catalog.sqlite",
        oai_url="https://source.test/oai",
        sitemap_url="https://source.test/sitemap",
        sparql_url="https://source.test/sparql",
        catalog_metadata_url="https://source.test/catalog",
        landing_fallback=False,
        retries=0,
    )
    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        async with Harvester(config, client=client) as harvester:
            result = await harvester.sync(full=True)
            assert result["status"] == "complete"
            assert result["dataportalen"]["scanned"] == 1
            assert result["dataportalen"]["matched"] == 1
            assert result["researchdata_matching"] == {
                "matched": 1,
                "unmatched": 0,
                "ambiguous": 0,
            }
            assert (
                harvester.store.status()["researchdata_matching"]
                == result["researchdata_matching"]
            )
            record = harvester.store.record(IDENTIFIER)
            assert record["current_catalog"] == 1
            assert {
                representation["format"] for representation in record["representations"]
            } == {"datacite", "dcat", "json_ld"}
            assert (
                harvester.store.file_entries(IDENTIFIER)[0]["metadata"]["name"]
                == "tables/data.csv"
            )
            checkpoint = harvester.store.connection.execute(
                "SELECT details FROM checkpoints WHERE source='reconciliation'"
            ).fetchone()
            assert json.loads(checkpoint[0])["sitemap_datasets"] == 1
            snapshot = harvester.store.export(tmp_path / "exports")
            assert len((snapshot / "links.jsonl").read_text().splitlines()) == 1
            before = sum(
                "/sparql" in url and b"SELECT+DISTINCT+%3Fdataset+WHERE" in content
                for url, content in calls
            )
            await harvester.sync(full=False)
            after = sum(
                "/sparql" in url and b"SELECT+DISTINCT+%3Fdataset+WHERE" in content
                for url, content in calls
            )
            assert after == before == 0
            assert sum("/catalog" in url for url, _ in calls) == 2
            incremental_calls = [
                url for url, _ in calls if "verb=ListRecords" in url and "from=" in url
            ]
            assert len(incremental_calls) == 3


async def test_rdf_term_languages_nested_blank_nodes_and_scoping(tmp_path):
    content = f"""<{URI}> <http://www.w3.org/ns/prov#qualifiedAttribution> _:a .
_:a <http://www.w3.org/ns/prov#agent> <https://publisher.test/> .
_:a <https://example.test/nested> _:b .
_:b <https://example.test/value> "Hej"@sv .
<https://publisher.test/> <http://xmlns.com/foaf/0.1/name> "Institute" ."""
    config = Config(database=tmp_path / "catalog.sqlite", retries=0)
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(
                200, text=content, headers={"Content-Type": "application/n-triples"}
            )
        )
    ) as client:
        api = DataportalenClient(
            Transport(config, client), "https://source.test/sparql"
        )
        first = (await api.metadata([URI]))[URI]
        second = (await api.metadata([URI]))[URI]
    assert len(first) == 5
    assert (
        next(triple["o"] for triple in first if triple["o"]["value"] == "Hej")[
            "xml:lang"
        ]
        == "sv"
    )
    first_blanks = {
        triple["s"]["value"] for triple in first if triple["s"]["type"] == "bnode"
    }
    second_blanks = {
        triple["s"]["value"] for triple in second if triple["s"]["type"] == "bnode"
    }
    assert first_blanks.isdisjoint(second_blanks)


class BrokenBody(httpx.AsyncByteStream):
    async def __aiter__(self):
        yield b"partial"
        raise httpx.ReadError("disconnected")


async def test_interrupted_download_leaves_no_partial_file(tmp_path):
    config = Config(database=tmp_path / "catalog.sqlite", retries=0)
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(200, stream=BrokenBody())
        )
    ) as client:
        outcome = await download_file(
            Transport(config, client),
            {"url": "https://files.test/file.csv", "size": 100},
            tmp_path / "downloads",
        )
    assert outcome["status"] == "failed"
    assert list((tmp_path / "downloads").iterdir()) == []


async def test_declared_filename_and_html_login_are_not_success(tmp_path):
    assert (
        safe_relative_path(
            "https://files.test/download/123", "data/table.csv"
        ).as_posix()
        == "data/table.csv"
    )
    for name in ["../table.csv", "/absolute.csv", "CON.txt", "data/../../outside.csv"]:
        with pytest.raises(ValueError):
            safe_relative_path("https://files.test/download/123", name)
    config = Config(database=tmp_path / "catalog.sqlite", retries=0)
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(
                200, text="<html>Sign in</html>", headers={"Content-Type": "text/html"}
            )
        )
    ) as client:
        outcome = await download_file(
            Transport(config, client),
            {
                "url": "https://files.test/download/123",
                "name": "data/table.csv",
                "encoding_format": "text/csv",
            },
            tmp_path / "downloads",
        )
    assert outcome["status"] == "failed"
    assert not (tmp_path / "downloads/data/table.csv").exists()


async def test_retry_after_and_bounded_network_retry(tmp_path, monkeypatch):
    waits = []

    async def sleep(delay):
        waits.append(delay)

    monkeypatch.setattr("researchdata_harvester.transport.asyncio.sleep", sleep)
    count = 0

    def handle(request):
        nonlocal count
        count += 1
        return (
            httpx.Response(429, headers={"Retry-After": "2"})
            if count == 1
            else httpx.Response(200, text="ready")
        )

    config = Config(database=tmp_path / "catalog.sqlite", retries=1)
    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        response = await Transport(config, client).request(
            "GET", "https://source.test/"
        )
    assert response.text == "ready"
    assert waits == [2]


async def test_refreshed_landing_metadata_retires_removed_file_urls(tmp_path):
    config = Config(
        database=tmp_path / "catalog.sqlite", retries=0, formats=["datacite"]
    )
    response = (
        '<html><script type="application/ld+json">'
        + json.dumps({"@type": "Dataset", "name": "Water", "distribution": []})
        + "</script></html>"
    )
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(200, text=response)
        )
    ) as client:
        async with Harvester(config, client=client) as harvester:
            old = harvester.store.start_run("full", "2026-10-20")
            record = parse_record(
                fromstring(
                    f'<record xmlns="{NS}"><header><identifier>{IDENTIFIER}</identifier><datestamp>2026-10-20</datestamp></header><metadata><resource xmlns="http://datacite.org/schema/kernel-4"><identifier>{URI}</identifier></resource></metadata></record>'
                )
            )
            with harvester.store.connection:
                harvester.store.put_record(
                    record, "datacite", old, "source", catalogue=True
                )
                harvester.store.put_file(
                    IDENTIFIER,
                    {"url": "https://files.test/removed.csv", "landing_url": URI},
                )
            current = harvester.store.start_run("incremental", "2026-10-21")
            with harvester.store.connection:
                harvester.store.put_record(
                    record, "datacite", current, "source", catalogue=True
                )
            outcome = await harvester._discover_files(current)
            assert outcome == {"no_files_in_landing_metadata": 1}
            assert harvester.store.file_entries(IDENTIFIER) == []
            assert (
                harvester.store.file_entries(IDENTIFIER, include_absent=True)[0][
                    "present"
                ]
                == 0
            )
