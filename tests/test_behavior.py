# SPDX-License-Identifier: NOASSERTION
# Copyright (c) 2026 NordicIntel
"""Behavior checks for source pagination, durable transactions and identity evidence."""

import hashlib
import json
import sqlite3
from xml.etree.ElementTree import fromstring

import httpx
import pytest

from researchdata_harvester.config import Config
from researchdata_harvester.dataportalen import match_identities
from researchdata_harvester.downloads import download_file
from researchdata_harvester.harvester import Harvester
from researchdata_harvester.metadata import extract_evidence, normalize_identity
from researchdata_harvester.oai import OAIClient, Page, parse_record, xml_tree
from researchdata_harvester.store import Store
from researchdata_harvester.transport import Transport

NS = "http://www.openarchives.org/OAI/2.0/"


def record_xml(identifier="oai:researchdata.se:sample/1", doi="10.1234/PRIMARY"):
    return f'''<record xmlns="{NS}"><header><identifier>{identifier}</identifier><datestamp>2026-10-20</datestamp></header><metadata><resource xmlns="http://datacite.org/schema/kernel-4"><identifier identifierType="DOI">{doi}</identifier><titles><title xml:lang="en">Water</title><title xml:lang="sv">Vatten</title></titles><publisher>Institute</publisher><relatedIdentifiers><relatedIdentifier relationType="Cites">10.1234/CITATION</relatedIdentifier><relatedIdentifier relationType="IsVersionOf">10.1234/CONCEPT</relatedIdentifier></relatedIdentifiers></resource></metadata></record>'''


def record(identifier="oai:researchdata.se:sample/1", doi="10.1234/PRIMARY"):
    return parse_record(fromstring(record_xml(identifier, doi)))


def envelope(verb, body):
    return f'<OAI-PMH xmlns="{NS}"><{verb}>{body}</{verb}></OAI-PMH>'


@pytest.fixture
def config(tmp_path):
    return Config(
        database=tmp_path / "catalog.sqlite",
        retries=0,
        formats=["datacite"],
        landing_fallback=False,
    )


async def test_complete_pagination_preserves_opaque_tokens_and_languages(config):
    calls = []

    def handle(request):
        calls.append(dict(request.url.params))
        if len(calls) == 1:
            return httpx.Response(
                200,
                text=envelope(
                    "ListRecords",
                    record_xml()
                    + '<resumptionToken completeListSize="2">opaque +/=</resumptionToken>',
                ),
            )
        return httpx.Response(
            200,
            text=envelope(
                "ListRecords",
                record_xml("oai:researchdata.se:second/2") + "<resumptionToken/>",
            ),
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        transport = Transport(config, client)
        records = [
            item
            async for item in OAIClient(
                transport, "https://source.test/oai"
            ).iter_records()
        ]
    assert len(records) == 2
    assert calls[1] == {"verb": "ListRecords", "resumptionToken": "opaque +/="}
    titles = records[0].metadata["children"][1]["children"]
    assert titles[1]["attributes"]["{http://www.w3.org/XML/1998/namespace}lang"] == "sv"
    assert "http://datacite.org/schema/kernel-4" in titles[0]["tag"]


async def test_repeated_and_malformed_tokens_are_incomplete(config):
    def handle(request):
        return httpx.Response(
            200,
            text=envelope(
                "ListRecords", record_xml() + "<resumptionToken>same</resumptionToken>"
            ),
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        oai = OAIClient(Transport(config, client), "https://source.test/oai")
        with pytest.raises(ValueError, match="repeated"):
            _ = [item async for item in oai.iter_records()]


async def test_empty_no_records_is_complete(config):
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(
                200,
                text=f'<OAI-PMH xmlns="{NS}"><error code="noRecordsMatch">empty</error></OAI-PMH>',
            )
        )
    ) as client:
        items = [
            item
            async for item in OAIClient(
                Transport(config, client), "https://source.test/oai"
            ).iter_records()
        ]
    assert items == []


def test_identity_normalization_excludes_citations_and_preserves_url_distinctions():
    evidence = extract_evidence(record().metadata)
    assert {item["normalized"] for item in evidence} == {
        "doi:10.1234/primary",
        "doi:10.1234/concept",
    }
    assert (
        next(item for item in evidence if item["normalized"].endswith("concept"))[
            "relation"
        ]
        == "dataset"
    )
    assert normalize_identity("https://doi.org/10.1234%2FABC") == "doi:10.1234/abc"
    assert normalize_identity("https://example.org/a%2Fb#one") != normalize_identity(
        "https://example.org/a/b#two"
    )
    assert normalize_identity("http://example.org/a") != normalize_identity(
        "https://example.org/a"
    )


def test_multiple_matches_versions_and_conflicting_datasets():
    primary = {
        "value": "10.1234/X",
        "normalized": "doi:10.1234/x",
        "field": "identifier",
        "relation": "version",
    }
    concept = dict(primary, relation="dataset")
    research = {
        "oai:researchdata.se:a/1": [concept],
        "oai:researchdata.se:a/2": [concept],
    }
    assert {item["status"] for item in match_identities(research, [primary])} == {
        "matched"
    }
    research["oai:researchdata.se:b/1"] = [primary]
    assert {item["status"] for item in match_identities(research, [primary])} == {
        "ambiguous"
    }
    assert match_identities(research, []) == []
    assert (
        match_identities(
            research, [dict(primary, field="http://www.w3.org/ns/dcat#downloadURL")]
        )
        == []
    )


def test_page_transaction_rolls_back_records_and_checkpoint(config):
    store = Store(config.database)
    run = store.start_run("full", "2026-10-21")
    with pytest.raises(RuntimeError), store.connection:
        store.put_record(
            record(), "datacite", run, "https://source.test/oai", catalogue=True
        )
        store.checkpoint("oai:datacite", run, token="next")
        raise RuntimeError("interrupted")
    assert store.record(record().identifier) is None
    assert (
        store.connection.execute("SELECT COUNT(*) FROM checkpoints").fetchone()[0] == 0
    )
    store.close()


def test_replay_updates_identity_without_retaining_removed_values(config):
    store = Store(config.database)
    run = store.start_run("full", "2026-10-21")
    with store.connection:
        for doi in ["10.1234/OLD", "10.1234/NEW", "10.1234/NEW"]:
            store.put_record(
                record(doi=doi),
                "datacite",
                run,
                "https://source.test/oai",
                catalogue=True,
            )
    result = store.record(record().identifier)
    assert not any(
        item["normalized"] == "doi:10.1234/old" for item in result["identities"]
    )
    assert len(result["representations"]) == 1
    assert len(result["representations"][0]["content_hash"]) == 64
    store.close()


async def test_failed_scan_does_not_mark_absence_and_expired_token_restarts_same_interval(
    config,
):
    async with Harvester(config) as harvester:
        run = harvester.store.start_run("full", "2026-10-21")
        with harvester.store.connection:
            harvester.store.put_record(
                record(), "datacite", run, "source", catalogue=True
            )
            harvester.store.checkpoint(
                "oai:datacite", run, until_date="2026-10-21", token="expired"
            )
        calls = []

        async def pages(verb, **options):
            calls.append(options)
            if options["resumption_token"] == "expired":
                from researchdata_harvester.oai import OAIError

                raise OAIError("badResumptionToken", "expired")
            yield Page([record()], "next", 2, "<page/>")
            raise ValueError("malformed next page")

        harvester.oai.iter_pages = pages
        with pytest.raises(ValueError, match="malformed"):
            await harvester._research_stream("datacite", run, None, "2026-10-21")
        assert calls[1]["resumption_token"] is None
        assert calls[1]["until"] == "2026-10-21"
        assert (
            harvester.store.record(record().identifier)["representations"][0]["present"]
            == 1
        )


@pytest.mark.parametrize(
    "status,expected", [(403, "restricted"), (404, "unavailable"), (500, "failed")]
)
async def test_download_http_outcomes(config, tmp_path, status, expected):
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(status))
    ) as client:
        outcome = await download_file(
            Transport(config, client),
            {"url": "https://file.test/data.csv"},
            tmp_path / "downloads",
        )
    assert outcome["status"] == expected
    assert not list((tmp_path / "downloads").glob(".download-*"))


async def test_download_redirect_checksum_reuse_and_conflict(config, tmp_path):
    body = b"column\nvalue\n"

    def handle(request):
        return (
            httpx.Response(302, headers={"Location": "https://file.test/final"})
            if request.url.path == "/file/data/a.csv"
            else httpx.Response(200, content=body)
        )

    metadata = {
        "url": "https://file.test/file/data/a.csv",
        "size": len(body),
        "sha256": hashlib.sha256(body).hexdigest(),
    }
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handle), follow_redirects=True
    ) as client:
        transport = Transport(config, client)
        assert (await download_file(transport, metadata, tmp_path))[
            "status"
        ] == "verified"
        assert (tmp_path / "data/a.csv").read_bytes() == body
        assert (await download_file(transport, metadata, tmp_path))[
            "status"
        ] == "verified"
        (tmp_path / "data/a.csv").write_bytes(b"conflict")
        assert (await download_file(transport, metadata, tmp_path))[
            "status"
        ] == "conflict"
        assert (tmp_path / "data/a.csv").read_bytes() == b"conflict"
        metadata["url"] = "https://file.test/other.csv"
        metadata["sha256"] = "0" * 64
        assert (await download_file(transport, metadata, tmp_path))[
            "status"
        ] == "failed"
        assert not (tmp_path / "other.csv").exists()


def test_atomic_export_separate_payloads_and_record_does_not_advance_boundary(
    config, tmp_path
):
    store = Store(config.database)
    sync = store.start_run("full", "2026-10-19")
    store.finish_run(sync, "complete", {})
    single = store.start_run("record", "2026-10-21")
    with store.connection:
        store.put_record(record(), "datacite", single, "source")
    store.finish_run(single, "complete", {})
    assert store.last_boundary() == "2026-10-19"
    snapshot = store.export(tmp_path / "export")
    assert (tmp_path / "export/CURRENT").read_text().strip() == snapshot.name
    assert (
        json.loads((snapshot / "researchdata.jsonl").read_text())["identifier"]
        == record().identifier
    )
    assert (snapshot / "dataportalen.jsonl").read_text() == ""
    store.close()


def test_search_includes_multilingual_titles_and_publisher(config):
    store = Store(config.database)
    run = store.start_run("full", "2026-10-21")
    with store.connection:
        store.put_record(record(), "datacite", run, "source", catalogue=True)
    result = store.search("Vatten")
    assert result[0]["titles"] == [
        {"value": "Water", "language": "en"},
        {"value": "Vatten", "language": "sv"},
    ]
    assert result[0]["publishers"] == [{"value": "Institute", "language": None}]
    store.close()


async def test_resumed_finished_page_finalizes_presence(config):
    async with Harvester(config) as harvester:
        old_run = harvester.store.start_run("full", "2026-10-20")
        with harvester.store.connection:
            harvester.store.put_record(
                record(), "datacite", old_run, "source", catalogue=True
            )
        new_run = harvester.store.start_run("full", "2026-10-21")
        with harvester.store.connection:
            harvester.store.checkpoint(
                "oai:datacite", new_run, until_date="2026-10-21", completed=True
            )
        await harvester._research_stream("datacite", new_run, None, "2026-10-21")
        assert (
            harvester.store.record(record().identifier)["representations"][0]["present"]
            == 0
        )


async def test_on_demand_record_cannot_reactivate_an_absent_catalog_version(config):
    async with Harvester(config) as harvester:
        first = harvester.store.start_run("full", "2026-10-20")
        with harvester.store.connection:
            harvester.store.put_record(
                record(), "datacite", first, "source", catalogue=True
            )
            harvester.store.connection.execute("UPDATE representations SET present=0")
            harvester.store.connection.execute(
                "UPDATE research_records SET current_catalog=0"
            )
        second = harvester.store.start_run("record", "2026-10-21")
        with harvester.store.connection:
            harvester.store.put_record(record(), "datacite", second, "source")
        item = harvester.store.record(record().identifier)
        assert item["current_catalog"] == 0
        assert item["representations"][0]["present"] == 0
        assert item["representations"][0]["seen_run"] == first


def test_removed_file_urls_are_retained_but_not_download_candidates(config):
    store = Store(config.database)
    run = store.start_run("full", "2026-10-21")
    with store.connection:
        store.put_record(record(), "datacite", run, "source", catalogue=True)
        store.put_file(
            record().identifier, {"url": "https://files.test/old.csv", "size": 10}
        )
        store.put_record(record(), "json_ld", run, "source", catalogue=True)
    assert store.file_entries(record().identifier) == []
    assert (
        store.file_entries(record().identifier, include_absent=True)[0]["present"] == 0
    )
    store.close()


async def test_unfiltered_inventory_replaces_current_version_after_incremental_feed(
    config,
):
    async with Harvester(config) as harvester:
        first = harvester.store.start_run("full", "2026-10-20")
        with harvester.store.connection:
            harvester.store.put_record(
                record(), "datacite", first, "source", catalogue=True
            )
            harvester.store.connection.execute(
                "INSERT INTO sitemap_entries VALUES(?,?,?,1,?)",
                (
                    "https://researchdata.se/catalogue/dataset/sample",
                    "dataset",
                    first,
                    "{}",
                ),
            )
        second = harvester.store.start_run("incremental", "2026-10-21")
        current = "oai:researchdata.se:sample/2"

        async def pages(verb, **options):
            assert verb == "ListIdentifiers"
            assert options["metadata_prefix"] == "oai_dc"
            assert "from_" not in options
            header = xml_tree(
                fromstring(
                    f'<header xmlns="{NS}"><identifier>{current}</identifier><datestamp>2026-10-21</datestamp></header>'
                )
            )
            yield Page([header], None, 1, "<page/>")

        async def get_record(identifier, prefix):
            assert identifier == current
            return record(identifier, "10.1234/NEWVERSION")

        harvester.oai.iter_pages = pages
        harvester.oai.get_record = get_record
        await harvester._current_inventory(second)
        await harvester._reconcile(second)
        assert harvester.store.record(record().identifier)["current_catalog"] == 0
        assert harvester.store.record(current)["current_catalog"] == 1
        assert (
            harvester.store.record(record().identifier)["representations"][0]["present"]
            == 0
        )


def test_existing_file_schema_is_migrated_without_losing_history(config):

    connection = sqlite3.connect(config.database)
    connection.execute(
        "CREATE TABLE files (identifier TEXT NOT NULL,url TEXT NOT NULL,metadata JSON NOT NULL,status TEXT NOT NULL DEFAULT 'discovered',local_path TEXT,error TEXT,fetched_at TEXT,PRIMARY KEY(identifier,url))"
    )
    connection.execute(
        "INSERT INTO files(identifier,url,metadata) VALUES('old','https://files.test/old.csv','{}')"
    )
    connection.commit()
    connection.close()
    store = Store(config.database)
    assert store.file_entries("old")[0]["present"] == 1
    store.close()


def test_nested_distribution_identity_does_not_become_dataset_identity():
    metadata = xml_tree(
        fromstring(
            '<RDF xmlns="http://www.w3.org/1999/02/22-rdf-syntax-ns#" xmlns:dcat="http://www.w3.org/ns/dcat#" xmlns:dct="http://purl.org/dc/terms/"><dcat:Dataset><dct:identifier>10.1234/DATASET</dct:identifier><dcat:distribution><dcat:Distribution><dct:identifier>10.1234/FILE</dct:identifier></dcat:Distribution></dcat:distribution></dcat:Dataset></RDF>'
        )
    )
    assert {item["normalized"] for item in extract_evidence(metadata)} == {
        "doi:10.1234/dataset"
    }
