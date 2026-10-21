# SPDX-License-Identifier: NOASSERTION
# Copyright (c) 2026 NordicIntel
"""Verify catalog scope, immutable resume inventory and retained SQLite history."""

import json
from urllib.parse import parse_qs

import httpx
import pytest

from researchdata_harvester.config import Config
from researchdata_harvester.harvester import Harvester
from researchdata_harvester.store import Store

CATALOG = "https://researchdata.se/catalogue/"
TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
DATASET = "http://www.w3.org/ns/dcat#dataset"
MEMBERS = ["https://data.test/a", "https://data.test/b", "https://data.test/c"]


def catalog_document(members):
    return {
        CATALOG: {
            TYPE: [{"type": "uri", "value": "http://www.w3.org/ns/dcat#Catalog"}],
            DATASET: [{"type": "uri", "value": uri} for uri in members],
        }
    }


def seed_dataportalen(store, uri, run):
    store.connection.execute(
        "INSERT INTO dataportalen_records(uri,identities,triples,seen_run,fetched_at,provenance) VALUES(?,'[]','[]',?,'old','{}')",
        (uri, run),
    )


async def test_catalog_pages_enrich_unmatched_and_resume_saved_inventory(tmp_path):
    config = Config(database=tmp_path / "catalog.sqlite", sparql_page_size=1, retries=0)
    requested = []
    interrupted = True
    inventory_calls = 0

    def handle(request):
        nonlocal inventory_calls
        if request.method == "GET":
            inventory_calls += 1
            # A second read would change both membership and sort order.
            members = (
                list(reversed(MEMBERS)) + [MEMBERS[0]] if inventory_calls == 1 else []
            )
            return httpx.Response(200, json=catalog_document(members))
        query = parse_qs(request.content.decode())["query"][0]
        assert "SELECT DISTINCT ?dataset WHERE" not in query
        if "DESCRIBE" not in query:
            return httpx.Response(200, json={"results": {"bindings": []}})
        uri = next(uri for uri in MEMBERS if f"<{uri}>" in query)
        requested.append(uri)
        if interrupted and uri == MEMBERS[1]:
            return httpx.Response(503)
        return httpx.Response(
            200,
            text=f'<{uri}> <http://purl.org/dc/terms/title> "Unmatched" .',
            headers={"Content-Type": "application/n-triples"},
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        async with Harvester(config, client=client) as harvester:
            store = harvester.store
            old = store.start_run("full", "2026-10-20")
            with store.connection:
                seed_dataportalen(store, "https://old.test/global", old)
            run = store.start_run("incremental", "2026-10-21")
            with pytest.raises(httpx.HTTPStatusError):
                await harvester._dataportalen(run)
            flags = dict(
                store.connection.execute(
                    "SELECT uri,in_scope FROM dataportalen_records"
                )
            )
            assert flags == {"https://old.test/global": 1, MEMBERS[0]: 0}
            saved = json.loads(
                store.connection.execute(
                    "SELECT details FROM checkpoints WHERE source='dataportalen:inventory' AND run_id=?",
                    (run,),
                ).fetchone()[0]
            )
            assert saved["members"] == MEMBERS
            assert saved["source"] == str(config.catalog_metadata_url)
            assert saved["catalog"] == CATALOG
            assert len(saved["sha256"]) == 64

        interrupted = False
        async with Harvester(config, client=client) as resumed:
            stats = await resumed._dataportalen(run)
            assert stats == {
                "scope": "researchdata",
                "scanned": 3,
                "matched": 0,
                "unresolved": 3,
                "no_identifier": 3,
            }
            assert inventory_calls == 1
            assert requested == [MEMBERS[0], MEMBERS[1], MEMBERS[1], MEMBERS[2]]
            rows = [
                dict(row)
                for row in resumed.store.connection.execute(
                    "SELECT * FROM dataportalen_records"
                )
            ]
            assert all(row["metadata_complete"] for row in rows if row["in_scope"])
            old_row = next(
                row for row in rows if row["uri"] == "https://old.test/global"
            )
            assert old_row["present"] == 1 and old_row["in_scope"] == 0
            status = resumed.store.status()
            assert status["dataportalen_records"] == 4
            assert status["dataportalen_in_scope"] == 3
            snapshot = resumed.store.export(tmp_path / "exports")
            exported = [
                json.loads(line)
                for line in (snapshot / "dataportalen.jsonl").read_text().splitlines()
            ]
            assert len(exported) == 4
            assert sum(row["in_scope"] for row in exported) == 3


@pytest.mark.parametrize(
    "document",
    [
        {},
        {
            CATALOG: {
                TYPE: [{"type": "uri", "value": "http://www.w3.org/ns/dcat#Dataset"}],
                DATASET: [],
            }
        },
        {
            CATALOG: {
                TYPE: [{"type": "uri", "value": "http://www.w3.org/ns/dcat#Catalog"}]
            }
        },
        {
            CATALOG: {
                TYPE: [{"type": "uri", "value": "http://www.w3.org/ns/dcat#Catalog"}],
                DATASET: [{"type": "literal", "value": MEMBERS[0]}],
            }
        },
        {
            CATALOG: {
                TYPE: [{"type": "uri", "value": "http://www.w3.org/ns/dcat#Catalog"}],
                DATASET: [{"type": "uri", "value": "relative"}],
            }
        },
    ],
)
async def test_malformed_catalog_preserves_scope_and_creates_no_inventory(
    tmp_path, document
):
    config = Config(database=tmp_path / "catalog.sqlite", retries=0)
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(200, json=document)
        )
    ) as client:
        async with Harvester(config, client=client) as harvester:
            run = harvester.store.start_run("full", "2026-10-21")
            with harvester.store.connection:
                seed_dataportalen(harvester.store, MEMBERS[0], run)
            with pytest.raises(ValueError):
                await harvester._dataportalen(run)
            assert harvester.store.status()["dataportalen_in_scope"] == 1
            assert (
                harvester.store.connection.execute(
                    "SELECT COUNT(*) FROM checkpoints"
                ).fetchone()[0]
                == 0
            )


async def test_explicit_empty_catalog_retires_scope_without_marking_absence(tmp_path):
    config = Config(database=tmp_path / "catalog.sqlite")
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(200, json=catalog_document([]))
        )
    ) as client:
        async with Harvester(config, client=client) as harvester:
            old = harvester.store.start_run("full", "2026-10-20")
            with harvester.store.connection:
                seed_dataportalen(harvester.store, MEMBERS[0], old)
            run = harvester.store.start_run("incremental", "2026-10-21")
            assert (await harvester._dataportalen(run))["scanned"] == 0
            assert tuple(
                harvester.store.connection.execute(
                    "SELECT present,in_scope FROM dataportalen_records"
                ).fetchone()
            ) == (1, 0)


def test_old_database_migration_preserves_records_and_matching_respects_scope(tmp_path):
    path = tmp_path / "catalog.sqlite"
    store = Store(path)
    run = store.start_run("full", "2026-10-21")
    with store.connection:
        seed_dataportalen(store, MEMBERS[0], run)
        store.connection.execute(
            "ALTER TABLE dataportalen_records DROP COLUMN in_scope"
        )
    store.close()
    store = Store(path)
    with store.connection:
        assert (
            store.connection.execute(
                "SELECT in_scope FROM dataportalen_records"
            ).fetchone()[0]
            == 1
        )
        store.connection.execute(
            "INSERT INTO research_records(identifier,dataset_id,identities,first_seen,last_seen,provenance,current_catalog) VALUES('id','dataset','[]','old','old','{}',1)"
        )
        store.connection.execute(
            "INSERT INTO links VALUES('id',?,'exact_identity','dataset','matched','[]',?)",
            (MEMBERS[0], run),
        )
    assert store.matching_summary()["matched"] == 1
    with store.connection:
        store.connection.execute("UPDATE dataportalen_records SET in_scope=0")
    assert store.matching_summary() == {"matched": 0, "ambiguous": 0, "unmatched": 1}
    store.close()


async def test_global_audit_uses_global_inventory_and_reconciles_absence(tmp_path):
    config = Config(database=tmp_path / "catalog.sqlite", dataportalen_scope="all")
    queries = []

    def handle(request):
        if request.method == "GET":
            return httpx.Response(200, json=catalog_document([]))
        query = parse_qs(request.content.decode())["query"][0]
        queries.append(query)
        rows = []
        if "SELECT DISTINCT ?dataset WHERE" in query and "FILTER(STR" not in query:
            rows = [{"dataset": {"type": "uri", "value": MEMBERS[0]}}]
        return httpx.Response(200, json={"results": {"bindings": rows}})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        async with Harvester(config, client=client) as harvester:
            old = harvester.store.start_run("full", "2026-10-20")
            with harvester.store.connection:
                seed_dataportalen(harvester.store, MEMBERS[1], old)
            run = harvester.store.start_run("full", "2026-10-21")
            stats = await harvester._dataportalen(run)
            assert stats["scope"] == "all" and stats["scanned"] == 1
            rows = {
                row["uri"]: dict(row)
                for row in harvester.store.connection.execute(
                    "SELECT * FROM dataportalen_records"
                )
            }
            assert rows[MEMBERS[0]]["in_scope"] == 1
            assert rows[MEMBERS[1]]["present"] == rows[MEMBERS[1]]["in_scope"] == 0
            assert len(queries) == 3


async def test_legacy_incomplete_scope_cannot_resume_filtered(tmp_path):
    config = Config(database=tmp_path / "catalog.sqlite")
    async with Harvester(config) as harvester:
        run = harvester.store.start_run("full", "2026-10-21")
        with harvester.store.connection:
            harvester.store.checkpoint(
                "scope",
                run,
                completed=True,
                details={"sparql_url": str(config.sparql_url)},
            )
        with pytest.raises(ValueError, match="Resume configuration differs"):
            await harvester.sync(full=True)
        assert (
            harvester.store.connection.execute(
                "SELECT status FROM runs WHERE id=?", (run,)
            ).fetchone()[0]
            == "incomplete"
        )
