# SPDX-License-Identifier: NOASSERTION
# Copyright (c) 2026 NordicIntel
"""Verify the output contract before adding retrieval and normalization code."""

import asyncio
import gzip
import json
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import httpx
import pytest

from harvester.vocabulary import (
    IDENTITY_PREDICATES,
    NAMESPACES,
    PREDICATE_COLUMNS,
    REGISTRY_PREDICATES,
    CacheFetchError,
    RdfCache,
)

SCHEMA = Path(__file__).parents[1] / "harvester" / "schema.sql"


def test_vocabulary_targets_the_sql_contract() -> None:
    with duckdb.connect() as connection:
        connection.execute(SCHEMA.read_text(encoding="utf-8"))
        for table in ("datasets", "dataportalen"):
            columns = set(connection.sql(f"SELECT * FROM {table}").columns)
            assert set(PREDICATE_COLUMNS["dataset"].values()) <= columns
        for context in ("agent", "distribution", "service"):
            data_type = connection.sql(f"SELECT NULL::{context}_metadata").types[0]
            fields = {name for name, _ in data_type.children}
            assert set(PREDICATE_COLUMNS[context].values()) <= fields

        modified = NAMESPACES["dct"] + "modified"
        assert REGISTRY_PREDICATES["registry_modified"] == modified
        assert PREDICATE_COLUMNS["dataset"][modified] == "modified"
        assert PREDICATE_COLUMNS["dataset"]["http://schema.org/name"] == "titles"
        assert PREDICATE_COLUMNS["dataset"]["https://schema.org/name"] == "titles"
        assert NAMESPACES["dct"] + "references" not in IDENTITY_PREDICATES
        assert NAMESPACES["dct"] + "isVersionOf" not in IDENTITY_PREDICATES
        assert NAMESPACES["dct"] + "title" not in IDENTITY_PREDICATES
        assert "https://example.org/unknown" not in PREDICATE_COLUMNS["dataset"]


def test_parquet_fidelity_and_publication_requirements(tmp_path: Path) -> None:
    timestamp = datetime(2026, 10, 22, tzinfo=UTC)
    dataset_uri = "https://example.org/dataset/1"
    original = {
        "value": "Temperatur",
        "value_type": "literal",
        "language": "sv",
        "datatype": None,
        "source_url": "https://example.org/metadata/1",
        "entity_id": dataset_uri,
        "graph_id": None,
        "property": NAMESPACES["dct"] + "title",
        "path": None,
    }
    titles = [original, {**original, "value": "Temperature", "language": "en"}]
    unknown = {
        **original,
        "entity_id": "_:stable-child",
        "property": "https://example.org/precision",
        "value": "001.50",
        "language": None,
        "datatype": NAMESPACES["xsd"] + "decimal",
    }
    reference = {
        **original,
        "property": "https://example.org/details",
        "value": "_:stable-child",
        "value_type": "bnode",
        "language": None,
    }
    resource = {
        "url": "https://example.org/data.csv",
        "source_url": original["source_url"],
        "entity_id": dataset_uri,
        "distribution_id": None,
        "filename": "data.csv",
        "outcome": "verified",
        "reason": None,
        "checked_at": timestamp,
        "verification": {
            "verified_at": timestamp,
            "resolved_url": "https://example.org/data.csv",
            "format": "csv",
            "media_type": "text/csv",
            "access_method": "anonymous",
            "sampled_rows": 2,
            "columns": [{"name": "temperature", "data_type": "DOUBLE"}],
            "transferred_bytes": 42,
            "decompressed_bytes": 42,
            "license_acknowledgement_url": None,
        },
    }
    with duckdb.connect() as connection:
        connection.execute(SCHEMA.read_text(encoding="utf-8"))
        connection.execute(
            "INSERT INTO datasets (dataset_uri, titles, resources, availability, "
            "properties) VALUES (?, ?, ?, 'available', ?)",
            [dataset_uri, titles, [resource], [*titles, unknown, reference]],
        )
        assert connection.sql("SELECT reason FROM publication_errors").fetchall() == [
            ("table_membership_mismatch",)
        ]
        connection.execute(
            "INSERT INTO dataportalen (dataset_uri, registry_entry_uri, metadata_url, "
            "registry_modified, registry_modified_value) VALUES (?, ?, ?, ?, ?)",
            [
                dataset_uri,
                "https://example.org/entry/1",
                original["source_url"],
                timestamp,
                "2026-10-22T02:00:00+02:00",
            ],
        )
        assert connection.sql("SELECT * FROM publication_errors").fetchall() == []
        with pytest.raises(duckdb.ConstraintException):
            connection.sql("INSERT INTO datasets SELECT * FROM datasets")
        with pytest.raises(duckdb.ConstraintException):
            connection.sql("UPDATE datasets SET resources = []")

        for table in ("datasets", "dataportalen"):
            path = tmp_path / f"{table}.parquet"
            connection.execute(
                f"COPY (SELECT * FROM {table} ORDER BY dataset_uri) TO ? "
                "(FORMAT PARQUET, COMPRESSION ZSTD)",
                [str(path)],
            )
            assert connection.execute(
                f"SELECT count(*) FROM ((SELECT * FROM {table} EXCEPT ALL "
                "SELECT * FROM read_parquet(?)) UNION ALL "
                f"(SELECT * FROM read_parquet(?) EXCEPT ALL SELECT * FROM {table}))",
                [str(path), str(path)],
            ).fetchall() == [(0,)]
        assert len(list(tmp_path.glob("*.parquet"))) == 2
        assert connection.sql("SELECT properties FROM datasets").fetchall()[0][0] == [
            *titles,
            unknown,
            reference,
        ]

        resource["outcome"] = "unavailable"
        resource["reason"] = "HTTP 503"
        connection.execute("UPDATE datasets SET resources = ?", [[resource]])
        assert connection.sql("SELECT reason FROM publication_errors").fetchall() == [
            ("available_without_current_verification",)
        ]
        connection.sql("UPDATE datasets SET availability = 'unavailable'")
        assert connection.sql("SELECT * FROM publication_errors").fetchall() == []
        resource["verification"] = None
        connection.execute("UPDATE datasets SET resources = ?", [[resource]])
        assert connection.sql("SELECT reason FROM publication_errors").fetchall() == [
            ("no_verified_tabular_resource",)
        ]


async def test_cache_refresh_sharing_and_failure_retention(tmp_path: Path) -> None:
    uri = "https://example.org/entity"
    path = tmp_path / "rdf-cache.json.gz"
    metadata = {
        uri: {
            "https://example.org/label": [
                {"type": "literal", "lang": "sv", "value": "Mätning"},
                {
                    "type": "literal",
                    "datatype": NAMESPACES["xsd"] + "decimal",
                    "value": "001.50",
                },
                {"type": "bnode", "value": "_:linked"},
            ]
        }
    }
    requests: list[httpx.Request] = []
    status = 200

    async def respond(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        await asyncio.sleep(0)
        return httpx.Response(
            status, json=metadata if status == 200 else None, headers={"ETag": '"one"'}
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        cache = RdfCache.load(path)
        assert not path.exists()
        results = await asyncio.gather(*(cache.fetch(uri, client) for _ in range(5)))
        assert len(requests) == 1
        assert all(entry.data == metadata for entry in results)
        results[0].data.clear()
        assert (await cache.fetch(uri, client)).data == metadata
        cache.save(path)

        status = 304
        refreshed = RdfCache.load(path)
        assert (await refreshed.fetch(uri, client)).data == metadata
        assert requests[-1].headers["If-None-Match"] == '"one"'
        assert refreshed.changes() == {}

        status = 503
        failed = RdfCache.load(path)
        entry = await failed.fetch(uri, client)
        assert entry.stale and entry.data == metadata
        count = len(requests)
        with pytest.raises(CacheFetchError):
            await failed.fetch(uri, client, required=True)
        assert len(requests) == count
        failed.save(path)
        status = 200
        assert not (await RdfCache.load(path).fetch(uri, client)).stale

        vocabulary = RdfCache()
        await vocabulary.fetch(uri, client, kind="vocabulary")
        vocabulary.save(path)
        count = len(requests)
        loaded = RdfCache.load(path)
        await loaded.fetch(uri, client, kind="vocabulary")
        assert len(requests) == count
        await loaded.fetch(uri, client, kind="vocabulary", full=True)
        assert len(requests) == count + 1

        status = 304
        with pytest.raises(CacheFetchError):
            await RdfCache().fetch(uri, client, required=True)


def test_cache_creation_merge_pruning_and_atomic_save(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "candidate" / "rdf-cache.json.gz"
    cache = RdfCache.load(path)
    cache.save(path)
    assert json.loads(gzip.decompress(path.read_bytes())) == {}
    update = {
        "https://example.org/entity": {
            "kind": "entity",
            "source_url": "https://example.org/metadata",
            "data": {"label": "Mätning"},
        },
        "https://example.org/vocabulary": {
            "kind": "vocabulary",
            "source_url": "https://example.org/vocabulary",
            "data": {"@graph": []},
        },
    }
    cache.merge(update, update)
    before = cache.changes()
    conflict = {
        "https://example.org/entity": {
            **update["https://example.org/entity"],
            "data": {"label": "Changed"},
        }
    }
    with pytest.raises(ValueError, match="Conflicting"):
        cache.merge(conflict)
    assert cache.changes() == before
    cache.save(path)
    original = path.read_bytes()
    modified = path.stat().st_mtime_ns
    cache.save(path)
    assert path.read_bytes() == original and path.stat().st_mtime_ns == modified
    second = tmp_path / "different-name.gz"
    cache.save(second)
    assert second.read_bytes() == original

    loaded = RdfCache.load(path)
    assert loaded.changes() == {}
    loaded.merge(conflict)

    def fail_replace(*arguments: object) -> None:
        raise OSError("simulated replacement failure")

    with monkeypatch.context() as patch:
        patch.setattr("harvester.vocabulary.os.replace", fail_replace)
        with pytest.raises(OSError, match="simulated"):
            loaded.save(path)
    assert path.read_bytes() == original
    assert list(path.parent.iterdir()) == [path]
    cache.save(path, referenced=set())
    assert list(json.loads(gzip.decompress(path.read_bytes()))) == [
        "https://example.org/vocabulary"
    ]
    path.write_bytes(b"not a gzip file")
    with pytest.raises(gzip.BadGzipFile):
        RdfCache.load(path)
