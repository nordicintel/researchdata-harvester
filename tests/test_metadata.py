# SPDX-License-Identifier: NOASSERTION
# Copyright (c) 2026 NordicIntel
"""Verify the output contract before adding retrieval and normalization code."""

from datetime import UTC, datetime
from pathlib import Path

import duckdb
import pytest

from harvester.vocabulary import (
    IDENTITY_PREDICATES,
    NAMESPACES,
    PREDICATE_COLUMNS,
    REGISTRY_PREDICATES,
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
