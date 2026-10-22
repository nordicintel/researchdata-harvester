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
    SemanticResolver,
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
        assert connection.sql("SELECT reason FROM publication_errors").fetchall() == [("table_membership_mismatch",)]
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
                f"COPY (SELECT * FROM {table} ORDER BY dataset_uri) TO ? (FORMAT PARQUET, COMPRESSION ZSTD)",
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
        assert connection.sql("SELECT reason FROM publication_errors").fetchall() == [("no_verified_tabular_resource",)]


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
        return httpx.Response(status, json=metadata if status == 200 else None, headers={"ETag": '"one"'})

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


def test_cache_creation_merge_pruning_and_atomic_save(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
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
    unavailable = {
        "https://example.org/entity": {
            **update["https://example.org/entity"],
            "outcome": "unavailable",
            "error": "ReadTimeout",
            "retry": True,
        }
    }
    for order in ((update, unavailable), (unavailable, update)):
        merged = RdfCache()
        merged.merge(*order)
        assert merged.changes()["https://example.org/entity"]["outcome"] == "available"
        assert merged.changes()["https://example.org/entity"]["data"] == {"label": "Mätning"}
    other_failure = {
        "https://example.org/entity": {
            **unavailable["https://example.org/entity"],
            "error": "HTTP 503",
        }
    }
    merged = RdfCache()
    merged.merge(unavailable, other_failure)
    assert merged.changes()["https://example.org/entity"]["retry"] is True
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
    assert list(json.loads(gzip.decompress(path.read_bytes()))) == ["https://example.org/vocabulary"]
    path.write_bytes(b"not a gzip file")
    with pytest.raises(gzip.BadGzipFile):
        RdfCache.load(path)


async def test_resolver_expansion_semantics_and_automatic_artifact(
    tmp_path: Path,
) -> None:
    namespace = "https://schema.org/"
    context_url = "https://schema.org/"
    context_document = "https://schema.org/docs/jsonldcontext.jsonld"
    definition_url = "https://vocab.example/definitions"
    source_url = "https://data.example/metadata"
    citation = "https://publication.example/do-not-fetch"
    context = {
        "v": namespace,
        "title": "v:title",
        "details": "v:details",
        "value": {"@id": "v:value", "@type": NAMESPACES["xsd"] + "decimal"},
        "citation": {"@id": "v:citation", "@type": "@id"},
    }
    metadata = {
        "@context": context_url,
        "@id": "https://data.example/dataset",
        "title": {"@value": "Mätning", "@language": "sv"},
        "details": {"@id": "_:first", "value": "001.50"},
        "citation": citation,
        "notDefined": {"keep": [1, 2]},
    }
    rdf_json = {
        "https://data.example/dataset": {
            namespace + "title": [{"type": "literal", "lang": "sv", "value": "Mätning"}],
            namespace + "details": [{"type": "bnode", "value": "_:different"}],
            namespace + "citation": [{"type": "uri", "value": citation}],
        },
        "_:different": {
            namespace + "value": [
                {
                    "type": "literal",
                    "datatype": NAMESPACES["xsd"] + "decimal",
                    "value": "001.50",
                }
            ]
        },
    }
    requests: list[str] = []

    def respond(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        requests.append(url)
        if request.headers.get("if-none-match") == '"one"':
            return httpx.Response(304)
        if url == context_document:
            return httpx.Response(200, json={"@context": context}, headers={"ETag": '"one"'})
        assert url == definition_url
        return httpx.Response(
            200,
            text=f'<{namespace}title> <{NAMESPACES["rdfs"]}label> "Titel"@sv .',
            headers={"Content-Type": "text/turtle", "ETag": '"one"'},
        )

    baseline, updates = tmp_path / "rdf-cache.json.gz", tmp_path / "updates.json.gz"
    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        async with SemanticResolver(baseline, client, updates, documents={namespace: definition_url}) as resolver:
            first, second = await asyncio.gather(
                resolver.resolve(metadata, source_url=source_url),
                resolver.resolve(rdf_json, source_url=source_url),
            )
            assert not updates.exists() and not baseline.exists()
            assert first.properties == second.properties
            assert first.source == metadata
            assert "notDefined" in first.unmapped_properties
            assert first.semantics[namespace + "title"]["labels"][0]["value"] == "Titel"
            assert any(row["value"] == "001.50" for row in first.properties)
            assert any(row["language"] == "sv" for row in first.properties)
        assert sorted(requests) == sorted([context_document, definition_url])
        assert not baseline.exists()
        cache = RdfCache.load(baseline)
        cache.merge(json.loads(gzip.decompress(updates.read_bytes())))
        cache.save(baseline)
        original = baseline.read_bytes()
        async with SemanticResolver(baseline, client, updates, documents={namespace: definition_url}) as resolver:
            again = await resolver.resolve(metadata, source_url=source_url)
            assert again == first
        assert len(requests) == 2
        assert json.loads(gzip.decompress(updates.read_bytes())) == {}
        assert baseline.read_bytes() == original
        async with SemanticResolver(
            baseline, client, updates, full=True, documents={namespace: definition_url}
        ) as resolver:
            await resolver.resolve(metadata, source_url=source_url)
        assert len(requests) == 4


async def test_resolver_failure_boundaries(tmp_path: Path) -> None:
    predicate = "https://missing.example/term"
    metadata = {
        "https://data.example/dataset": {
            predicate: [{"type": "literal", "value": "preserved"}],
        }
    }
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda request: httpx.Response(503))) as client:
        baseline, updates = tmp_path / "cache.gz", tmp_path / "updates.gz"
        async with SemanticResolver(baseline, client, updates) as resolver:
            result = await resolver.resolve(metadata, source_url="https://data.example/source")
            assert result.properties[0]["value"] == "preserved"
            assert result.semantics[predicate]["outcome"] == "unavailable"
        assert updates.exists()
        failed = tmp_path / "failed.gz"
        async with SemanticResolver(baseline, client, failed) as resolver:
            with pytest.raises(Exception, match="context"):
                await resolver.resolve(
                    {"@context": "https://missing.example/context", "name": "test"},
                    source_url="https://data.example/source",
                )
        assert not failed.exists()
        partial = tmp_path / "partial.gz"
        async with SemanticResolver(baseline, client, partial) as resolver:
            good = await resolver.resolve(metadata, source_url="https://data.example/source", required=False)
            with pytest.raises(Exception, match="context"):
                await resolver.resolve(
                    {"@context": "https://missing.example/context", "name": "test"},
                    source_url="https://data.example/broken",
                    required=False,
                )
            assert good.properties[0]["value"] == "preserved"
        assert partial.exists()
        assert json.loads(gzip.decompress(partial.read_bytes()))
        aborted = tmp_path / "aborted.gz"
        with pytest.raises(RuntimeError, match="abort"):
            async with SemanticResolver(baseline, client, aborted) as resolver:
                await resolver.resolve(metadata, source_url="https://data.example/source")
                raise RuntimeError("abort")
        assert not aborted.exists()
        with pytest.raises(ValueError, match="overwrite"):
            SemanticResolver(baseline, client, baseline)


async def test_schema_references_and_openapi(tmp_path: Path) -> None:
    source_url = "https://schemas.example/root.json"
    schema = {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$defs": {"node": {"$anchor": "node", "title": "Node", "$ref": "#node"}},
        "type": "object",
        "properties": {
            "node": {"$ref": "#/$defs/node"},
            "amount": {"$id": "nested/amount.json", "$ref": "../unit.json#/$defs/unit"},
            "missing": {"$ref": "missing.json"},
            "dynamic": {"$dynamicRef": "#node"},
        },
        "examples": [{"$ref": "https://example.org/do-not-fetch"}],
    }
    requests = []

    def respond(request: httpx.Request) -> httpx.Response:
        requests.append(str(request.url))
        if request.url.path == "/unit.json":
            return httpx.Response(200, json={"$defs": {"unit": {"title": "Unit", "enum": ["kg", "m"]}}})
        assert request.url.path == "/missing.json"
        return httpx.Response(404)

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        async with SemanticResolver(tmp_path / "cache.gz", client, tmp_path / "updates.gz") as resolver:
            result = await resolver.resolve(schema, source_url=source_url)
            assert result.source == schema
            assert any(value.get("title") == "Unit" for value in result.schemas.values() if isinstance(value, dict))
            assert len(result.schemas) < 20
            assert {row["outcome"] for row in result.schema_references} == {
                "available",
                "unavailable",
                "unsupported",
            }
            assert sorted(requests) == [
                "https://schemas.example/missing.json",
                "https://schemas.example/unit.json",
            ]
            api = {
                "openapi": "3.1.0",
                "info": {"title": "API", "version": "1"},
                "components": {
                    "schemas": {
                        "Measurement": {
                            "type": "number",
                            "description": "A measured value",
                        }
                    }
                },
                "paths": {
                    "/data": {
                        "get": {
                            "responses": {
                                "200": {
                                    "content": {
                                        "application/json": {"schema": {"$ref": "#/components/schemas/Measurement"}}
                                    }
                                }
                            }
                        }
                    }
                },
            }
            result = await resolver.resolve(api, source_url="https://schemas.example/api.json")
            assert result.schema_references[0]["outcome"] == "available"
            assert (
                result.schemas["https://schemas.example/api.json#/components/schemas/Measurement"]["description"]
                == "A measured value"
            )
            boolean = await resolver.resolve(False, source_url=source_url, format="json-schema")
            assert boolean.schemas[source_url] is False
        assert (tmp_path / "updates.gz").exists()


def test_rdf_unicode_line_separators_preserve_literal_values() -> None:
    from harvester.vocabulary import normalize_rdf

    text = "First\u2028second\u2029third\x85fourth"
    document = {
        "https://example.org/dataset": {"http://purl.org/dc/terms/description": [{"type": "literal", "value": text}]}
    }
    assert normalize_rdf(document, "https://example.org/metadata")[0]["value"] == text


async def test_harvest_normalizes_own_doi_without_following_cited_datasets(tmp_path: Path) -> None:
    from harvester.researchdata import harvest
    from harvester.runtime import Config, Http
    from harvester.vocabulary import RdfCache

    uri = "https://catalog.example/dataset/one"
    graph = {
        uri: {
            "http://www.w3.org/ns/dcat#downloadURL": [
                {"type": "uri", "value": "https://data.example/a.csv"},
                {"type": "uri", "value": "https://data.example/b.csv"},
            ],
            "http://purl.org/dc/terms/identifier": [{"type": "literal", "value": "doi:10.1234/OWN"}],
            "http://purl.org/dc/terms/description": [
                {"type": "literal", "value": "Other work [Data set] https://doi.org/10.9876/unrelated"}
            ],
        }
    }
    citation = "Station (2021). Water [Data set]. SITES. https://hdl.handle.net/11676.1/example"
    requested = []

    def respond(request: httpx.Request) -> httpx.Response:
        requested.append(str(request.url))
        if request.url.host == "hdl.handle.net":
            return httpx.Response(
                200,
                json={
                    "pid": "11676.1/example",
                    "references": {"citationString": citation},
                    "fileName": "water.csv",
                    "accessUrl": "https://data.example/a.csv",
                },
            )
        if request.url.path.endswith(".csv"):
            return httpx.Response(200, text="station,temperature\nA,12\n", headers={"Content-Type": "text/csv"})
        return httpx.Response(200, json={"name": "Source metadata"})

    async with (
        httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client,
        Http(Config(), client=client) as http,
    ):
        result = await harvest(
            http,
            Config(),
            {
                "dataset_uri": uri,
                "metadata_url": uri,
                "metadata": graph,
                "documents": [{"url": uri, "metadata": graph}],
            },
            RdfCache(),
            tmp_path / "cache.gz",
            tmp_path / "updates.gz",
        )
    assert requested == ["https://doi.org/10.1234/own", "https://data.example/a.csv"]
    assert result["outcome"] == "eligible"
    assert len(result["resources"]) == 2
    assert result["resources"][1]["verification"] is None

    graph = {uri: {"http://purl.org/dc/terms/description": [{"type": "literal", "value": citation}]}}
    requested.clear()
    async with (
        httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client,
        Http(Config(), client=client) as http,
    ):
        result = await harvest(
            http,
            Config(),
            {
                "dataset_uri": uri,
                "metadata_url": uri,
                "metadata": graph,
                "documents": [{"url": uri, "metadata": graph}],
            },
            RdfCache(),
            tmp_path / "cache.gz",
            tmp_path / "updates.gz",
        )
    assert result["outcome"] == "eligible"
    assert requested == ["https://hdl.handle.net/11676.1/example", "https://data.example/a.csv"]
