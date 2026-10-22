# SPDX-License-Identifier: NOASSERTION
# Copyright (c) 2026 NordicIntel
"""Typed current-state construction, deterministic export and local publication."""

import contextlib
import hashlib
import json
import shutil
import sys
from collections.abc import Iterator
from copy import deepcopy
from pathlib import Path
from typing import Any

import duckdb

from harvester.runtime import read_json, record_key, validate_result, write_json
from harvester.vocabulary import PREDICATE_COLUMNS, RdfCache

SCHEMA = Path(__file__).with_name("schema.sql")
MAX_BYTES = 100_000_000
TABLES = ("datasets", "dataportalen")


def sha(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def json_properties(
    value: Any, source: str, path: str = "", entity: str | None = None, prop: str = ""
) -> list[dict[str, Any]]:
    """Preserve every JSON value, container and array position without opaque blobs."""
    kind = (
        "null"
        if value is None
        else "boolean"
        if isinstance(value, bool)
        else "object"
        if isinstance(value, dict)
        else "array"
        if isinstance(value, list)
        else "number"
        if isinstance(value, (int, float))
        else "string"
    )
    lexical = (
        None
        if kind in {"null", "object", "array"}
        else json.dumps(value)
        if kind in {"number", "boolean"}
        else str(value)
    )
    result = [
        {
            "value": lexical,
            "value_type": kind,
            "language": None,
            "datatype": None,
            "source_url": source,
            "entity_id": entity or source,
            "graph_id": None,
            "property": prop,
            "path": path,
        }
    ]
    children = value.items() if isinstance(value, dict) else enumerate(value) if isinstance(value, list) else []
    for key, child in children:
        name = str(key)
        pointer = path + "/" + name.replace("~", "~0").replace("/", "~1")
        result.extend(json_properties(child, source, pointer, entity, name if isinstance(value, dict) else prop))
    return result


def unique(values: list[Any]) -> list[Any]:
    return [
        item for _, item in sorted({json.dumps(value, sort_keys=True, default=str): value for value in values}.items())
    ]


def project(documents: list[dict[str, Any]], dataset_uri: str) -> dict[str, Any]:
    properties: list[dict[str, Any]] = []
    for document in documents:
        properties.extend(document.get("properties", []))
        if document.get("json"):
            properties.extend(json_properties(document["metadata"], document["url"]))
    properties = unique(properties)
    by_entity: dict[str, list[dict[str, Any]]] = {}
    for item in properties:
        by_entity.setdefault(item["entity_id"], []).append(item)
    roots = {dataset_uri} | {
        item["entity_id"]
        for item in properties
        if item["property"].endswith("#type")
        and item["value"] in {"https://schema.org/Dataset", "http://schema.org/Dataset"}
    }
    row: dict[str, Any] = {"dataset_uri": dataset_uri, "properties": properties}
    special = {
        "publishers": "agent",
        "creators": "agent",
        "contacts": "agent",
        "distributions": "distribution",
        "services": "service",
    }
    for entity in roots:
        for item in by_entity.get(entity, []):
            column = PREDICATE_COLUMNS["dataset"].get(item["property"])
            if not column:
                continue
            if column in special:
                context = special[column]
                nested: dict[str, Any] = {"entity_id": item["value"], "source_url": item["source_url"]}
                for field in set(PREDICATE_COLUMNS[context].values()):
                    nested[field] = []
                for term in by_entity.get(item["value"], []):
                    target_field = PREDICATE_COLUMNS[context].get(term["property"])
                    if target_field:
                        nested[target_field].append(term)
                row.setdefault(column, []).append(nested)
            else:
                row.setdefault(column, []).append(item)
    for key, value in row.items():
        if isinstance(value, list):
            row[key] = unique(value)
    return row


def load_rows(path: Path, table: str, manifest: dict[str, Any]) -> dict[str, dict[str, Any]]:
    files = [str(path / name) for name, item in manifest.get("files", {}).items() if item.get("table") == table]
    if not files:
        return {}
    with duckdb.connect() as connection:
        result = connection.execute("SELECT * FROM read_parquet(?) ORDER BY dataset_uri", [files])
        names = [item[0] for item in result.description]
        return {row[0]: dict(zip(names, row, strict=True)) for row in result.fetchall()}


def insert(connection: duckdb.DuckDBPyConnection, table: str, row: dict[str, Any]) -> None:
    allowed = {item[1] for item in connection.execute(f"PRAGMA table_info('{table}')").fetchall()}
    columns = sorted(set(row) & allowed)
    connection.execute(
        f"INSERT INTO {table} ({','.join(columns)}) VALUES ({','.join('?' for _ in columns)})",
        [row[key] for key in columns],
    )


def export(connection: duckdb.DuckDBPyConnection, table: str, path: Path) -> list[Path]:
    count = connection.execute(f"SELECT count(*) FROM {table}").fetchall()[0][0]
    parts = 1
    while True:
        written = []
        size = max(1, (count + parts - 1) // parts)
        for part in range(parts):
            name = f"{table}.parquet" if parts == 1 else f"{table}_part_{part + 1}.parquet"
            file = path / name
            connection.execute(
                f"COPY (SELECT * FROM {table} ORDER BY dataset_uri LIMIT {size} OFFSET {part * size}) TO '{str(file).replace(chr(39), chr(39) * 2)}' (FORMAT PARQUET, COMPRESSION ZSTD)"
            )
            written.append(file)
        if all(file.stat().st_size < MAX_BYTES for file in written):
            return written
        for file in written:
            file.unlink()
        if parts >= max(count, 1):
            raise ValueError("One row exceeds the maximum Parquet file size")
        parts += 1


def build(
    current: Path,
    candidate: Path,
    plan: dict[str, Any],
    dataportalen_results: dict[str, Any],
    researchdata_results: dict[str, Any],
    cache_updates: list[dict[str, Any]],
) -> dict[str, Any]:
    """Build a complete pair of typed tables without mutating the prior generation."""
    if candidate.resolve() == current.resolve():
        raise ValueError("Candidate must be separate from current state")
    prior = validate(current) if (current / "manifest.json").exists() else {}
    old_inventory = read_json(current / "inventory.json", {"entries": {}})
    rows = {table: load_rows(current, table, prior) for table in TABLES}
    inventory = deepcopy(plan)
    for key in ("selected", "removed", "full"):
        inventory.pop(key, None)
    for uri in set(plan.get("removed", [])) | {
        uri for uri, item in plan["entries"].items() if item.get("outcome") == "excluded"
    }:
        for table in TABLES:
            rows[table].pop(uri, None)
    for uri in plan["selected"]:
        if uri not in dataportalen_results or uri not in researchdata_results:
            raise ValueError(f"Incomplete required stage results for {uri}")
        dp, rd = dataportalen_results[uri], researchdata_results[uri]
        validate_result(dp, "dataportalen", uri)
        validate_result(rd, "researchdata", uri)
        item = inventory["entries"][uri]
        item["stages"] = {
            stage: {"status": result["status"], "retry": bool(result.get("retry")), "reason": result.get("reason")}
            for stage, result in (("dataportalen", dp), ("researchdata", rd))
        }
        item["retry"] = any(result.get("retry", False) for result in (dp, rd))
        if dp["status"] == "failed" or rd["status"] == "failed":
            previous = old_inventory["entries"].get(uri, {})
            for key in ("researchdata", "association_fingerprint", "cache_references", "sources", "published_keys"):
                if key in previous:
                    item[key] = deepcopy(previous[key])
            if rd["status"] == "succeeded":
                for key in ("researchdata", "sources"):
                    if key in rd:
                        item[key] = deepcopy(rd[key])
                if rd.get("researchdata"):
                    item["association_fingerprint"] = item["fingerprint"]
            item["failures"] = [result for result in (dp, rd) if result["status"] == "failed"]
            item["cache_references"] = sorted(
                set(item.get("cache_references", []))
                | {key for result in (dp, rd) for key in result.get("cache_references", [])}
            )
            item["reason"] = "; ".join(result["reason"] for result in item["failures"])
            item["outcome"] = "unavailable" if uri in rows["datasets"] else "unresolved"
            if uri in rows["datasets"]:
                retained = rows["datasets"][uri]
                retained["availability"] = "unavailable"
                for source in retained.get("sources", []):
                    source.update(stale=True, outcome="unavailable")
                for resource in retained["resources"]:
                    resource.update(outcome="unavailable", reason="Metadata refresh failed")
            continue
        item.pop("failures", None)
        item.update(
            {
                key: rd[key]
                for key in ("outcome", "reason", "retry", "researchdata", "sources", "cache_references")
                if key in rd
            }
        )
        if rd.get("researchdata"):
            item["association_fingerprint"] = item["fingerprint"]
        if rd.get("outcome") == "eligible":
            row = deepcopy(rd.get("row")) or project(rd.get("documents", []), uri)
            row.update({key: rd[key] for key in ("resources", "researchdata", "sources") if key in rd})
            row.setdefault("availability", "available")
            rows["datasets"][uri] = row
        elif uri in rows["datasets"] and rd.get("outcome") in {"unavailable", "unresolved"}:
            row = deepcopy(rows["datasets"][uri])
            row["availability"] = "unavailable"
            latest = {resource["url"]: resource for resource in rd.get("resources", [])}
            for resource in row["resources"]:
                update = latest.pop(resource["url"], {})
                evidence = resource["verification"]
                resource.update(update)
                resource["verification"] = evidence
                resource["outcome"] = "unavailable"
                resource["reason"] = update.get("reason", rd.get("reason", "Refresh unavailable"))
            row["resources"].extend(deepcopy(list(latest.values())))
            for source in row.get("sources", []):
                source.update(stale=True, outcome="unavailable")
            rows["datasets"][uri] = row
            item["outcome"] = "unavailable"
            item["cache_references"] = sorted(
                set(item.get("cache_references", []))
                | set(old_inventory["entries"].get(uri, {}).get("cache_references", []))
            )
        else:
            rows["datasets"].pop(uri, None)
            rows["dataportalen"].pop(uri, None)
            continue
        dp_row = deepcopy(dp.get("row")) or project(dp.get("documents", []), uri)
        dp_row.update(
            {
                key: plan["entries"][uri][key]
                for key in ("registry_entry_uri", "metadata_url", "registry_modified", "registry_modified_value")
            }
        )
        rows["dataportalen"][uri] = dp_row
        item["published_keys"] = dict(old_inventory["entries"].get(uri, {}).get("published_keys", {}))
        item["published_keys"]["dataportalen"] = record_key(plan, item)
        if rd.get("outcome") == "eligible":
            item["published_keys"]["researchdata"] = record_key(plan, item)
        item["cache_references"] = sorted(set(item.get("cache_references", [])) | set(dp.get("cache_references", [])))
    for item in inventory["entries"].values():
        item.pop("metadata", None)
    candidate.mkdir(parents=True, exist_ok=True)
    if list(candidate.iterdir()):
        raise ValueError("Candidate directory must be empty")
    files: dict[str, Any] = {}
    with duckdb.connect() as connection:
        connection.execute(SCHEMA.read_text(encoding="utf-8"))
        for table in TABLES:
            for uri in sorted(rows[table]):
                insert(connection, table, rows[table][uri])
        errors = connection.execute("SELECT * FROM publication_errors").fetchall()
        if errors:
            raise ValueError(f"Candidate validation failed: {errors[:5]}")
        for table in TABLES:
            for file in export(connection, table, candidate):
                files[file.name] = {
                    "table": table,
                    "rows": connection.execute("SELECT count(*) FROM read_parquet(?)", [str(file)]).fetchall()[0][0],
                }
    write_json(candidate / "inventory.json", inventory)
    cache = RdfCache.load(current / "rdf-cache.json.gz")
    cache.merge(*cache_updates)
    referenced = {uri for item in inventory["entries"].values() for uri in item.get("cache_references", [])}
    cache.save(candidate / "rdf-cache.json.gz", referenced=referenced)
    for name in ("inventory.json", "rdf-cache.json.gz"):
        files[name] = {}
    for name, details in files.items():
        path = candidate / name
        details.update(bytes=path.stat().st_size, sha256=sha(path))
    manifest = {
        "schema_version": 1,
        "complete": bool(plan["complete"]),
        "base_commit": plan.get("base_commit"),
        "files": files,
        "counts": {table: len(rows[table]) for table in TABLES},
    }
    write_json(candidate / "manifest.json", manifest)
    validate(candidate)
    return manifest


def validate(path: Path) -> dict[str, Any]:
    """Check all published bytes and restore SQL constraints before trusting state."""
    manifest = read_json(path / "manifest.json")
    if not isinstance(manifest, dict) or manifest.get("schema_version") != 1:
        raise ValueError("Missing or unsupported manifest")
    files = manifest["files"]
    expected = set(files) | {"manifest.json"}
    if {file.name for file in path.iterdir() if file.is_file()} != expected:
        raise ValueError("Manifest file membership mismatch")
    for name, info in files.items():
        if Path(name).name != name:
            raise ValueError("Unsafe manifest filename")
        file = path / name
        if file.stat().st_size != info["bytes"] or sha(file) != info["sha256"]:
            raise ValueError(f"Manifest checksum/size mismatch: {name}")
        if file.stat().st_size >= MAX_BYTES:
            raise ValueError(f"Published file exceeds maximum size: {name}")
    with duckdb.connect() as connection:
        connection.execute(SCHEMA.read_text(encoding="utf-8"))
        for table in TABLES:
            paths = [str(path / name) for name, info in files.items() if info.get("table") == table]
            if not paths:
                raise ValueError(f"Manifest is missing table {table}")
            connection.execute(f"INSERT INTO {table} BY NAME SELECT * FROM read_parquet(?)", [paths])
            if connection.execute(f"SELECT count(*) FROM {table}").fetchall()[0][0] != manifest["counts"][table]:
                raise ValueError("Manifest row count mismatch")
        if connection.execute("SELECT * FROM publication_errors").fetchall():
            raise ValueError("Published SQL contract is invalid")
        identities = {row[0] for row in connection.execute("SELECT dataset_uri FROM datasets").fetchall()}
    inventory = read_json(path / "inventory.json")
    eligible = {uri for uri, item in inventory["entries"].items() if item.get("outcome") in {"eligible", "unavailable"}}
    # A new unavailable entry has no prior verification and belongs only to inventory.
    required = {uri for uri, item in inventory["entries"].items() if item.get("outcome") == "eligible"}
    if not identities <= eligible or not required <= identities:
        raise ValueError("Inventory outcomes do not agree with table membership")
    RdfCache.load(path / "rdf-cache.json.gz")
    return manifest


@contextlib.contextmanager
def lock(destination: Path) -> Iterator[None]:
    destination.parent.mkdir(parents=True, exist_ok=True)
    with destination.with_name(destination.name + ".lock").open("a+b") as stream:
        stream.seek(0)
        if sys.platform == "win32":
            import msvcrt

            if stream.read(1) == b"":
                stream.write(b"0")
                stream.flush()
            stream.seek(0)
            msvcrt.locking(stream.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl

            fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        try:
            yield
        finally:
            if sys.platform == "win32":
                stream.seek(0)
                msvcrt.locking(stream.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                fcntl.flock(stream.fileno(), fcntl.LOCK_UN)


def recover(destination: Path) -> None:
    previous = destination.with_name(destination.name + ".previous")
    if previous.exists() and not destination.exists():
        previous.replace(destination)
    if previous.exists() and destination.exists():
        validate(destination)
        shutil.rmtree(previous)


def replace(candidate: Path, destination: Path) -> None:
    """Replace one validated generation while retaining crash recovery until success."""
    validate(candidate)
    recover(destination)
    if destination.exists() and any(destination.iterdir()):
        validate(destination)
    pending = destination.with_name(destination.name + ".pending")
    previous = destination.with_name(destination.name + ".previous")
    if pending.exists():
        shutil.rmtree(pending)
    shutil.copytree(candidate, pending)
    if destination.exists():
        destination.replace(previous)
    pending.replace(destination)
    validate(destination)
    if previous.exists():
        shutil.rmtree(previous)


def clear_completed_candidate(candidate: Path, destination: Path, work: Path) -> None:
    """Remove only a work-owned candidate already installed as current data."""
    if not candidate.exists():
        return
    target = candidate.resolve()
    if target.parent != work.resolve() or candidate.is_symlink() or candidate.is_junction():
        raise ValueError("Candidate cleanup target is outside its work directory")
    if not (destination / "manifest.json").exists() or validate(candidate) != validate(destination):
        raise ValueError("Candidate is not the installed generation; inspect it or use a fresh work directory")
    shutil.rmtree(target)
