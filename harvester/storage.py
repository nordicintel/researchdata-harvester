# SPDX-License-Identifier: NOASSERTION
# Copyright (c) 2026 NordicIntel
"""Persist and validate one current JSON catalog without a database or run history."""

import gzip
import hashlib
import json
import os
import shutil
from collections import defaultdict
from collections.abc import Iterable, Iterator
from pathlib import Path
from typing import Any

COLLECTIONS = ("researchdata", "dataportalen", "external", "links", "files")
MAX_FILE_BYTES = 100_000_000


def encode(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def fingerprint(value: Any) -> str:
    return hashlib.sha256(encode(value).encode("utf-8")).hexdigest()


def record_key(collection: str, record: dict[str, Any]) -> str:
    if collection == "researchdata":
        return record["identifier"]
    if collection == "dataportalen":
        return record["uri"]
    if collection == "external":
        return record["url"]
    if collection == "links":
        return encode([record["identifier"], record["uri"], record["rule"]])
    if collection == "files":
        return encode([record["identifier"], record["url"]])
    raise ValueError(f"Unknown collection: {collection}")


def file_hash(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, value: Any) -> None:
    path.write_text(encode(value) + "\n", encoding="utf-8", newline="\n")


def iter_records(directory: Path, collection: str) -> Iterator[dict[str, Any]]:
    if not directory.exists():
        return
    manifest = read_json(directory / "manifest.json")
    for entry in manifest["collections"][collection]["files"]:
        with gzip.open(directory / entry["name"], "rt", encoding="utf-8") as stream:
            for line in stream:
                yield json.loads(line)


def load_records(directory: Path, collection: str) -> dict[str, dict[str, Any]]:
    return {
        record_key(collection, record): record
        for record in iter_records(directory, collection)
    }


def write_collection(
    directory: Path,
    collection: str,
    records: Iterable[dict[str, Any]],
    *,
    max_bytes: int = MAX_FILE_BYTES,
) -> dict[str, Any]:
    buckets: dict[str, list[tuple[str, dict[str, Any]]]] = defaultdict(list)
    seen: set[str] = set()
    for record in records:
        key = record_key(collection, record)
        if key in seen:
            raise ValueError(f"Duplicate {collection} identity: {key}")
        seen.add(key)
        buckets[hashlib.sha256(key.encode()).hexdigest()[0]].append((key, record))
    files: list[dict[str, Any]] = []

    def emit(prefix: str, entries: list[tuple[str, dict[str, Any]]]) -> None:
        path = directory / f"{collection}-{prefix}.jsonl.gz"
        with (
            path.open("wb") as raw,
            gzip.GzipFile(filename="", fileobj=raw, mode="wb", mtime=0) as compressed,
        ):
            for _, record in sorted(entries, key=lambda item: item[0]):
                compressed.write((encode(record) + "\n").encode("utf-8"))
        if path.stat().st_size >= max_bytes:
            path.unlink()
            if len(entries) < 2 or len(prefix) == 64:
                raise ValueError(
                    f"A {collection} record exceeds the publication size limit."
                )
            children: dict[str, list[tuple[str, dict[str, Any]]]] = defaultdict(list)
            for key, record in entries:
                children[
                    hashlib.sha256(key.encode()).hexdigest()[: len(prefix) + 1]
                ].append((key, record))
            for child in sorted(children):
                emit(child, children[child])
        else:
            files.append(
                {
                    "name": path.name,
                    "count": len(entries),
                    "bytes": path.stat().st_size,
                    "sha256": file_hash(path),
                }
            )

    for prefix in sorted(buckets):
        emit(prefix, buckets[prefix])
    return {"count": len(seen), "files": files}


def write_catalog(
    directory: Path,
    collections: dict[str, Iterable[dict[str, Any]]],
    matches: dict[str, list[str]],
    state: dict[str, Any],
) -> dict[str, Any]:
    directory.mkdir(parents=True, exist_ok=False)
    manifest: dict[str, Any] = {"schema": 1, "collections": {}, "metadata": {}}
    for collection in COLLECTIONS:
        manifest["collections"][collection] = write_collection(
            directory, collection, collections[collection]
        )
    for name, value in (("matches.json", matches), ("state.json", state)):
        write_json(directory / name, value)
        manifest["metadata"][name] = {"sha256": file_hash(directory / name)}
    write_json(directory / "manifest.json", manifest)
    validate_catalog(directory)
    return manifest


def validate_catalog(directory: Path) -> dict[str, int]:
    manifest = read_json(directory / "manifest.json")
    if manifest.get("schema") != 1 or set(manifest["collections"]) != set(COLLECTIONS):
        raise ValueError("Unsupported or incomplete current catalog manifest.")
    expected = {"manifest.json", "state.json", "matches.json"}
    for collection in COLLECTIONS:
        for entry in manifest["collections"][collection]["files"]:
            name = entry["name"]
            if Path(name).name != name or not name.startswith(collection + "-"):
                raise ValueError("Unsafe catalog filename.")
            expected.add(name)
            path = directory / name
            if (
                path.stat().st_size >= MAX_FILE_BYTES
                or file_hash(path) != entry["sha256"]
            ):
                raise ValueError(f"Invalid catalog file: {name}")
    for name in ("state.json", "matches.json"):
        if file_hash(directory / name) != manifest["metadata"][name]["sha256"]:
            raise ValueError(f"Invalid catalog state file: {name}")
    if {path.name for path in directory.iterdir()} != expected:
        raise ValueError("Current directory contains files outside its manifest.")
    identities: dict[str, set[str]] = {}
    counts: dict[str, int] = {}
    confirmed: set[tuple[str, str]] = set()
    for collection in COLLECTIONS:
        keys: set[str] = set()
        dataset_ids: set[str] = set()
        for record in iter_records(directory, collection):
            key = record_key(collection, record)
            if key in keys:
                raise ValueError(f"Duplicate {collection} identity: {key}")
            keys.add(key)
            if collection == "researchdata":
                if record["dataset_id"] in dataset_ids:
                    raise ValueError(
                        "Multiple current records for one Researchdata dataset."
                    )
                dataset_ids.add(record["dataset_id"])
                if set(record["representations"]) != {"datacite", "dcat", "json_ld"}:
                    raise ValueError(f"Missing metadata representation: {key}")
            if collection in {"links", "files"}:
                if record["identifier"] not in identities["researchdata"]:
                    raise ValueError(
                        f"Dangling {collection} Researchdata identity: {key}"
                    )
            if collection == "links":
                if record["uri"] not in identities["dataportalen"]:
                    raise ValueError(f"Dangling Dataportalen link: {key}")
                if record["status"] == "matched" and record["rule"] != "exact_identity":
                    confirmed.add((record["identifier"], record["uri"]))
            if (
                collection == "external"
                and not set(record["identifiers"]) <= identities["researchdata"]
            ):
                raise ValueError(f"Dangling external metadata owner: {key}")
        counts[collection] = len(keys)
        if len(keys) != manifest["collections"][collection]["count"]:
            raise ValueError(f"Manifest count differs for {collection}.")
        identities[collection] = keys
    matches = read_json(directory / "matches.json")
    mapped = {(identifier, uri) for identifier, uris in matches.items() for uri in uris}
    if mapped != confirmed or any(
        uris != sorted(set(uris)) for uris in matches.values()
    ):
        raise ValueError(
            "Confirmed indirect map differs from current evidence-bearing links."
        )
    return counts


def recover_current(current: Path) -> None:
    previous = current.parent / ".replacing-current"
    if previous.exists() and not current.exists():
        previous.rename(current)
    elif previous.exists():
        validate_catalog(current)
        shutil.rmtree(previous)


def activate(candidate: Path, current: Path) -> None:
    """Swap a validated candidate, retaining the old directory only during replacement."""
    validate_catalog(candidate)
    recover_current(current)
    previous = current.parent / ".replacing-current"
    if current.exists():
        current.rename(previous)
    try:
        candidate.rename(current)
    except BaseException:
        if previous.exists():
            previous.rename(current)
        raise
    if previous.exists():
        shutil.rmtree(previous)


class CatalogLock:
    """Hold an OS lock so crashed writers do not leave a stale lock to clear."""

    def __init__(self, directory: Path):
        directory.mkdir(parents=True, exist_ok=True)
        self.path = directory / ".sync.lock"

    def __enter__(self) -> "CatalogLock":
        self.stream = self.path.open("a+b")
        if self.path.stat().st_size == 0:
            self.stream.write(b"0")
            self.stream.flush()
        self.stream.seek(0)
        try:
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(self.stream.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(self.stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            self.stream.close()
            raise RuntimeError("Another process is updating this catalog.") from None
        return self

    def __exit__(self, *args: Any) -> None:
        self.stream.close()
