# SPDX-License-Identifier: NOASSERTION
# Copyright (c) 2026 NordicIntel
"""Exercise deterministic current publication and failed replacement recovery."""

import json
from pathlib import Path

import pytest

from harvester.storage import (
    CatalogLock,
    activate,
    file_hash,
    iter_records,
    recover_current,
    validate_catalog,
    write_catalog,
    write_collection,
)


def catalog() -> dict:
    return {
        "researchdata": [
            {
                "identifier": "r1",
                "dataset_id": "d1",
                "representations": {"datacite": {}, "dcat": {}, "json_ld": {}},
            }
        ],
        "dataportalen": [{"uri": "https://example.org/d1"}],
        "external": [],
        "links": [
            {
                "identifier": "r1",
                "uri": "https://example.org/d1",
                "rule": "exact_identity",
                "status": "matched",
            }
        ],
        "files": [{"identifier": "r1", "url": "https://example.org/f.csv"}],
    }


def test_identical_catalog_has_identical_compressed_bytes(tmp_path):
    first, second = tmp_path / "a", tmp_path / "b"
    write_catalog(first, catalog(), {}, {"boundary": "2026-10-21"})
    write_catalog(second, catalog(), {}, {"boundary": "2026-10-21"})
    assert {p.name: file_hash(p) for p in first.iterdir()} == {
        p.name: file_hash(p) for p in second.iterdir()
    }
    assert list(iter_records(first, "files")) == catalog()["files"]


def test_corrupt_candidate_cannot_replace_current(tmp_path):
    current, candidate = tmp_path / "current", tmp_path / "candidate"
    write_catalog(current, catalog(), {}, {"boundary": "2026-10-20"})
    write_catalog(candidate, catalog(), {}, {"boundary": "2026-10-21"})
    (candidate / "matches.json").write_text("{} broken")
    with pytest.raises(ValueError, match="state file"):
        activate(candidate, current)
    assert json.loads((current / "state.json").read_text())["boundary"] == "2026-10-20"


def test_interrupted_directory_swap_recovers_last_current(tmp_path):
    current = tmp_path / "current"
    write_catalog(current, catalog(), {}, {"boundary": "2026-10-20"})
    current.rename(tmp_path / ".replacing-current")
    recover_current(current)
    assert validate_catalog(current)["researchdata"] == 1
    assert not (tmp_path / ".replacing-current").exists()


def test_dangling_link_and_unconfirmed_map_are_rejected(tmp_path):
    data = catalog()
    data["links"][0]["identifier"] = "missing"
    with pytest.raises(ValueError, match="Dangling"):
        write_catalog(tmp_path / "a", data, {}, {})
    with pytest.raises(ValueError, match="map differs"):
        write_catalog(tmp_path / "b", catalog(), {"r1": ["https://example.org/d1"]}, {})


def test_compression_limit_splits_without_record_loss(tmp_path):
    records = [
        {"uri": f"https://example.org/{index}", "value": str(index) * 10}
        for index in range(200)
    ]
    manifest = write_collection(tmp_path, "dataportalen", records, max_bytes=150)
    assert manifest["count"] == 200
    assert all(entry["bytes"] < 150 for entry in manifest["files"])
    assert sum(entry["count"] for entry in manifest["files"]) == 200


def test_two_local_writers_cannot_overlap(tmp_path):
    with CatalogLock(tmp_path), pytest.raises(RuntimeError, match="Another process"):
        with CatalogLock(tmp_path):
            pass


def test_failed_rename_restores_current(tmp_path, monkeypatch):
    current, candidate = tmp_path / "current", tmp_path / "candidate"
    write_catalog(current, catalog(), {}, {"boundary": "old"})
    write_catalog(candidate, catalog(), {}, {"boundary": "new"})
    original = Path.rename

    def fail_candidate(self, target):
        if self == candidate:
            raise OSError("disk failure")
        return original(self, target)

    monkeypatch.setattr(Path, "rename", fail_candidate)
    with pytest.raises(OSError, match="disk failure"):
        activate(candidate, current)
    assert json.loads((current / "state.json").read_text())["boundary"] == "old"
