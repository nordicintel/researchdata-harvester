# SPDX-License-Identifier: NOASSERTION
# Copyright (c) 2026 NordicIntel
"""Check the coordinator's successful replacement and failure boundary."""

import importlib
import json

import pytest

from harvester.config import Config
from harvester.storage import file_hash, load_records, write_catalog

coordinator = importlib.import_module("harvester.sync")


def records(identifier):
    return {
        identifier: {
            "identifier": identifier,
            "dataset_id": identifier,
            "representations": {"datacite": {}, "dcat": {}, "json_ld": {}},
            "identities": [],
        }
    }


def initial(directory):
    write_catalog(
        directory,
        {
            "researchdata": records("old").values(),
            "dataportalen": [],
            "links": [],
            "files": [],
            "external": [],
        },
        {},
        {"boundary": "2026-10-20"},
    )


async def test_changed_membership_replaces_only_after_complete_harvest(
    tmp_path, monkeypatch
):
    current = tmp_path / "current"
    initial(current)

    async def research(*args, **kwargs):
        assert args[3] == "2026-10-20"
        return records("new"), {"removed": ["old"]}

    async def dataportalen(*args):
        return {}, {"members": []}

    async def external(*args):
        assert (
            json.loads((current / "state.json").read_text())["boundary"] == "2026-10-20"
        )
        return [], []

    monkeypatch.setattr(coordinator, "harvest_researchdata", research)
    monkeypatch.setattr(coordinator, "harvest_dataportalen", dataportalen)
    monkeypatch.setattr(coordinator, "enrich_records", external)
    monkeypatch.setattr(coordinator, "match_records", lambda *args: ([], {}, {}))
    result = await coordinator.sync(
        Config(current_directory=current), progress=lambda message: None
    )
    assert result["counts"]["researchdata"] == 1
    assert set(load_records(current, "researchdata")) == {"new"}


async def test_interrupted_enrichment_keeps_all_published_bytes(tmp_path, monkeypatch):
    current = tmp_path / "current"
    initial(current)
    before = {path.name: file_hash(path) for path in current.iterdir()}

    async def research(*args, **kwargs):
        return records("new"), {}

    async def dataportalen(*args):
        return {}, {"members": []}

    async def external(*args):
        raise RuntimeError("interrupted")

    monkeypatch.setattr(coordinator, "harvest_researchdata", research)
    monkeypatch.setattr(coordinator, "harvest_dataportalen", dataportalen)
    monkeypatch.setattr(coordinator, "enrich_records", external)
    monkeypatch.setattr(coordinator, "match_records", lambda *args: ([], {}, {}))
    with pytest.raises(RuntimeError, match="interrupted"):
        await coordinator.sync(
            Config(current_directory=current), progress=lambda message: None
        )
    assert {path.name: file_hash(path) for path in current.iterdir()} == before
