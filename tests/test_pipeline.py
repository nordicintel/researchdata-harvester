# SPDX-License-Identifier: NOASSERTION
# Copyright (c) 2026 NordicIntel
"""Check bounded eligibility evidence, paired output retention and publication guards."""

import json
from collections.abc import AsyncIterator
from copy import deepcopy
from pathlib import Path
from typing import Any

import duckdb
import httpx
import pytest
from harvester.output import build, validate
from harvester.researchdata import verify
from scripts.publish import publish

from harvester.runtime import Config, Http


async def test_verification_requires_observations_and_keeps_only_bounded_evidence() -> None:
    csv = "station,temperature\n" + "\n".join(f"station-{index},{index}" for index in range(120))

    def respond(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/data.csv":
            return httpx.Response(200, text=csv, headers={"Content-Type": "text/csv"})
        return httpx.Response(
            200,
            json={"@type": "Dataset", "name": "Catalog metadata", "distribution": [{"name": "data.csv"}]},
        )

    config = Config(attempts=1)
    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        http = Http(config, client=client)
        result = await verify(http, "https://data.example/data.csv", config)
        assert result["outcome"] == "verified"
        evidence = result["verification"]
        assert evidence["sampled_rows"] == config.verification_rows == 100
        assert [column["name"] for column in evidence["columns"]] == ["station", "temperature"]
        assert evidence["transferred_bytes"] <= config.verification_bytes
        assert evidence["decompressed_bytes"] <= config.verification_bytes
        assert "station-0" not in json.dumps(result, default=str)
        metadata = await verify(http, "https://data.example/metadata.json", config)
        assert metadata["outcome"] == "unresolved"
        assert metadata["verification"] is None and metadata["reason"]


async def test_verification_rejects_content_exceeding_byte_limit() -> None:
    config = Config(attempts=1, verification_bytes=64)
    consumed: list[int] = []

    class ResponseStream(httpx.AsyncByteStream):
        async def __aiter__(self) -> AsyncIterator[bytes]:
            for index in range(10):
                consumed.append(index)
                yield b"a" * 32

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(200, headers={"Content-Type": "text/csv"}, stream=ResponseStream())
        )
    ) as client:
        result = await verify(Http(config, client=client), "https://data.example/oversize.csv", config)
    assert result["outcome"] == "unresolved"
    assert result["verification"] is None
    assert "limit" in result["reason"].lower() or "exceed" in result["reason"].lower()
    assert len(consumed) <= 3


def candidate_inputs() -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    """Supply one verified record pair and one unresolved metadata-only candidate."""
    timestamp = "2026-10-22T00:00:00Z"
    uri = "https://data.example/dataset/verified"
    unresolved_uri = "https://data.example/dataset/unresolved"
    entries = {
        key: {
            "dataset_uri": key,
            "registry_entry_uri": f"https://registry.example/entry/{index}",
            "metadata_url": f"https://registry.example/metadata/{index}",
            "registry_modified": timestamp,
            "registry_modified_value": timestamp,
            "metadata": {},
            "fingerprint": str(index),
        }
        for index, key in enumerate((uri, unresolved_uri))
    }
    resource = {
        "url": "https://data.example/data.csv",
        "source_url": entries[uri]["metadata_url"],
        "entity_id": uri,
        "distribution_id": None,
        "filename": "data.csv",
        "outcome": "verified",
        "reason": None,
        "checked_at": timestamp,
        "verification": {
            "verified_at": timestamp,
            "resolved_url": "https://data.example/data.csv",
            "format": "csv",
            "media_type": "text/csv",
            "access_method": "anonymous",
            "sampled_rows": 1,
            "columns": [{"name": "temperature", "data_type": "DOUBLE"}],
            "transferred_bytes": 20,
            "decompressed_bytes": 20,
            "license_acknowledgement_url": None,
        },
    }
    plan = {"entries": entries, "selected": list(entries), "removed": [], "complete": True}
    dataportalen = {
        key: {
            "row": {name: value for name, value in entry.items() if name not in {"metadata", "fingerprint"}},
            "outcome": "eligible",
        }
        for key, entry in entries.items()
    }
    researchdata = {
        uri: {"row": {"dataset_uri": uri, "resources": [resource], "availability": "available"}, "outcome": "eligible"},
        unresolved_uri: {"outcome": "unresolved", "resources": [], "reason": "No retrievable data"},
    }
    return plan, dataportalen, researchdata


def test_build_retains_prior_verification_and_never_publishes_half_a_pair(tmp_path: Path) -> None:
    plan, dataportalen, researchdata = candidate_inputs()
    current = tmp_path / "current"
    build(tmp_path / "absent", current, plan, dataportalen, researchdata, [])
    validate(current)
    uri = plan["selected"][0]
    assert sorted(path.name for path in current.glob("*.parquet")) == ["dataportalen.parquet", "datasets.parquet"]
    with duckdb.connect() as connection:
        for name in ("datasets", "dataportalen"):
            assert connection.execute(
                "SELECT dataset_uri FROM read_parquet(?)", [str(current / f"{name}.parquet")]
            ).fetchall() == [(uri,)]
        original = connection.execute(
            "SELECT resources FROM read_parquet(?)", [str(current / "datasets.parquet")]
        ).fetchone()[0][0]["verification"]

    failure_plan = deepcopy(plan)
    failure_plan["selected"] = [uri]
    resource = deepcopy(researchdata[uri]["row"]["resources"][0])
    resource.update(outcome="unavailable", reason="HTTP 503", verification=None)
    failed = {uri: {"outcome": "unavailable", "retry": True, "resources": [resource], "reason": "HTTP 503"}}
    candidate = tmp_path / "candidate"
    build(current, candidate, failure_plan, {uri: dataportalen[uri]}, failed, [])
    validate(candidate)
    with duckdb.connect() as connection:
        availability, resources = connection.execute(
            "SELECT availability, resources FROM read_parquet(?)", [str(candidate / "datasets.parquet")]
        ).fetchone()
        assert availability == "unavailable"
        assert resources[0]["outcome"] == "unavailable"
        assert resources[0]["verification"] == original
        assert connection.execute(
            "SELECT dataset_uri FROM read_parquet(?)", [str(candidate / "dataportalen.parquet")]
        ).fetchall() == [(uri,)]
    # Rechecking a candidate also detects a changed file after manifest generation.
    with (candidate / "inventory.json").open("ab") as stream:
        stream.write(b" ")
    with pytest.raises(ValueError, match="(?i)checksum|hash|size|manifest"):
        validate(candidate)


def test_scoped_candidate_is_rejected_before_publication(tmp_path: Path) -> None:
    plan, dataportalen, researchdata = candidate_inputs()
    plan["complete"] = False
    candidate = tmp_path / "scoped"
    build(tmp_path / "absent", candidate, plan, dataportalen, researchdata, [])
    before = {path.name: path.read_bytes() for path in candidate.iterdir() if path.is_file()}
    with pytest.raises(ValueError, match="(?i)scoped|complete|partial"):
        publish(candidate, "0" * 40, repo=tmp_path)
    assert {path.name: path.read_bytes() for path in candidate.iterdir() if path.is_file()} == before
