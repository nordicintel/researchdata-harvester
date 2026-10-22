# SPDX-License-Identifier: NOASSERTION
# Copyright (c) 2026 NordicIntel
"""Check bounded eligibility evidence, paired output retention and publication guards."""

import asyncio
import json
import subprocess
from collections.abc import AsyncIterator
from copy import deepcopy
from pathlib import Path
from typing import Any

import duckdb
import httpx
import pytest

from harvester.output import build, replace, validate
from harvester.researchdata import verify
from harvester.runtime import Config, Http, read_json
from scripts.publish import publish


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
    plan = {
        "entries": entries,
        "selected": list(entries),
        "removed": [],
        "complete": True,
        "implementation": "code",
        "configuration": "config",
        "full": False,
    }
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
    for result in (*dataportalen.values(), *researchdata.values()):
        result["status"] = "succeeded"
    return plan, dataportalen, researchdata


def test_build_retains_prior_verification_and_never_publishes_half_a_pair(tmp_path: Path) -> None:
    plan, dataportalen, researchdata = candidate_inputs()
    current = tmp_path / "current"
    build(tmp_path / "absent", current, plan, dataportalen, researchdata, [])
    validate(current)
    unrelated = tmp_path / "unrelated"
    unrelated.mkdir()
    (unrelated / "keep.txt").write_text("User-owned content", encoding="utf-8")
    with pytest.raises(ValueError, match="manifest"):
        replace(current, unrelated)
    assert (unrelated / "keep.txt").read_text(encoding="utf-8") == "User-owned content"
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
    failed = {
        uri: {
            "status": "succeeded",
            "outcome": "unavailable",
            "retry": True,
            "resources": [resource],
            "reason": "HTTP 503",
        }
    }
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


async def test_fetch_contains_expected_failures_and_reuses_completed_records(tmp_path: Path, monkeypatch: Any) -> None:
    from harvester.runtime import RecordError
    from scripts import sync

    plan = {
        "selected": ["good", "bad"],
        "entries": {key: {"fingerprint": key} for key in ("good", "bad")},
        "implementation": "code",
        "configuration": "config",
        "base_commit": "head",
        "plan_id": "plan",
        "full": False,
    }
    calls: list[str] = []
    failing = True

    async def metadata(http: Any, config: Any, entry: Any, cache: Any) -> Any:
        key = entry["fingerprint"]
        calls.append(key)
        if key == "bad" and failing:
            raise RecordError("ReadTimeout", url="https://example.org/data?secret=hidden", retry=True)
        return {"documents": []}

    monkeypatch.setattr(sync.dataportalen, "metadata", metadata)
    result = await sync.fetch("dataportalen", plan, Config(), tmp_path / "work", tmp_path / "current")
    assert result["results"]["good"]["status"] == "succeeded"
    assert result["results"]["bad"]["status"] == "failed"
    assert "secret" not in json.dumps(result)
    failing = False
    result = await sync.fetch("dataportalen", plan, Config(), tmp_path / "work", tmp_path / "current")
    assert calls == ["good", "bad", "bad"]
    assert result["outcomes"] == {"succeeded": 2}
    from harvester.runtime import fingerprint, write_json

    write_json(
        tmp_path / "work/dataportalen" / (fingerprint("good") + ".json"),
        {
            "key": "obsolete-implementation",
            "result": {},
            "cache_updates": {},
            "sequence": 0,
        },
    )
    refreshed = await sync.fetch("dataportalen", plan, Config(), tmp_path / "work", tmp_path / "current")
    assert refreshed["outcomes"] == {"succeeded": 2}
    assert calls == ["good", "bad", "bad", "good"]
    plan.update(full=True, plan_id="explicit-full-refresh")
    await sync.fetch("dataportalen", plan, Config(), tmp_path / "work", tmp_path / "current")
    assert calls == ["good", "bad", "bad", "good", "good", "bad"]
    await sync.fetch("dataportalen", plan, Config(), tmp_path / "work", tmp_path / "current")
    assert calls == ["good", "bad", "bad", "good", "good", "bad"]

    async def broken(*args: Any, **kwargs: Any) -> Any:
        raise KeyError("programming defect")

    monkeypatch.setattr(sync.dataportalen, "metadata", broken)
    with pytest.raises(ExceptionGroup):
        await sync.fetch("dataportalen", plan, Config(), tmp_path / "fatal", tmp_path / "current")
    assert not (tmp_path / "fatal/dataportalen.json").exists()


def test_required_record_failure_retains_pair_and_defers_new_record(tmp_path: Path) -> None:
    plan, dp, rd = candidate_inputs()
    current = tmp_path / "current"
    build(tmp_path / "absent", current, plan, dp, rd, [])
    failed = {
        uri: {
            "status": "failed",
            "dataset_uri": uri,
            "stage": "dataportalen",
            "operation": "retrieve_metadata",
            "url": "https://example.org",
            "category": "ReadTimeout",
            "reason": "ReadTimeout",
            "retry": True,
        }
        for uri in plan["selected"]
    }
    candidate = tmp_path / "candidate"
    build(current, candidate, plan, failed, rd, [])
    assert validate(candidate)["counts"] == {"datasets": 1, "dataportalen": 1}
    inventory = json.loads((candidate / "inventory.json").read_text())
    assert all(item["retry"] for item in inventory["entries"].values())
    assert inventory["entries"][plan["selected"][1]]["outcome"] == "unresolved"
    with pytest.raises(ValueError, match="Incomplete required"):
        build(current, tmp_path / "missing", plan, {}, rd, [])


@pytest.mark.parametrize("stage", ["dataportalen", "researchdata"])
async def test_http_retry_exhaustion_is_record_local(tmp_path: Path, monkeypatch: Any, stage: str) -> None:
    from tenacity import wait_none

    from scripts import sync

    attempts: dict[str, int] = {}
    failing = True
    plan = {
        "selected": ["good", "bad"],
        "entries": {key: {"dataset_uri": key, "fingerprint": key} for key in ("good", "bad")},
        "implementation": "code",
        "configuration": "config",
        "base_commit": "head",
        "plan_id": "plan",
        "full": False,
    }

    def respond(request: httpx.Request) -> httpx.Response:
        key = request.url.path.strip("/")
        attempts[key] = attempts.get(key, 0) + 1
        if key == "bad" and failing:
            raise httpx.ReadTimeout("secret response text must not be logged", request=request)
        return httpx.Response(200, json={})

    async def metadata(http: Any, config: Any, entry: Any, cache: Any) -> Any:
        await http.get("https://example.org/" + entry["dataset_uri"] + "?token=secret")
        return {**entry, "documents": []}

    async def harvest(*args: Any, **kwargs: Any) -> Any:
        return {"outcome": "unresolved", "resources": []}

    monkeypatch.setattr(sync.dataportalen, "metadata", metadata)
    monkeypatch.setattr(sync.researchdata, "harvest", harvest)
    config = Config(attempts=3)
    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client, Http(config, client) as http:
        retry = http.retry

        def immediate_retry() -> Any:
            policy = retry()
            policy.wait = wait_none()
            return policy

        monkeypatch.setattr(http, "retry", immediate_retry)
        first = await sync.fetch(stage, plan, config, tmp_path / "work", tmp_path / "current", http)
        assert first["outcomes"] == {"succeeded": 1, "failed": 1}
        assert attempts == {"good": 1, "bad": 3}
        assert "secret" not in json.dumps(first)
        failing = False
        second = await sync.fetch(stage, plan, config, tmp_path / "work", tmp_path / "current", http)
        assert second["outcomes"] == {"succeeded": 2}
        assert attempts == {"good": 1, "bad": 4}
        artifact = next((tmp_path / "work" / stage).glob("*.json"))
        artifact.write_text("{", encoding="utf-8")
        with pytest.raises(json.JSONDecodeError):
            await sync.fetch(stage, plan, config, tmp_path / "work", tmp_path / "current", http)


async def test_successful_counterpart_reuses_published_rows(tmp_path: Path, monkeypatch: Any) -> None:
    from scripts import sync

    plan, dp, rd = candidate_inputs()
    current = tmp_path / "current"
    build(tmp_path / "absent", current, plan, dp, rd, [])
    uri = plan["selected"][0]
    plan["selected"] = [uri]
    plan.update(base_commit="head", plan_id="plan")

    async def unexpected(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError("A successful persisted counterpart must not be retrieved")

    monkeypatch.setattr(sync.dataportalen, "metadata", unexpected)
    result = await sync.fetch("dataportalen", plan, Config(), tmp_path / "retry", current)
    assert result["results"][uri]["status"] == "succeeded"
    assert result["requests"] == 0
    assert result["results"][uri]["row"]["dataset_uri"] == uri


async def test_malformed_sources_and_deadlines_are_expected_failures(tmp_path: Path) -> None:
    from harvester.researchdata import decode
    from harvester.vocabulary import CacheFetchError, RdfCache

    async def respond(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/slow":
            await asyncio.sleep(1)
        return httpx.Response(200, text="<broken", headers={"Content-Type": "application/xml"})

    config = Config(operation_deadline=0.05, attempts=1)
    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client, Http(config, client) as http:
        cache = RdfCache()
        with pytest.raises(CacheFetchError) as malformed:
            await cache.fetch("https://example.org/malformed", http, decoder=decode, required=True)
        assert not malformed.value.retry
        with pytest.raises(CacheFetchError) as timed_out:
            await cache.fetch("https://example.org/slow", http, decoder=decode, required=True)
        assert timed_out.value.retry


def test_mixed_success_publication_and_advanced_head_rejection(tmp_path: Path) -> None:
    root, remote = tmp_path / "repo", tmp_path / "remote.git"
    root.mkdir()

    def git(*args: str) -> str:
        return subprocess.check_output(["git", *args], cwd=root, text=True).strip()

    git("init", "--bare", str(remote))
    git("init", "-b", "main")
    git("config", "user.name", "Test")
    git("config", "user.email", "test@example.org")
    git("commit", "--allow-empty", "-m", "Initialize test repository")
    git("remote", "add", "origin", str(remote))
    git("push", "origin", "main")
    head = git("rev-parse", "HEAD")
    plan, dp, rd = candidate_inputs()
    plan["base_commit"] = head
    uri = plan["selected"][1]
    dp[uri] = {
        "status": "failed",
        "dataset_uri": uri,
        "stage": "dataportalen",
        "operation": "retrieve_metadata",
        "url": "https://example.org",
        "category": "ReadTimeout",
        "reason": "ReadTimeout",
        "retry": True,
    }
    candidate = tmp_path / "candidate"
    build(root / "data/current", candidate, plan, dp, rd, [])
    publish(candidate, head, repo=root)
    assert git("diff-tree", "--no-commit-id", "--name-only", "-r", "HEAD").splitlines()
    assert all(
        path.startswith("data/current/")
        for path in git("diff-tree", "--no-commit-id", "--name-only", "-r", "HEAD").splitlines()
    )
    assert read_json(root / "data/current/inventory.json")["entries"][uri]["retry"]
    next_head = git("rev-parse", "HEAD")
    plan["base_commit"] = next_head
    build(root / "data/current", tmp_path / "outdated", plan, dp, rd, [])
    git("commit", "--allow-empty", "-m", "Advance main")
    with pytest.raises(ValueError, match="expected head"):
        publish(tmp_path / "outdated", next_head, repo=root)


async def test_complete_sync_retries_only_failed_datasets(tmp_path: Path, monkeypatch: Any) -> None:
    from test_inventory import registry_entry, registry_transport

    from harvester.vocabulary import NAMESPACES
    from scripts import sync

    config = Config(attempts=1)
    entries = [registry_entry(config, key) for key in ("a", "b")]
    members = [next(iter(entry["metadata"])) for entry in entries]
    for entry, uri in zip(entries, members, strict=True):
        entry["metadata"][uri][NAMESPACES["dcat"] + "distribution"] = [{"type": "uri", "value": uri + "/distribution"}]
    requests: list[httpx.Request] = []
    registry = registry_transport(config, entries, members, requests)
    failing = True
    data_requests: list[str] = []

    def respond(request: httpx.Request) -> httpx.Response:
        query = request.url.params.get("query", "").replace("\\", "")
        for uri in members:
            if 'resource:"' + uri + '/distribution"' in query:
                if uri == members[1] and failing:
                    raise httpx.ConnectTimeout("network interruption", request=request)
                graph = {
                    uri + "/distribution": {
                        NAMESPACES["dcat"] + "downloadURL": [{"type": "uri", "value": uri + ".csv"}]
                    }
                }
                return httpx.Response(200, json={"results": 1, "resource": {"children": [{"metadata": graph}]}})
        if request.url.path.endswith(".csv"):
            data_requests.append(str(request.url))
            return httpx.Response(200, text="station,value\nA,12\n", headers={"Content-Type": "text/csv"})
        return registry.handler(request)

    config_path = tmp_path / "config.toml"
    config_path.write_text("attempts = 1\n", encoding="utf-8")
    current = tmp_path / "current"
    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        monkeypatch.setattr(sync, "Http", lambda settings: Http(settings, client))

        def args(work: str) -> Any:
            return sync.parser().parse_args(
                ["--config", str(config_path), "--destination", str(current), "--work-directory", str(tmp_path / work)]
            )

        first = await sync.run(args("first"))
        assert first["counts"] == {"datasets": 1, "dataportalen": 1}
        assert first["newly_deferred"] == 1
        assert first["retry_required"] == 1
        failing = False
        second = await sync.run(args("retry"))
        assert second["selected"] == 1
        assert second["retry_required"] == 0
        assert second["counts"] == {"datasets": 2, "dataportalen": 2}
        unchanged = await sync.run(args("unchanged"))
        assert unchanged["outcome"] == "unchanged"
        assert data_requests == [members[0] + ".csv", members[1] + ".csv"]
