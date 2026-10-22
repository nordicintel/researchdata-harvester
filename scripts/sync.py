# SPDX-License-Identifier: NOASSERTION
# Copyright (c) 2026 NordicIntel
"""Run the same inventory, retrieval, build and validation stages locally and in Actions."""

import argparse
import asyncio
import json
import logging
import os
import subprocess
import time
from collections import Counter
from contextlib import AsyncExitStack
from pathlib import Path
from typing import Any, cast

import httpx
from pyld.jsonld import JsonLdError

from harvester import dataportalen, output, researchdata
from harvester.runtime import (
    Config,
    Http,
    RecordError,
    fingerprint,
    read_json,
    record_key,
    safe_url,
    transient,
    validate_result,
    write_json,
)
from harvester.vocabulary import RdfCache, SemanticResolver

ROOT = Path(__file__).resolve().parents[1]
logger = logging.getLogger(__name__)


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--config", type=Path, default=ROOT / "config.toml")
    result.add_argument("--destination", type=Path)
    result.add_argument("--work-directory", type=Path, default=ROOT / "data/work/run")
    result.add_argument("--report", type=Path)
    result.add_argument("--full", action="store_true")
    result.add_argument(
        "--stage", choices=["all", "inventory", "dataportalen", "researchdata", "build", "validate"], default="all"
    )
    scope = result.add_mutually_exclusive_group()
    scope.add_argument("--dataset-uri", action="append")
    scope.add_argument("--limit", type=int)
    result.add_argument("--log-level", choices=["DEBUG", "INFO", "WARNING", "ERROR"], default="INFO")
    return result


def read_cache(path: Path) -> dict[str, Any]:
    import gzip

    with gzip.open(path, "rt", encoding="utf-8") as stream:
        return cast(dict[str, Any], json.load(stream))


def implementation() -> str:
    return fingerprint(
        {
            path.relative_to(ROOT).as_posix(): path.read_text(encoding="utf-8-sig")
            for directory in (ROOT / "harvester", ROOT / "scripts")
            for path in sorted(directory.iterdir())
            if path.suffix in {".py", ".sql"}
        }
    )


async def fetch(
    stage: str, plan: dict[str, Any], config: Config, work: Path, current: Path, shared_http: Http | None = None
) -> dict[str, Any]:
    saved_cache = work / (stage + "-cache.json.gz")
    cache = RdfCache.load(saved_cache if saved_cache.exists() else current / "rdf-cache.json.gz")
    results: dict[str, Any] = {}
    gate = asyncio.Semaphore(config.concurrency)
    directory = work / stage
    directory.mkdir(parents=True, exist_ok=True)
    prior = read_json(current / "inventory.json", {"entries": {}})["entries"]
    published: dict[str, Any] | None = None
    completed = sorted((read_json(path).get("sequence", 0), path) for path in directory.glob("*.json"))
    for _, path in completed:
        cache.merge(read_json(path).get("cache_updates", {}))
        cache.mark_saved()
    # Consolidate replayed deltas before any record artifact can be replaced.
    cache.save(saved_cache)
    sequence = max((number for number, _ in completed), default=0)
    async with AsyncExitStack() as stack:
        if shared_http is None:
            http = Http(config)
            await stack.enter_async_context(http)
        else:
            http = shared_http
        resolver = await stack.enter_async_context(
            SemanticResolver(
                current / "rdf-cache.json.gz",
                http,
                directory / "semantic-updates.json.gz",
                cache=cache,
                full=plan["full"],
            )
        )

        async def record(uri: str) -> None:
            nonlocal sequence, published
            async with gate:
                entry = plan["entries"][uri]
                artifact = directory / (fingerprint(uri) + ".json")
                key = record_key(plan, entry)
                previous = read_json(artifact)
                if previous and previous.get("key") == key:
                    validate_result(previous.get("result"), stage, uri)
                if (
                    previous
                    and previous.get("key") == key
                    and not previous["result"].get("retry")
                    and (
                        not plan["full"]
                        or (previous.get("plan_id") == plan["plan_id"] and previous["result"]["status"] == "succeeded")
                    )
                ):
                    results[uri] = previous["result"]
                    logger.info("%s reused %s", stage, uri)
                    return
                before = prior.get(uri, {})
                stage_state = before.get("stages", {}).get(stage, {})
                if (
                    not plan["full"]
                    and before.get("published_keys", {}).get(stage) == key
                    and stage_state.get("status") == "succeeded"
                    and not stage_state.get("retry")
                ):
                    if published is None:
                        manifest = output.validate(current)
                        table = "dataportalen" if stage == "dataportalen" else "datasets"
                        published = output.load_rows(current, table, manifest)
                    if uri in published and (stage == "dataportalen" or published[uri]["availability"] == "available"):
                        row = published[uri]
                        result = {
                            "status": "succeeded",
                            "row": row,
                            "cache_references": before.get("cache_references", []),
                        }
                        if stage == "researchdata":
                            result.update({key: row[key] for key in ("resources", "researchdata", "sources")})
                            result.update(outcome="eligible", retry=False)
                        results[uri] = result
                        logger.info("%s reused published dataset=%s", stage, uri)
                        return
                try:
                    dp = await dataportalen.metadata(http, config, entry, cache)
                    if stage == "dataportalen":
                        for document in dp["documents"]:
                            try:
                                normalized = await resolver.resolve(
                                    document["metadata"], source_url=document["url"], required=False
                                )
                            except ValueError as error:
                                raise RecordError(type(error).__name__, url=document["url"]) from error
                            document["properties"] = normalized.properties
                        result = dp
                    else:
                        result = await researchdata.harvest(
                            http,
                            config,
                            dp,
                            cache,
                            current / "rdf-cache.json.gz",
                            directory / "semantic-updates.json.gz",
                            full=plan["full"],
                            resolver=resolver,
                        )
                    result["status"] = "succeeded"
                except (RecordError, httpx.HTTPError, JsonLdError) as error:
                    url = (
                        error.url
                        if isinstance(error, RecordError)
                        else (
                            safe_url(str(error.request.url))
                            if isinstance(error, httpx.HTTPError) and error.request
                            else ""
                        )
                    )
                    retry = error.retry if isinstance(error, RecordError) else transient(error)
                    reason = str(error) if isinstance(error, RecordError) else type(error).__name__
                    result = {
                        "status": "failed",
                        "dataset_uri": uri,
                        "stage": stage,
                        "operation": "retrieve_metadata",
                        "url": url,
                        "category": type(error).__name__,
                        "reason": reason,
                        "retry": retry,
                    }
                    logger.warning("%s dataset=%s reason=%s retry=%s", stage, uri, reason, retry)
                except Exception:
                    logger.exception("%s failed dataset=%s", stage, uri)
                    raise
                results[uri] = result
                validate_result(result, stage, uri)
                sequence += 1
                write_json(
                    artifact,
                    {
                        "key": key,
                        "plan_id": plan["plan_id"],
                        "result": result,
                        "sequence": sequence,
                        "cache_updates": cache.changes(),
                    },
                )
                cache.mark_saved()
                logger.info("%s %d/%d %s status=%s", stage, len(results), len(plan["selected"]), uri, result["status"])

        async with asyncio.TaskGroup() as tasks:
            for uri in plan["selected"]:
                tasks.create_task(record(uri))
        cache.save(saved_cache)
        baseline_cache = read_cache(current / "rdf-cache.json.gz") if (current / "rdf-cache.json.gz").exists() else {}
        payload = {
            "implementation": plan["implementation"],
            "configuration": plan["configuration"],
            "base_commit": plan["base_commit"],
            "plan_id": plan["plan_id"],
            "results": results,
            "cache_updates": {
                key: value for key, value in read_cache(saved_cache).items() if value != baseline_cache.get(key)
            }
            if saved_cache.exists()
            else cache.changes(),
            "requests": http.requests,
            "failures": http.failures,
            "elapsed_seconds": time.monotonic() - http.started,
        }
        payload["outcomes"] = dict(Counter(item["status"] for item in results.values()))
        payload["retryable"] = sum(bool(item.get("retry")) for item in results.values())
        payload["retained_stale"] = sum(
            (item["status"] == "failed" or item.get("outcome") in {"unavailable", "unresolved"})
            and prior.get(uri, {}).get("outcome") in {"eligible", "unavailable"}
            for uri, item in results.items()
        )
        payload["newly_deferred"] = sum(
            item["status"] == "failed" and prior.get(uri, {}).get("outcome") not in {"eligible", "unavailable"}
            for uri, item in results.items()
        )
        if os.environ.get("GITHUB_STEP_SUMMARY"):
            with open(os.environ["GITHUB_STEP_SUMMARY"], "a", encoding="utf-8") as summary:
                summary.write(
                    f"### {stage}\n{json.dumps(payload['outcomes'])}; retryable: {payload['retryable']}; "
                    f"retained stale: {payload['retained_stale']}; newly deferred: {payload['newly_deferred']}\n"
                )
        write_json(work / (stage + ".json"), payload)
        return payload


async def run(args: argparse.Namespace) -> dict[str, Any]:
    started = time.monotonic()
    config = Config.load(args.config.resolve())
    current = (args.destination or config.current_directory).resolve()
    work = args.work_directory.resolve()
    if args.stage == "validate":
        return output.validate(current)
    scoped = bool(args.dataset_uri or args.limit)
    if args.limit is not None and args.limit < 1:
        raise ValueError("--limit must be positive")
    if scoped:
        ignored = subprocess.run(["git", "check-ignore", "--quiet", str(current)], cwd=ROOT, check=False)
        if ignored.returncode != 0 or current == config.current_directory.resolve():
            raise ValueError("Scoped runs require an ignored destination outside production current data")
    work.mkdir(parents=True, exist_ok=True)
    config_hash = fingerprint(
        {key: value for key, value in config.model_dump(mode="json").items() if key != "current_directory"}
    )
    code_hash = implementation()
    with output.lock(current):
        output.recover(current)
        if current.exists() and any(current.iterdir()) and not (current / "manifest.json").exists():
            raise ValueError(
                "Nonempty destination has no manifest; choose an empty directory or a valid current generation"
            )
        if args.stage in {"all", "inventory"}:
            prior = read_json(current / "inventory.json", {"entries": {}})
            if (current / "manifest.json").exists():
                output.validate(current)
            async with Http(config) as http:
                plan = await dataportalen.inventory(
                    http, config, prior, full=args.full, dataset_uris=args.dataset_uri, limit=args.limit
                )
                plan.update(
                    configuration=config_hash,
                    implementation=code_hash,
                    base_commit=subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
                    inventory_requests=http.requests,
                    inventory_seconds=time.monotonic() - http.started,
                )
            clean_entries = {
                uri: {key: value for key, value in entry.items() if key != "metadata"}
                for uri, entry in plan["entries"].items()
            }
            plan["has_changes"] = bool(plan["selected"] or plan["removed"] or clean_entries != prior.get("entries", {}))
            plan["plan_id"] = fingerprint(plan)
            write_json(work / "inventory.json", plan)
        else:
            plan = read_json(work / "inventory.json")
            if not plan or plan["configuration"] != config_hash or plan["implementation"] != code_hash:
                raise ValueError("Work artifacts do not match this configuration/implementation; rerun inventory")
        if args.stage == "inventory":
            return {
                "selected": len(plan["selected"]),
                "removed": len(plan["removed"]),
                "complete": plan["complete"],
                "base_commit": plan["base_commit"],
                "has_changes": plan["has_changes"],
            }
        if args.stage == "all" and not plan["has_changes"]:
            report = {
                "outcome": "unchanged",
                "requests": plan["inventory_requests"],
                "elapsed_seconds": time.monotonic() - started,
            }
            write_json(args.report or work / "report.json", report)
            return report
        if args.stage in {"dataportalen", "researchdata"}:
            stage_result = await fetch(args.stage, plan, config, work, current)
            return {
                "stage": args.stage,
                "records": len(stage_result["results"]),
                "outcomes": stage_result["outcomes"],
                "retryable": stage_result["retryable"],
                "requests": stage_result["requests"],
                "elapsed_seconds": stage_result["elapsed_seconds"],
            }
        if args.stage == "all":
            async with Http(config) as shared_http:
                async with asyncio.TaskGroup() as tasks:
                    dp_task = tasks.create_task(fetch("dataportalen", plan, config, work, current, shared_http))
                    rd_task = tasks.create_task(fetch("researchdata", plan, config, work, current, shared_http))
                dp, rd = dp_task.result(), rd_task.result()
                fetch_requests = shared_http.requests
        else:
            dp, rd = (read_json(work / (stage + ".json")) for stage in ("dataportalen", "researchdata"))
            fetch_requests = dp["requests"] + rd["requests"]
        for artifact in (dp, rd):
            if not artifact or any(
                artifact[key] != plan[key] for key in ("implementation", "configuration", "base_commit", "plan_id")
            ):
                raise ValueError("Fetch artifacts do not match the inventory generation")
        candidate = work / "candidate"
        if candidate.exists():
            raise ValueError(f"Candidate already exists at {candidate}; use a fresh work directory for rebuilding")
        manifest = output.build(
            current, candidate, plan, dp["results"], rd["results"], [dp["cache_updates"], rd["cache_updates"]]
        )
        inventory_state = read_json(candidate / "inventory.json")
        report = {
            "complete": plan["complete"],
            "selected": len(plan["selected"]),
            "removed": len(plan["removed"]),
            "unchanged": len(plan["entries"]) - len(plan["selected"]),
            "reasons": dict(
                Counter(item.get("reason") for item in inventory_state["entries"].values() if item.get("reason"))
            ),
            "stages": {
                stage: {
                    **artifact["outcomes"],
                    **{key: artifact[key] for key in ("retryable", "retained_stale", "newly_deferred")},
                }
                for stage, artifact in (("dataportalen", dp), ("researchdata", rd))
            },
            "stage_failures": {
                stage: [item for item in artifact["results"].values() if item.get("status") == "failed"]
                for stage, artifact in (("dataportalen", dp), ("researchdata", rd))
            },
            "retained_stale": sum(item.get("outcome") == "unavailable" for item in inventory_state["entries"].values()),
            "newly_deferred": sum(
                bool(item.get("failures")) and item.get("outcome") == "unresolved"
                for item in inventory_state["entries"].values()
            ),
            "retry_required": sum(bool(item.get("retry")) for item in inventory_state["entries"].values()),
            "outcomes": dict(Counter(item.get("outcome", "pending") for item in inventory_state["entries"].values())),
            "requests": plan["inventory_requests"] + fetch_requests,
            "elapsed_seconds": time.monotonic() - started,
            "counts": manifest["counts"],
            "files": manifest["files"],
        }
        write_json(args.report or work / "report.json", report)
        if args.stage == "all":
            output.replace(candidate, current)
        return report


def main() -> None:
    args = parser().parse_args()
    logging.basicConfig(level=args.log_level, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    try:
        result = asyncio.run(run(args))
        if args.stage == "inventory" and os.environ.get("GITHUB_OUTPUT"):
            with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as stream:
                for key in ("base_commit", "has_changes"):
                    stream.write(f"{key}={str(result[key]).lower()}\n")
        print(json.dumps(result, default=str, ensure_ascii=False))
    except Exception:
        logger.exception("Sync failed; published state was not advanced")
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
