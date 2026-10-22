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

from harvester import dataportalen, output, researchdata
from harvester.runtime import Config, Http, fingerprint, read_json, write_json
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
            str(path.relative_to(ROOT)): path.read_text(encoding="utf-8-sig")
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
            async with gate:
                entry = plan["entries"][uri]
                artifact = directory / (fingerprint(uri) + ".json")
                key = fingerprint([plan["implementation"], plan["configuration"], entry, plan.get("full")])
                previous = read_json(artifact)
                if previous and previous.get("key") == key and not previous["result"].get("retry"):
                    results[uri] = previous["result"]
                    cache.merge(previous.get("cache_updates", {}))
                    logger.info("%s reused %s", stage, uri)
                    return
                try:
                    dp = await dataportalen.metadata(http, config, entry, cache)
                    if stage == "dataportalen":
                        for document in dp["documents"]:
                            normalized = await resolver.resolve(document["metadata"], source_url=document["url"])
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
                    results[uri] = result
                    cache.save(saved_cache)
                    write_json(artifact, {"key": key, "result": result})
                    logger.info(
                        "%s %d/%d %s outcome=%s",
                        stage,
                        len(results),
                        len(plan["selected"]),
                        uri,
                        result.get("outcome", "retrieved"),
                    )
                except Exception:
                    logger.exception("%s failed dataset=%s", stage, uri)
                    raise

        async with asyncio.TaskGroup() as tasks:
            for uri in plan["selected"]:
                tasks.create_task(record(uri))
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
            return {
                "outcome": "unchanged",
                "requests": plan["inventory_requests"],
                "elapsed_seconds": time.monotonic() - started,
            }
        if args.stage in {"dataportalen", "researchdata"}:
            stage_result = await fetch(args.stage, plan, config, work, current)
            return {
                "stage": args.stage,
                "records": len(stage_result["results"]),
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
