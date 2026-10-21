# SPDX-License-Identifier: NOASSERTION
# Copyright (c) 2026 NordicIntel
"""Standalone command-line entry points for harvesting and inspecting metadata."""

import argparse
import asyncio
import json
import sys
from pathlib import Path

from researchdata_harvester.config import Config
from researchdata_harvester.harvester import Harvester


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(description=__doc__)
    root.add_argument(
        "--config",
        type=Path,
        default=Path(__file__).resolve().parents[2] / "config.toml",
    )
    root.add_argument("--database", type=Path)
    root.add_argument("--timeout", type=float)
    root.add_argument("--concurrency", type=int)
    root.add_argument("--retries", type=int)
    root.add_argument(
        "--dataportalen-scope",
        choices=("researchdata", "all"),
        help="Dataportalen inventory scope; default Researchdata catalog, all for a global audit",
    )
    root.add_argument(
        "--formats",
        action="append",
        help="Repeat for each OAI prefix; default datacite dcat json_ld",
    )
    commands = root.add_subparsers(dest="command", required=True)
    sync = commands.add_parser(
        "sync",
        help="Harvest metadata, reconcile and scan the selected Dataportalen catalog",
    )
    mode = sync.add_mutually_exclusive_group(required=True)
    mode.add_argument("--full", action="store_true")
    mode.add_argument("--incremental", action="store_true")
    record = commands.add_parser(
        "record", help="Fetch this exact version from the source"
    )
    record.add_argument("identifier")
    search = commands.add_parser("search", help="Search local metadata")
    search.add_argument("text")
    search.add_argument("--limit", type=int, default=100)
    listing = commands.add_parser(
        "files", help="Inspect known explicit file URLs and download status"
    )
    listing.add_argument("identifier")
    download = commands.add_parser(
        "download", help="Download only explicit metadata file URLs"
    )
    download.add_argument("identifier")
    download.add_argument("--destination", type=Path)
    download.add_argument(
        "--url",
        action="append",
        help="Select an advertised file URL; repeat to select several files",
    )
    export = commands.add_parser(
        "export", help="Publish an atomic JSONL snapshot generation"
    )
    export.add_argument("--destination", type=Path)
    commands.add_parser(
        "status", help="Inspect catalog counts, runs and source checkpoints"
    )
    return root


async def execute(args: argparse.Namespace) -> object:
    config = Config.load(args.config) if args.config.exists() else Config()
    changes = {
        name: getattr(args, name)
        for name in (
            "database",
            "timeout",
            "concurrency",
            "retries",
            "formats",
            "dataportalen_scope",
        )
        if getattr(args, name) is not None
    }
    config = Config.model_validate({**config.model_dump(), **changes})
    async with Harvester(
        config, progress=lambda message: print(message, file=sys.stderr, flush=True)
    ) as harvester:
        match args.command:
            case "sync":
                return await harvester.sync(full=args.full)
            case "record":
                return await harvester.fetch_record(args.identifier)
            case "search":
                return harvester.store.search(args.text, args.limit)
            case "files":
                return harvester.store.file_entries(args.identifier)
            case "download":
                return await harvester.download(
                    args.identifier, args.destination, urls=args.url
                )
            case "export":
                return {
                    "snapshot": str(
                        harvester.store.export(
                            args.destination or config.export_directory
                        )
                    )
                }
            case "status":
                return harvester.store.status()
    raise ValueError("Unknown command.")


def main() -> None:
    try:
        result = asyncio.run(execute(parser().parse_args()))
    except (ValueError, OSError, RuntimeError) as error:
        print(f"Error: {error}", file=sys.stderr)
        raise SystemExit(1) from error
    print(json.dumps(result, ensure_ascii=True, indent=2, default=str))


if __name__ == "__main__":
    main()
