# SPDX-License-Identifier: NOASSERTION
# Copyright (c) 2026 NordicIntel
"""Run the same current-catalog harvest locally and in GitHub Actions."""

import argparse
import asyncio
import json
import logging
from pathlib import Path

from harvester.config import Config
from harvester.storage import write_json
from harvester.sync import sync


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=Path("config.toml"))
    parser.add_argument(
        "--full",
        action="store_true",
        help="Refresh all current Researchdata representations.",
    )
    parser.add_argument(
        "--destination", type=Path, help="Override the current catalog directory."
    )
    parser.add_argument(
        "--report", type=Path, help="Write an optional local acceptance report."
    )
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)
    config = Config.load(args.config)
    if args.destination:
        config.current_directory = args.destination.resolve()
    result = asyncio.run(sync(config, full=args.full))
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        write_json(args.report, result)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
