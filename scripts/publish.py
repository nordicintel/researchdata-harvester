# SPDX-License-Identifier: NOASSERTION
# Copyright (c) 2026 NordicIntel
"""Publish one validated data-only commit without overwriting newer main commits."""

import argparse
import subprocess
from pathlib import Path

from harvester.storage import validate_catalog


def git(*arguments: str) -> str:
    return subprocess.check_output(["git", *arguments], text=True).strip()


def publish(expected_head: str) -> bool:
    validate_catalog(Path("data/current"))
    if git("rev-parse", "HEAD") != expected_head:
        raise ValueError(
            "The checkout advanced during harvesting; rerun from the current main branch."
        )
    git("fetch", "origin", "main")
    if git("rev-parse", "FETCH_HEAD") != expected_head:
        raise ValueError(
            "Remote main advanced during harvesting; rerun from the new main commit."
        )
    if git("diff", "--cached", "--name-only"):
        raise ValueError(
            "The index already contains changes; publication requires a clean index."
        )
    git("add", "--all", "--", "data/current")
    if not git("diff", "--cached", "--name-only"):
        return False
    git("commit", "-m", "Refresh the current Researchdata catalog")
    git("push", "origin", "HEAD:main")
    return True


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--expected-head", required=True)
    args = parser.parse_args()
    print(
        "Published current catalog."
        if publish(args.expected_head)
        else "Current catalog is unchanged."
    )


if __name__ == "__main__":
    main()
