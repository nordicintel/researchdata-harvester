# SPDX-License-Identifier: NOASSERTION
# Copyright (c) 2026 NordicIntel
"""Validate and publish only generated current data against an unchanged main head."""

import argparse
import subprocess
from pathlib import Path

from harvester.output import lock, replace, validate


def publish(candidate: Path, expected_head: str, repo: Path | None = None) -> None:
    manifest = validate(candidate)
    if not manifest.get("complete"):
        raise ValueError("Scoped or incomplete candidates cannot be published")
    if manifest.get("base_commit") != expected_head:
        raise ValueError("Candidate does not belong to the expected base commit")
    root = repo or Path(__file__).resolve().parents[1]

    def git(*arguments: str) -> str:
        return subprocess.check_output(["git", *arguments], cwd=root, text=True).strip()

    if git("branch", "--show-current") != "main" or git("rev-parse", "HEAD") != expected_head:
        raise ValueError("Publication requires main checked out at the expected head")
    if git("diff", "--cached", "--name-only"):
        raise ValueError("Publication requires a clean Git index")
    if git("diff", "--name-only", "--", "data/current"):
        raise ValueError("Current data has local edits; publication would overwrite them")
    remote = git("ls-remote", "origin", "refs/heads/main").split()[0]
    if remote != expected_head:
        raise ValueError("Remote main advanced; update the checkout and rerun")
    destination = root / "data/current"
    with lock(destination):
        replace(candidate, destination)
        git("add", "--", "data/current")
        staged = git("diff", "--cached", "--name-only").splitlines()
        if not staged:
            return
        if any(not name.startswith("data/current/") for name in staged):
            raise ValueError("Publication index includes files outside current data")
        if git("ls-remote", "origin", "refs/heads/main").split()[0] != expected_head:
            raise ValueError("Remote main advanced before commit; rerun against newer state")
        git("commit", "-m", "Refresh current research datasets")
        validate(destination)
        git("push", "origin", "HEAD:main")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--expected-head", required=True)
    args = parser.parse_args()
    publish(args.candidate, args.expected_head)


if __name__ == "__main__":
    main()
