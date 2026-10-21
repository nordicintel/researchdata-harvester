# SPDX-License-Identifier: NOASSERTION
# Copyright (c) 2026 NordicIntel
"""Split oversized staged Parquet files before committing; run with Python -m."""

import argparse
import math
import os
import shutil
import subprocess
import sys
import tempfile
import tomllib
from pathlib import Path

import duckdb


def git(root: Path, *arguments: str) -> bytes:
    """Run Git against the current index, including a commit's temporary index."""
    result = subprocess.run(
        ["git", "-C", str(root), *arguments], capture_output=True, check=False
    )
    if result.returncode:
        raise RuntimeError(result.stderr.decode(errors="replace").strip())
    return result.stdout


def split_file(source: Path, directory: Path, maximum: int, target: int) -> list[Path]:
    """Divide rows evenly using source bytes as the initial size estimate."""
    with duckdb.connect(config={"threads": 1}) as connection:
        relation = connection.read_parquet(str(source))
        rows = relation.count("*").fetchall()[0][0]
        if rows < 2:
            raise ValueError(
                f"{source.name}: fewer than two rows; cannot split safely."
            )
        count = min(rows, max(2, math.ceil(source.stat().st_size / target)))
        while True:
            parts = []
            offset = 0
            for index in range(count):
                length = rows // count + (index < rows % count)
                part = directory / f"part_{index + 1}.parquet"
                relation.limit(length, offset).write_parquet(
                    str(part), compression="zstd"
                )
                parts.append(part)
                offset += length
            largest = max(part.stat().st_size for part in parts)
            if largest < maximum:
                return parts
            for part in parts:
                part.unlink()
            if count == rows:
                raise ValueError(
                    f"{source.name}: a single-row part reaches the maximum size."
                )
            # The target is an estimate, not a per-part validation requirement.
            count = min(rows, max(count + 1, math.floor(count * largest / maximum) + 1))


def split_staged(root: Path, maximum: int, target: int) -> int:
    """Prepare all replacements before changing files or the Git index."""
    changed = git(
        root,
        "diff",
        "--cached",
        "--name-only",
        "--no-renames",
        "--diff-filter=AM",
        "-z",
    )
    candidates = []
    for raw in changed.split(b"\0"):
        if not raw:
            continue
        name = os.fsdecode(raw)
        if Path(name).suffix.lower() != ".parquet":
            continue
        oid = git(root, "rev-parse", f":{name}").decode().strip()
        if int(git(root, "cat-file", "-s", oid)) < maximum:
            continue
        source = root / name
        if source.is_symlink() or not source.resolve().is_relative_to(root):
            raise ValueError(
                f"{name}: refusing a symlink or path outside the repository."
            )
        if (
            not source.is_file()
            or git(root, "hash-object", "--no-filters", "--", name).decode().strip()
            != oid
        ):
            raise ValueError(
                f"{name}: staged and working copies differ. Stage or stash edits first."
            )
        candidates.append((name, oid))

    if not candidates:
        return 0
    with tempfile.TemporaryDirectory(prefix="parquet-hook-") as temporary:
        prepared = []
        for index, (name, oid) in enumerate(candidates):
            directory = Path(temporary) / str(index)
            directory.mkdir()
            source = root / name
            backup = directory / "source.parquet"
            shutil.copyfile(source, backup)
            if (
                git(root, "hash-object", "--no-filters", str(backup)).decode().strip()
                != oid
            ):
                raise ValueError(f"{name}: file changed while preparing the split.")
            parts = split_file(backup, directory, maximum, target)
            destinations = [
                source.with_name(f"{source.stem}_part_{i + 1}{source.suffix}")
                for i in range(len(parts))
            ]
            for destination in destinations:
                relative = destination.relative_to(root).as_posix()
                if (
                    destination.exists()
                    or destination.is_symlink()
                    or git(root, "ls-files", "--", relative)
                ):
                    raise ValueError(
                        f"{relative}: output already exists or is tracked; resolve the "
                        "name conflict before committing."
                    )
            prepared.append((source, backup, parts, destinations))

        for source, backup, _, _ in prepared:
            name = source.relative_to(root).as_posix()
            expected = git(root, "hash-object", "--no-filters", str(backup)).strip()
            if (
                git(root, "hash-object", "--no-filters", "--", name).strip() != expected
                or git(root, "rev-parse", f":{name}").strip() != expected
            ):
                raise ValueError(
                    f"{name}: working file or index changed during splitting."
                )

        created: list[Path] = []
        removed: list[tuple[Path, Path]] = []
        paths = []
        try:
            for source, backup, parts, destinations in prepared:
                for part, destination in zip(parts, destinations, strict=True):
                    with destination.open("xb") as output:
                        created.append(destination)
                        with part.open("rb") as content:
                            shutil.copyfileobj(content, output)
                    paths.append(destination.relative_to(root).as_posix())
                source.unlink()
                removed.append((source, backup))
                paths.append(source.relative_to(root).as_posix())
            # One git-add transaction stages only the explicit replacements.
            git(root, "add", "--force", "--all", "--", *paths)
        except BaseException:
            for source, backup in removed:
                shutil.copyfile(backup, source)
            for destination in created:
                destination.unlink(missing_ok=True)
            raise
        for source, _, parts, _ in prepared:
            sizes = ", ".join(
                f"{part.stat().st_size / 1_000_000:.2f} MB" for part in parts
            )
            print(
                f"parquet-hook: {source.relative_to(root)} -> {len(parts)} parts ({sizes})"
            )
    return len(candidates)


def main() -> int:
    """Read project defaults, optionally install the hook, or process the index."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--install", action="store_true", help="Enable .githooks locally"
    )
    parser.add_argument(
        "--max-mb", type=float, help="Split threshold and hard part limit"
    )
    parser.add_argument("--target-mb", type=float, help="Initial estimated part size")
    args = parser.parse_args()
    try:
        root = Path(
            os.fsdecode(git(Path.cwd(), "rev-parse", "--show-toplevel")).strip()
        )
        if args.install:
            current = subprocess.run(
                ["git", "config", "--get", "core.hooksPath"],
                cwd=root,
                capture_output=True,
                text=True,
                check=False,
            ).stdout.strip()
            if current and current != ".githooks":
                raise ValueError(
                    f"Existing hooksPath {current!r}; integrate it before installing."
                )
            if not current:
                existing = (
                    root
                    / os.fsdecode(
                        git(root, "rev-parse", "--git-path", "hooks/pre-commit")
                    ).strip()
                )
                if existing.exists():
                    raise ValueError(
                        "An existing pre-commit hook must be integrated before installing."
                    )
            hook = root / ".githooks" / "pre-commit"
            hook.chmod(hook.stat().st_mode | 0o111)
            git(root, "config", "--local", "core.hooksPath", ".githooks")
            print("parquet-hook: enabled .githooks/pre-commit for this checkout")
            return 0
        with (root / "pyproject.toml").open("rb") as stream:
            settings = tomllib.load(stream).get("tool", {}).get("parquet-hook", {})
        maximum = (
            args.max_mb if args.max_mb is not None else settings.get("max_mb", 100)
        )
        target = (
            args.target_mb
            if args.target_mb is not None
            else settings.get("target_mb", 50)
        )
        maximum, target = (
            int(float(maximum) * 1_000_000),
            int(float(target) * 1_000_000),
        )
        if not 0 < target < maximum:
            raise ValueError("Sizes must satisfy 0 < target_mb < max_mb (decimal MB).")
        split_staged(root, maximum, target)
        return 0
    except (RuntimeError, ValueError, OSError, duckdb.Error) as error:
        print(f"parquet-hook: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
