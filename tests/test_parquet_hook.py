# SPDX-License-Identifier: NOASSERTION
# Copyright (c) 2026 NordicIntel
"""Exercise automatic Parquet splitting through a real Git pre-commit hook."""

import os
import shutil
import subprocess
from pathlib import Path

import duckdb
import pytest

from scripts.parquet_hook import split_file

PROJECT = Path(__file__).resolve().parents[1]


def git(root: Path, *args: str, check: bool = True) -> subprocess.CompletedProcess:
    environment = os.environ.copy()
    for key in ("GIT_DIR", "GIT_WORK_TREE", "GIT_INDEX_FILE"):
        environment.pop(key, None)
    environment["UV_PROJECT_ENVIRONMENT"] = str(PROJECT / ".venv")
    result = subprocess.run(
        ["git", "-C", str(root), *args],
        capture_output=True,
        text=True,
        check=False,
        env=environment,
    )
    if check:
        assert result.returncode == 0, result.stderr
    return result


@pytest.fixture
def repository(tmp_path: Path) -> Path:
    for name in (
        "pyproject.toml",
        "uv.lock",
        ".githooks/pre-commit",
        "scripts/parquet_hook.py",
    ):
        destination = tmp_path / name
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(PROJECT / name, destination)
    config = tmp_path / "pyproject.toml"
    config.write_text(
        config.read_text()
        .replace("max_mb = 100", "max_mb = 0.04")
        .replace("target_mb = 50", "target_mb = 0.02")
    )
    hook = tmp_path / ".githooks/pre-commit"
    hook.chmod(hook.stat().st_mode | 0o111)
    git(tmp_path, "init", "-q")
    git(tmp_path, "config", "user.name", "Hook test")
    git(tmp_path, "config", "user.email", "hook@example.invalid")
    git(tmp_path, "config", "core.hooksPath", ".githooks")
    git(tmp_path, "add", ".")
    git(tmp_path, "commit", "-qm", "Initial test repository")
    return tmp_path


def create_data(path: Path) -> list[tuple]:
    with duckdb.connect() as connection:
        data = connection.sql(
            "SELECT i AS id, md5(i::VARCHAR) AS value, "
            "CASE WHEN i % 3 = 0 THEN NULL ELSE [i, i + 1] END AS nested, "
            "(i / 100)::DECIMAL(12, 2) AS amount FROM range(4000) t(i)"
        )
        expected = data.fetchall()
        data.write_parquet(str(path), compression="uncompressed")
        return expected


def test_commit_splits_rows_without_staging_unrelated_edits(repository: Path) -> None:
    source = repository / "sample data.parquet"
    expected = create_data(source)
    untouched = repository / "small.parquet"
    with duckdb.connect() as connection:
        connection.sql("SELECT 1 AS id").write_parquet(str(untouched))
    small_bytes = untouched.read_bytes()
    git(repository, "add", "sample data.parquet", "small.parquet")
    (repository / "local.txt").write_text("Do not commit this")
    result = git(repository, "commit", "-m", "Split a dataset")
    assert "parquet-hook:" in result.stderr + result.stdout
    parts = sorted(
        repository.glob("sample data_part_*.parquet"),
        key=lambda p: int(p.stem.rsplit("_", 1)[1]),
    )
    assert len(parts) >= 2
    assert not source.exists()
    assert all(p.stat().st_size < 40_000 for p in parts)
    with duckdb.connect() as connection:
        actual = [
            row for p in parts for row in connection.read_parquet(str(p)).fetchall()
        ]
    assert actual == expected
    assert untouched.read_bytes() == small_bytes
    tracked = git(
        repository, "ls-tree", "-r", "--name-only", "HEAD"
    ).stdout.splitlines()
    assert "sample data.parquet" not in tracked
    assert all(p.name in tracked for p in parts)
    assert "local.txt" not in tracked
    git(repository, "commit", "--allow-empty", "-qm", "Unchanged files")
    assert list(repository.glob("*_part_*_part_*.parquet")) == []


@pytest.mark.parametrize("problem", ["unstaged", "collision", "malformed"])
def test_failed_commit_preserves_index_and_files(
    repository: Path, problem: str
) -> None:
    source = repository / "data.parquet"
    create_data(source)
    if problem == "malformed":
        source.write_bytes(b"invalid parquet" * 4000)
    git(repository, "add", "data.parquet")
    if problem == "unstaged":
        source.write_bytes(source.read_bytes() + b"uncommitted")
    if problem == "collision":
        (repository / "data_part_1.parquet").write_bytes(b"Keep this file")
    before = source.read_bytes()
    index = git(repository, "write-tree").stdout
    result = git(repository, "commit", "-qm", "Must fail", check=False)
    assert result.returncode != 0
    assert "parquet-hook:" in result.stderr
    assert git(repository, "write-tree").stdout == index
    assert source.read_bytes() == before
    parts = list(repository.glob("data_part_*.parquet"))
    assert len(parts) == (1 if problem == "collision" else 0)
    if parts:
        assert parts[0].read_bytes() == b"Keep this file"


def test_parts_can_exceed_target_when_below_maximum(tmp_path: Path) -> None:
    source = tmp_path / "wide.parquet"
    with duckdb.connect() as connection:
        columns = ", ".join(f"i AS column_{i}" for i in range(100))
        connection.sql(f"SELECT {columns} FROM range(100) t(i)").write_parquet(
            str(source), compression="zstd"
        )
    maximum = source.stat().st_size
    target = maximum // 2 + 1
    parts = split_file(source, tmp_path, maximum, target)
    assert len(parts) == 2
    assert any(part.stat().st_size > target for part in parts)
    assert all(part.stat().st_size < maximum for part in parts)
