# SPDX-License-Identifier: NOASSERTION
# Copyright (c) 2026 NordicIntel
"""Exercise Parquet, JSONL and gzip splitting through real Git commits."""

import gzip
import hashlib
import json
import os
import shutil
import subprocess
from pathlib import Path

import duckdb
import pytest

from scripts.split_data_files import split_file

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
        ".gitattributes",
        ".githooks/pre-commit",
        "scripts/split_data_files.py",
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


@pytest.mark.parametrize("suffix", [".parquet", ".jsonl", ".parquet.gz", ".jsonl.gz"])
def test_commit_splits_rows_without_staging_unrelated_edits(
    repository: Path, suffix: str
) -> None:
    source = repository / f"sample data{suffix}"
    if ".jsonl" in suffix:
        expected_bytes = b"\r\n".join(
            json.dumps(
                {
                    "id": i,
                    "label": "å",
                    "value": hashlib.md5(str(i).encode()).hexdigest(),
                },
                ensure_ascii=False,
            ).encode()
            for i in range(4000)
        )
        source.write_bytes(expected_bytes)
    else:
        expected = create_data(source)
    if suffix.endswith(".gz"):
        source.write_bytes(gzip.compress(source.read_bytes(), mtime=0))
    assert source.stat().st_size >= 40_000
    untouched = repository / "small.parquet"
    with duckdb.connect() as connection:
        connection.sql("SELECT 1 AS id").write_parquet(str(untouched))
    small_bytes = untouched.read_bytes()
    git(repository, "add", source.name, "small.parquet")
    (repository / "local.txt").write_text("Do not commit this")
    result = git(repository, "commit", "-m", "Split a dataset")
    assert "split-data-files:" in result.stderr + result.stdout
    parts = sorted(
        repository.glob(f"sample data_part_*{suffix}"),
        key=lambda p: int(p.name.rsplit("_", 1)[1].split(".")[0]),
    )
    assert len(parts) >= 2
    assert not source.exists()
    assert all(p.stat().st_size < 40_000 for p in parts)
    if ".jsonl" in suffix:
        contents = [
            gzip.decompress(p.read_bytes())
            if suffix.endswith(".gz")
            else p.read_bytes()
            for p in parts
        ]
        assert b"".join(contents) == expected_bytes
        assert (
            sum(
                len([json.loads(line) for line in data.splitlines()])
                for data in contents
            )
            == 4000
        )
    else:
        with duckdb.connect() as connection:
            actual = []
            for part in parts:
                decoded = repository / "decoded.parquet"
                if suffix.endswith(".gz"):
                    decoded.write_bytes(gzip.decompress(part.read_bytes()))
                actual.extend(
                    connection.read_parquet(
                        str(decoded if suffix.endswith(".gz") else part)
                    ).fetchall()
                )
                decoded.unlink(missing_ok=True)
        assert actual == expected
    assert untouched.read_bytes() == small_bytes
    tracked = git(
        repository, "ls-tree", "-r", "--name-only", "HEAD"
    ).stdout.splitlines()
    assert source.name not in tracked
    assert all(p.name in tracked for p in parts)
    assert "local.txt" not in tracked
    git(repository, "commit", "--allow-empty", "-qm", "Unchanged files")
    assert list(repository.glob(f"*_part_*_part_*{suffix}")) == []


@pytest.mark.parametrize(
    "problem", ["unstaged", "collision", "malformed", "truncated-gzip"]
)
def test_failed_commit_preserves_index_and_files(
    repository: Path, problem: str
) -> None:
    source = repository / (
        "data.parquet.gz" if problem == "truncated-gzip" else "data.parquet"
    )
    create_data(source)
    if problem == "malformed":
        source.write_bytes(b"invalid parquet" * 4000)
    if problem == "truncated-gzip":
        source.write_bytes(gzip.compress(source.read_bytes())[:-8])
        assert source.stat().st_size >= 40_000
    git(repository, "add", source.name)
    if problem == "unstaged":
        source.write_bytes(source.read_bytes() + b"uncommitted")
    if problem == "collision":
        (repository / "data_part_1.parquet").write_bytes(b"Keep this file")
    before = source.read_bytes()
    index = git(repository, "write-tree").stdout
    result = git(repository, "commit", "-qm", "Must fail", check=False)
    assert result.returncode != 0
    assert "split-data-files:" in result.stderr
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
