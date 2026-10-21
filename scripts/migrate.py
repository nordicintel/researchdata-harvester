# SPDX-License-Identifier: NOASSERTION
# Copyright (c) 2026 NordicIntel
"""Convert the former SQLite catalog once and verify current data retention."""

import argparse
import hashlib
import json
import sqlite3
import tempfile
from pathlib import Path
from typing import Any

from harvester.config import Config
from harvester.dataportalen import triples_to_metadata
from harvester.matching import match_records
from harvester.researchdata import make_record
from harvester.storage import (
    CatalogLock,
    activate,
    fingerprint,
    write_catalog,
    write_json,
)


def verified_download(path: str, metadata: dict[str, Any], root: Path) -> str:
    original = Path(path)
    candidates = [original]
    parts = original.parts
    if "downloads" in parts:
        candidates.append(root / "data" / Path(*parts[parts.index("downloads") :]))
    for candidate in candidates:
        if not candidate.is_file():
            continue
        expected_size = metadata.get("size")
        if expected_size is not None and candidate.stat().st_size != int(expected_size):
            continue
        checksum = metadata.get("checksum") or (
            {"algorithm": "sha256", "value": metadata["sha256"]}
            if metadata.get("sha256")
            else None
        )
        if checksum:
            if isinstance(checksum, dict):
                algorithm = checksum.get("algorithm", "sha256").lower().replace("-", "")
                expected = checksum.get("value", "")
            else:
                algorithm, separator, expected = str(checksum).partition(":")
                if not separator:
                    algorithm, expected = "sha256", algorithm
            with candidate.open("rb") as stream:
                actual = hashlib.file_digest(stream, algorithm).hexdigest()
            if actual.lower() != expected.lower():
                continue
        return str(candidate.resolve())
    raise ValueError(f"Previously downloaded file cannot be verified: {path}")


def migrate(
    database: Path, destination: Path, *, report_path: Path | None = None
) -> dict[str, Any]:
    if destination.exists():
        raise ValueError(
            "Migration destination exists; choose a new directory or sync the current catalog."
        )
    root = Path(__file__).resolve().parents[1]
    with sqlite3.connect(
        database.resolve().as_uri() + "?mode=ro", uri=True
    ) as connection:
        connection.row_factory = sqlite3.Row
        research = {}
        source_representation_hashes = {}
        for row in connection.execute(
            "SELECT * FROM research_records WHERE current_catalog=1 AND deleted=0 ORDER BY identifier"
        ):
            representations = {
                item["format"]: json.loads(item["metadata"])
                for item in connection.execute(
                    "SELECT format,metadata FROM representations WHERE identifier=? AND present=1 AND catalogue=1",
                    (row["identifier"],),
                )
            }
            source_representation_hashes[row["identifier"]] = fingerprint(
                representations
            )
            research[row["identifier"]] = make_record(
                row["identifier"],
                row["datestamp"],
                json.loads(row["provenance"]).get("sets", []),
                representations,
            )
        dataportalen = {
            row["uri"]: {
                "uri": row["uri"],
                "identities": json.loads(row["identities"]),
                "metadata": triples_to_metadata(json.loads(row["triples"]), row["uri"]),
            }
            for row in connection.execute(
                "SELECT * FROM dataportalen_records WHERE in_scope=1 AND present=1 AND metadata_complete=1 ORDER BY uri"
            )
        }
        legacy_links = [
            {
                key: row[key]
                for key in ("identifier", "uri", "rule", "relation", "status")
            }
            | {"evidence": json.loads(row["evidence"])}
            for row in connection.execute(
                "SELECT * FROM links ORDER BY identifier,uri,rule"
            )
            if row["identifier"] in research and row["uri"] in dataportalen
        ]
        catalog = {
            "members": sorted(dataportalen),
            "catalog": "https://researchdata.se/catalogue/",
            "source": "https://admin.dataportal.se/store/818/metadata/9198",
        }
        links, matches, fingerprints = match_records(
            research, dataportalen, legacy_links, {}, {}, catalog
        )
        old_pairs = {
            (row["identifier"], row["uri"], row["status"]) for row in legacy_links
        }
        new_pairs = {(row["identifier"], row["uri"], row["status"]) for row in links}
        if old_pairs != new_pairs:
            raise ValueError(
                f"Migration changed relationships: lost={len(old_pairs - new_pairs)}, added={len(new_pairs - old_pairs)}"
            )
        files = []
        downloads = []
        for row in connection.execute(
            "SELECT * FROM files WHERE present=1 ORDER BY identifier,url"
        ):
            if row["identifier"] not in research:
                continue
            metadata = json.loads(row["metadata"])
            if row["local_path"]:
                downloads.append(
                    {
                        "identifier": row["identifier"],
                        "url": row["url"],
                        "path": verified_download(row["local_path"], metadata, root),
                    }
                )
            files.append(
                {
                    "identifier": row["identifier"],
                    "url": row["url"],
                    "metadata": metadata,
                    "status": "discovered",
                }
            )
        boundary = connection.execute(
            "SELECT boundary FROM runs WHERE status='complete' AND mode IN ('full','incremental') ORDER BY id DESC LIMIT 1"
        ).fetchone()
        state = {
            "boundary": boundary[0] if boundary else None,
            "fingerprints": fingerprints,
        }
    collections = {
        "researchdata": research.values(),
        "dataportalen": dataportalen.values(),
        "external": [],
        "links": links,
        "files": files,
    }
    with (
        CatalogLock(destination.parent),
        tempfile.TemporaryDirectory(
            prefix=".migration-", dir=destination.parent
        ) as temporary,
    ):
        candidate = Path(temporary) / "current"
        manifest = write_catalog(candidate, collections, matches, state)
        activate(candidate, destination)
    report = {
        "database": str(database.resolve()),
        "destination": str(destination.resolve()),
        "counts": {
            name: entry["count"] for name, entry in manifest["collections"].items()
        },
        "retained_relationships": len(new_pairs),
        "downloaded_files": downloads,
        "source_representation_hashes": source_representation_hashes,
        "research_identifiers": sorted(research),
        "dataportalen_uris": sorted(dataportalen),
    }
    if report_path:
        report_path.parent.mkdir(parents=True, exist_ok=True)
        write_json(report_path, report)
    if downloads:
        write_json(root / "data" / "downloads" / "manifest.json", downloads)
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("database", type=Path)
    parser.add_argument("--config", type=Path, default=Path("config.toml"))
    parser.add_argument("--destination", type=Path)
    parser.add_argument("--report", type=Path, default=Path(".audit/migration.json"))
    args = parser.parse_args()
    destination = args.destination or Config.load(args.config).current_directory
    report = migrate(args.database, destination, report_path=args.report)
    print(
        json.dumps(
            {
                "counts": report["counts"],
                "verified_downloads": len(report["downloaded_files"]),
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
