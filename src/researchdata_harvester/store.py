# SPDX-License-Identifier: NOASSERTION
# Copyright (c) 2026 NordicIntel
"""SQLite metadata persistence, atomic page checkpoints and snapshot exports."""

import hashlib
import json
import os
import shutil
import sqlite3
import tempfile
from datetime import UTC, datetime
from importlib.resources import files
from pathlib import Path
from typing import Any

from researchdata_harvester.metadata import (
    extract_evidence,
    extract_files,
    jsonld_documents,
    metadata_summary,
)
from researchdata_harvester.oai import Record


def now() -> str:
    return datetime.now(UTC).isoformat()


def encode(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


class Store:
    """One explicit writer; transactions include records and their next-page cursor."""

    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(path)
        self.connection.row_factory = sqlite3.Row
        self.connection.executescript(
            files("researchdata_harvester").joinpath("schema.sql").read_text()
        )
        self.connection.execute("PRAGMA busy_timeout = 30000")
        file_columns = {
            row[1] for row in self.connection.execute("PRAGMA table_info(files)")
        }
        if "present" not in file_columns:
            with self.connection:
                self.connection.execute(
                    "ALTER TABLE files ADD COLUMN present INTEGER NOT NULL DEFAULT 1"
                )

        dataportalen_columns = {
            row[1]
            for row in self.connection.execute(
                "PRAGMA table_info(dataportalen_records)"
            )
        }
        if "in_scope" not in dataportalen_columns:
            with self.connection:
                self.connection.execute(
                    "ALTER TABLE dataportalen_records ADD COLUMN in_scope INTEGER NOT NULL DEFAULT 1"
                )

    def close(self) -> None:
        self.connection.close()

    def start_run(self, mode: str, boundary: str) -> int:
        with self.connection:
            return self.connection.execute(
                "INSERT INTO runs(started,mode,status,boundary) VALUES(?,?,'running',?)",
                (now(), mode, boundary),
            ).lastrowid

    def finish_run(
        self, run: int, status: str, stats: dict[str, Any], error: str | None = None
    ) -> None:
        with self.connection:
            self.connection.execute(
                "UPDATE runs SET finished=?,status=?,stats=?,error=? WHERE id=?",
                (now(), status, encode(stats), error, run),
            )

    def last_boundary(self) -> str | None:
        row = self.connection.execute(
            "SELECT boundary FROM runs WHERE status='complete' AND mode IN ('full','incremental') ORDER BY id DESC LIMIT 1"
        ).fetchone()
        return row[0] if row else None

    def checkpoint(
        self,
        source: str,
        run: int,
        *,
        from_date: str | None = None,
        until_date: str | None = None,
        token: str | None = None,
        completed: bool = False,
        details: Any = None,
    ) -> None:
        self.connection.execute(
            "INSERT OR REPLACE INTO checkpoints VALUES(?,?,?,?,?,?,?)",
            (
                source,
                run,
                from_date,
                until_date,
                token,
                completed,
                encode(details or {}),
            ),
        )

    def put_record(
        self,
        record: Record,
        prefix: str,
        run: int,
        source_url: str,
        *,
        catalogue: bool = False,
    ) -> None:
        timestamp = now()
        suffix = record.identifier.removeprefix("oai:researchdata.se:")
        dataset, separator, version = suffix.rpartition("/")
        dataset = dataset if separator else suffix
        evidence = extract_evidence(record.metadata)
        evidence.append(
            {
                "value": record.identifier,
                "normalized": record.identifier,
                "field": "oai/header/identifier",
                "relation": "version",
            }
        )
        self.connection.execute(
            "INSERT INTO research_records(identifier,dataset_id,version,datestamp,deleted,identities,first_seen,last_seen,provenance) VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(identifier) DO UPDATE SET "
            "datestamp=excluded.datestamp,deleted=excluded.deleted,identities=excluded.identities,last_seen=excluded.last_seen,provenance=excluded.provenance",
            (
                record.identifier,
                dataset,
                version or None,
                record.datestamp,
                record.deleted,
                encode(evidence),
                timestamp,
                timestamp,
                encode({"source": source_url, "sets": record.sets}),
            ),
        )
        self.connection.execute(
            "INSERT INTO representations VALUES(?,?,?,?,?,1,?,?,?,?) ON CONFLICT(identifier,format) DO UPDATE SET "
            "raw_xml=excluded.raw_xml,metadata=excluded.metadata,seen_run=CASE WHEN excluded.catalogue=1 THEN excluded.seen_run ELSE seen_run END,present=CASE WHEN excluded.catalogue=1 OR catalogue=0 THEN 1 ELSE present END,fetched_at=excluded.fetched_at,source_url=excluded.source_url,content_hash=excluded.content_hash,catalogue=MAX(catalogue,excluded.catalogue)",
            (
                record.identifier,
                prefix,
                record.raw_xml,
                encode(record.metadata),
                run,
                timestamp,
                source_url,
                hashlib.sha256(record.raw_xml.encode()).hexdigest(),
                catalogue,
            ),
        )
        self.refresh_identities(record.identifier)
        if catalogue:
            self.connection.execute(
                "UPDATE research_records SET current_catalog=1 WHERE identifier=?",
                (record.identifier,),
            )
        if prefix == "json_ld":
            self.connection.execute(
                "UPDATE files SET present=0 WHERE identifier=?", (record.identifier,)
            )
        for entry in extract_files(jsonld_documents(record.metadata)):
            self.put_file(record.identifier, entry)

    def refresh_identities(self, identifier: str | None = None) -> None:
        identifiers = (
            [identifier]
            if identifier
            else [
                row[0]
                for row in self.connection.execute(
                    "SELECT identifier FROM research_records"
                )
            ]
        )
        for current in identifiers:
            evidence = [
                dict(item, representation=row["format"])
                for row in self.connection.execute(
                    "SELECT metadata,format FROM representations WHERE identifier=? AND present=1 ORDER BY format",
                    (current,),
                )
                for item in extract_evidence(json.loads(row["metadata"]))
            ]
            evidence.append(
                {
                    "value": current,
                    "normalized": current,
                    "field": "oai/header/identifier",
                    "relation": "version",
                }
            )
            self.connection.execute(
                "UPDATE research_records SET identities=? WHERE identifier=?",
                (encode(evidence), current),
            )

    def put_file(self, identifier: str, metadata: dict[str, Any]) -> None:
        self.connection.execute(
            "INSERT INTO files(identifier,url,metadata) VALUES(?,?,?) ON CONFLICT(identifier,url) DO UPDATE SET status=CASE WHEN metadata != excluded.metadata THEN 'discovered' ELSE status END,metadata=excluded.metadata,present=1",
            (identifier, metadata["url"], encode(metadata)),
        )

    def record(self, identifier: str) -> dict[str, Any] | None:
        row = self.connection.execute(
            "SELECT * FROM research_records WHERE identifier=?", (identifier,)
        ).fetchone()
        if row is None:
            return None
        result = dict(row)
        result["identities"] = json.loads(result["identities"])
        result["provenance"] = json.loads(result["provenance"])
        result["representations"] = [
            dict(item)
            for item in self.connection.execute(
                "SELECT * FROM representations WHERE identifier=? ORDER BY format",
                (identifier,),
            )
        ]
        for item in result["representations"]:
            item["metadata"] = json.loads(item["metadata"])
        result["summary"] = metadata_summary(
            [item["metadata"] for item in result["representations"] if item["present"]]
        )
        return result

    def search(self, text: str, limit: int = 100) -> list[dict[str, Any]]:
        escaped = text.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
        results = [
            dict(row)
            for row in self.connection.execute(
                "SELECT DISTINCT r.identifier,r.dataset_id,r.version,r.datestamp FROM research_records r "
                "JOIN representations p USING(identifier) WHERE p.metadata LIKE ? ESCAPE '\\' "
                "ORDER BY r.identifier LIMIT ?",
                ("%" + escaped + "%", limit),
            )
        ]
        for item in results:
            summaries = self.record(item["identifier"])["summary"]
            item.update(summaries)
        return results

    def file_entries(
        self, identifier: str, *, include_absent: bool = False
    ) -> list[dict[str, Any]]:
        rows = [
            dict(row)
            for row in self.connection.execute(
                "SELECT * FROM files WHERE identifier=? "
                + ("" if include_absent else "AND present=1 ")
                + "ORDER BY url",
                (identifier,),
            )
        ]
        for row in rows:
            row["metadata"] = json.loads(row["metadata"])
        return rows

    def status(self) -> dict[str, Any]:
        tables = (
            "research_records",
            "representations",
            "dataportalen_records",
            "links",
            "files",
        )
        result = {
            table: self.connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[
                0
            ]
            for table in tables
        }
        result["dataportalen_in_scope"] = self.connection.execute(
            "SELECT COUNT(*) FROM dataportalen_records WHERE in_scope=1"
        ).fetchone()[0]
        result["researchdata_matching"] = self.matching_summary()
        result["runs"] = [
            dict(row)
            for row in self.connection.execute(
                "SELECT * FROM runs ORDER BY id DESC LIMIT 10"
            )
        ]
        result["checkpoints"] = [
            dict(row)
            for row in self.connection.execute(
                "SELECT * FROM checkpoints WHERE run_id=(SELECT MAX(id) FROM runs) ORDER BY source"
            )
        ]
        return result

    def matching_summary(self) -> dict[str, int]:
        """Count current records once, giving conflicting evidence precedence."""
        result = {"matched": 0, "unmatched": 0, "ambiguous": 0}
        rows = self.connection.execute(
            "SELECT r.identifier,MAX(l.status='ambiguous') AS ambiguous,COUNT(l.uri) AS links "
            "FROM research_records r LEFT JOIN links l ON l.identifier=r.identifier "
            "AND EXISTS(SELECT 1 FROM dataportalen_records d WHERE d.uri=l.uri AND d.present=1 AND d.in_scope=1) "
            "WHERE r.current_catalog=1 AND r.deleted=0 GROUP BY r.identifier"
        )
        for row in rows:
            outcome = (
                "ambiguous"
                if row["ambiguous"]
                else "matched"
                if row["links"]
                else "unmatched"
            )
            result[outcome] += 1
        return result

    def export(self, destination: Path) -> Path:
        """Publish a complete immutable generation and atomically update CURRENT.

        A reader opens CURRENT once, then reads only that generation. Existing generations
        remain available while consumers read them.
        """
        destination.mkdir(parents=True, exist_ok=True)
        generation = "snapshot-" + datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")
        temporary = Path(tempfile.mkdtemp(prefix=".snapshot-", dir=destination))
        counts = {}
        json_columns = {
            "identities",
            "provenance",
            "metadata",
            "triples",
            "evidence",
            "stats",
            "details",
        }
        tables = {
            "researchdata": ("research_records", "identifier"),
            "representations": ("representations", "identifier,format"),
            "dataportalen": ("dataportalen_records", "uri"),
            "links": ("links", "identifier,uri,rule"),
            "files": ("files", "identifier,url"),
            "sitemap": ("sitemap_entries", "url"),
        }
        try:
            self.connection.execute("BEGIN")
            for filename, (table, order) in tables.items():
                count = 0
                with (temporary / (filename + ".jsonl")).open(
                    "w", encoding="utf-8", newline="\n"
                ) as output:
                    for row in self.connection.execute(
                        f"SELECT * FROM {table} ORDER BY {order}"
                    ):
                        item = dict(row)
                        for key in item.keys() & json_columns:
                            item[key] = json.loads(item[key])
                        output.write(encode(item) + "\n")
                        count += 1
                counts[filename] = count
            manifest = {
                "created": now(),
                "counts": counts,
                "runs": [
                    dict(row)
                    for row in self.connection.execute("SELECT * FROM runs ORDER BY id")
                ],
                "checkpoints": [
                    dict(row)
                    for row in self.connection.execute(
                        "SELECT * FROM checkpoints ORDER BY run_id,source"
                    )
                ],
            }
            (temporary / "manifest.json").write_text(
                encode(manifest) + "\n", encoding="utf-8"
            )
            self.connection.commit()
            final = destination / generation
            temporary.rename(final)
            pointer = destination / (".CURRENT-" + generation)
            pointer.write_text(generation + "\n", encoding="utf-8")
            os.replace(pointer, destination / "CURRENT")
            return final
        except BaseException:
            self.connection.rollback()
            shutil.rmtree(temporary, ignore_errors=True)
            raise
