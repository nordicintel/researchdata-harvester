-- SPDX-License-Identifier: NOASSERTION
-- Copyright (c) 2026 NordicIntel
PRAGMA foreign_keys = ON;
CREATE TABLE IF NOT EXISTS runs (
 id INTEGER PRIMARY KEY, started TEXT NOT NULL, finished TEXT,
 mode TEXT NOT NULL, status TEXT NOT NULL, boundary TEXT NOT NULL,
 stats JSON NOT NULL DEFAULT '{}', error TEXT
);
CREATE TABLE IF NOT EXISTS research_records (
 identifier TEXT PRIMARY KEY, dataset_id TEXT NOT NULL, version TEXT,
 datestamp TEXT, deleted INTEGER NOT NULL DEFAULT 0,
 identities JSON NOT NULL, first_seen TEXT NOT NULL, last_seen TEXT NOT NULL,
 provenance JSON NOT NULL, current_catalog INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS representations (
 identifier TEXT NOT NULL REFERENCES research_records(identifier),
 format TEXT NOT NULL, raw_xml TEXT NOT NULL, metadata JSON NOT NULL,
 seen_run INTEGER REFERENCES runs(id), present INTEGER NOT NULL DEFAULT 1,
 fetched_at TEXT NOT NULL, source_url TEXT NOT NULL, content_hash TEXT NOT NULL, catalogue INTEGER NOT NULL DEFAULT 0,
 PRIMARY KEY(identifier, format)
);
CREATE TABLE IF NOT EXISTS dataportalen_records (
 uri TEXT PRIMARY KEY, identities JSON NOT NULL, triples JSON NOT NULL,
 metadata_complete INTEGER NOT NULL DEFAULT 0,
 present INTEGER NOT NULL DEFAULT 1, seen_run INTEGER REFERENCES runs(id),
 fetched_at TEXT NOT NULL, provenance JSON NOT NULL,
 in_scope INTEGER NOT NULL DEFAULT 1
);
CREATE TABLE IF NOT EXISTS links (
 identifier TEXT NOT NULL REFERENCES research_records(identifier),
 uri TEXT NOT NULL REFERENCES dataportalen_records(uri),
 rule TEXT NOT NULL, relation TEXT NOT NULL, status TEXT NOT NULL,
 evidence JSON NOT NULL, seen_run INTEGER REFERENCES runs(id),
 PRIMARY KEY(identifier, uri, rule)
);
CREATE TABLE IF NOT EXISTS files (
 identifier TEXT NOT NULL REFERENCES research_records(identifier), url TEXT NOT NULL,
 metadata JSON NOT NULL, status TEXT NOT NULL DEFAULT 'discovered',
 local_path TEXT, error TEXT, fetched_at TEXT, present INTEGER NOT NULL DEFAULT 1,
 PRIMARY KEY(identifier, url)
);
CREATE TABLE IF NOT EXISTS checkpoints (
 source TEXT NOT NULL, run_id INTEGER NOT NULL REFERENCES runs(id),
 from_date TEXT, until_date TEXT, token TEXT, completed INTEGER NOT NULL DEFAULT 0,
 details JSON NOT NULL DEFAULT '{}', PRIMARY KEY(source, run_id)
);
CREATE TABLE IF NOT EXISTS sitemap_entries (
 url TEXT PRIMARY KEY, kind TEXT NOT NULL, seen_run INTEGER REFERENCES runs(id),
 present INTEGER NOT NULL DEFAULT 1, provenance JSON NOT NULL
);
CREATE INDEX IF NOT EXISTS representations_run ON representations(format, seen_run);
CREATE INDEX IF NOT EXISTS dataportalen_run ON dataportalen_records(seen_run);

CREATE TABLE IF NOT EXISTS source_pages (
 run_id INTEGER NOT NULL REFERENCES runs(id), source TEXT NOT NULL,
 content_hash TEXT NOT NULL, raw_xml TEXT NOT NULL, fetched_at TEXT NOT NULL,
 PRIMARY KEY(run_id, source, content_hash)
);

CREATE TABLE IF NOT EXISTS current_inventory (
 identifier TEXT PRIMARY KEY, header JSON NOT NULL,
 seen_run INTEGER NOT NULL REFERENCES runs(id)
);
