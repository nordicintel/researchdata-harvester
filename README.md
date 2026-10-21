# Researchdata harvester

Harvest the current [Researchdata catalog](https://researchdata.se/) and its corresponding [Dataportalen](https://www.dataportal.se/) records. The current JSON files are both the published result and the state used by the next run.

## Run a harvest

Install Python 3.12 or later and [uv](https://docs.astral.sh/uv/), then run from this repository:

```sh
uv sync --locked
uv run python -m scripts.sync
```

The first run retrieves the current catalog. Subsequent runs retrieve changed Researchdata metadata, check both catalogs for additions and removals, and independently revalidate linked metadata. To refresh all current Researchdata representations:

```sh
uv run python -m scripts.sync --full
```

`--config` selects a TOML configuration file. Relative data paths resolve against that file. `--destination` overrides the current output directory. `--report` writes an optional local JSON acceptance report, separate from the published metadata.

## Read the output

`data/current/manifest.json` lists every compressed JSONL shard, its record count and checksum. Each line in a decompressed shard is a JSON object. Shards use stable identity hashes so unrelated additions do not move records between files.

| Collection | Identity and content |
| --- | --- |
| `researchdata` | Exact current OAI identifier, dataset base, datestamp, source identities, summary and three metadata representations. |
| `dataportalen` | Dataset URI, identity evidence and complete retrieved metadata graph. |
| `external` | Resource URL, owning Researchdata identifiers, complete retrieved metadata, access outcome and HTTP validators. |
| `links` | Researchdata identifier, Dataportalen URI, matching rule, status and supporting evidence. |
| `files` | Researchdata identifier, advertised file URL and available file attributes. |

`matches.json` contains confirmed indirect mappings only. Direct identifier matches and ambiguous evidence remain in the links collection. `state.json` contains the successful sync boundary and matching fingerprints. There is no runtime database or historical catalog.

Researchdata metadata preserves namespace names, attributes, ordered children, repeated values and mixed text in JSON trees. Embedded JSON-LD also has a parsed `json` value. Dataportalen graphs use `root` and `nodes`; predicates map to lists of typed terms. Blank-node references have `type: bnode` and resolve to a node key prefixed with `_:` within that record. Literal values retain their source spelling, language and datatype, including malformed source literals.

The output contains current records only. Source identifiers can contain revision suffixes, but the harvester does not fetch previous revisions. One Dataportalen entry can legitimately correspond to several Researchdata records.

## Retrieval and failure behavior

The [Researchdata OAI-PMH service](https://api.researchdata.se/oai-pmh) supplies DataCite, DCAT and JSON-LD. A complete identifier inventory and the [sitemap](https://researchdata.se/catalogue/sitemap.xml) control membership. Collection pages do not count as datasets. Missing representations are retrieved individually; unavailable formats and inventory discrepancies are explicit outcomes.

Dataportalen enumeration uses the [Researchdata catalog membership document](https://admin.dataportal.se/store/818/metadata/9198), then retrieves scoped descriptions through its [SPARQL service](https://docs.dataportal.se/dcat/docs/sparql/). It does not scan the global catalog. Because this service has no established reliable record-level change signal, each run refreshes the scoped metadata in batches.

External enrichment runs automatically for dataset-owned resources. Zenodo and KTH use their record APIs; ICOS and SITES support structured metadata requests. Other linked dataset pages use structured JSON, JSON-LD, metadata tags and explicitly advertised alternate metadata. HTTP validators avoid unnecessary transfers where supported. A source without validators must be fetched again to establish whether it changed. Citations, prior versions and unrelated repository pages are excluded.

The harvester keeps restricted datasets and records unavailable metadata explicitly. File discovery does not establish download rights, tabular usability or redistribution permission. Normal harvesting does not download underlying research files.

A failed inventory, malformed required response or exhausted transient retry leaves the published current directory and successful boundary unchanged. Continuation tokens live only in memory. Local writers use an OS lock; directory replacement retains the previous directory only while the swap completes. A later invocation recovers an interrupted swap.

## GitHub operation

The public repository is [nordicintel/researchdata-harvester](https://github.com/nordicintel/researchdata-harvester). The Harvest workflow runs daily at 03:17 UTC and can be started manually. It restores current files from `main`, runs the same sync command and publishes one data-only commit. It does not force-push or overwrite a newer `main` commit. Scheduled and manual harvests are serialized.

Published files remain below 100 MB. Compression is deterministic; oversized partitions split by additional identity-hash digits. The harvester retains one current output set. Ordinary Git history remains ordinary repository history.

## Validate changes

```sh
uv run python -m pytest
uv run ruff check harvester scripts tests/current
uv run ruff format --check harvester scripts tests/current
```

Default tests are offline. Live acceptance is a separate full harvest and subsequent incremental run, with counts, relationships and unchanged-output behavior checked against the migrated catalog. The GitHub Validate workflow runs the offline tests and validates the published files.

### Verification status on 21 October 2026

The current files are the verified SQLite migration: 6,691 Researchdata records, 6,626 Dataportalen records, 6,692 relationships and 23,436 file records. All Researchdata representation hashes and current relationship identities were retained, and eight existing downloads passed integrity checks. The 77 offline tests pass, including publication against a real local Git remote.

Full live retrieval completed the Researchdata and Dataportalen inventories, but external enrichment exhausted retries on the [SNSN Kiruna dataset landing page](https://www.snsn.se/data/Kir23.php), which returned HTTP 500. The approved strict failure policy prevented replacement of current data or advancement of its boundary. A successful full harvest, incremental rerun and GitHub workflow execution remain pending; the old database and exports have therefore not been deleted.

## Migrate the former SQLite catalog

```sh
uv run python -m scripts.migrate data/catalog.sqlite --report .audit/migration.json
```

Migration requires an absent destination. It opens SQLite read-only, retains current source metadata and relationships, and verifies existing downloads at their moved paths. The local download manifest stays under `data/downloads/`. Keep the old database until migration comparisons and live acceptance succeed.

The unchanged `src/researchdata_harvester` directory is reference material. New execution does not import that package. Tests cover the current runner; obsolete package tests have been removed. `pyproject.toml` manages dependencies without building or installing a library. [PROJECT_SCOPE.md](PROJECT_SCOPE.md) records the agreed scope.
