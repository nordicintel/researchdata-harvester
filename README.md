# Researchdata harvester

Harvest current entries from Dataportalen's Researchdata catalog, retrieve their metadata and publish datasets with verified tabular observations. Output is two typed Parquet tables, a current inventory, a semantic cache and a checksummed manifest. Research observations are inspected temporarily, never published.

## Setup

Install [uv](https://docs.astral.sh/uv/getting-started/installation/), then run:

```sh
git clone https://github.com/nordicintel/researchdata-harvester.git
cd researchdata-harvester
uv sync --locked
uv run python -m scripts.split_data_files --install
```

Python 3.12 is selected by `.python-version`. There is no package installation, server or installed CLI. VS Code defaults to `.venv`; use **Python: Select Interpreter** if another environment was previously selected.

| Directory | Role |
|---|---|
| `harvester/` | Retrieval, semantic resolution, verification and output construction. |
| `scripts/` | Sync, publication and oversized-file hook commands. |
| `tests/` | Essential offline behavior tests. |
| `.github/workflows/` | Coordinated harvesting and validation. |
| `data/current/` | Published data and next-run state. |
| `data/work/` | Ignored temporary artifacts and reports. |
| `data/downloads/` | Preserved local downloads, ignored by Git. |

## Harvest and eligibility

```sh
uv run python -m scripts.sync
uv run python -m scripts.sync --full
```

The default is incremental. Existing current files supply state. An empty destination starts fresh and does not borrow another directory's state. Local sync replaces its destination after validation but does not commit or push.

Inventory uses Dataportalen context `818`, excludes `NON_PUBLIC` and confirms membership against the catalog document. Registry modification values are separate from descriptive dataset dates. New entries, changed registry values or identity evidence, and transient failures select work. `--full` refreshes all candidates. Exact revision suffixes are retained, without historical-version retrieval, snapshots, full OAI enumeration or sitemap scans.

Selected records independently revalidate linked mutable metadata. Unchanged datasets do not trigger external crawling. Changes that never reach Dataportalen are discovered on another selection trigger or an explicit full run.

A new dataset must yield columns and an observation row. Public metadata, HTTP 200 and file extensions are insufficient. Missing distributions are allowed when explicitly linked repositories provide data. Unspecified rights and anonymous licence acknowledgements are allowed. Accounts, API keys, payment and individual approval are excluded. Licence terms are retained without a commercial-use filter.

Verification supports CSV/TSV, flat JSON observations, XLSX, XLS, supported statistical files and Parquet, including gzip wrappers. Generic archives, documents, imagery, map layers and APIs with unknown selectors remain unresolved. At most 100 rows and 32 MiB of transferred/decompressed content are inspected per resource. Verification stops after the first successful resource establishes dataset eligibility; every remaining advertised resource stays in metadata with an explicit unverified outcome.

Required inventory failures prevent publication. Expected failures retrieving or parsing an individual dataset become saved stage outcomes, so both fetch workflows continue with other datasets. External failures receive explicit outcomes; previous metadata can remain stale. Previously verified datasets retain evidence after failed refreshes with unavailable status. New unresolved/unavailable records remain in inventory only. Confirmed removal or exclusion removes both table rows. Confirmed Researchdata associations stay in the inventory and are reused only while their supporting metadata fingerprint still matches. Verification does not establish redistribution rights.

The builder retains an existing pair after a required record refresh fails, preserving its verification and marking metadata stale/unavailable. New failed datasets stay in inventory only. Inventory stores stage-specific retry decisions, the newly observed registry modification value and the fingerprint of each published stage. Confirmed removal or exclusion still removes both rows. Missing results, corrupt artifacts, incomplete inventory, unexpected programming errors and invalid output fail the run. Cancellation propagates and leaves completed artifacts reusable.

Manifest `complete` means inventory coverage is complete and every selected entry has a documented result. It does not mean every refresh succeeded. The report and inventory expose failed and stale records. Inventory-only changes can be published.

## Scoped execution and recovery

```sh
uv run python -m scripts.sync --limit 10 --destination data/work/sample-current --work-directory data/work/sample
uv run python -m scripts.sync --dataset-uri "https://doi.org/10.5878/d4bn-b702" --destination data/work/selected-current --work-directory data/work/selected --log-level DEBUG
```

`--dataset-uri` is repeatable and cannot combine with `--limit`. Scoped runs require an ignored destination, cannot infer catalog-wide removals and cannot publish. Limited runs stop at their sample.

Use `--config config.local.toml` for an ignored complete alternative configuration. Configured paths resolve relative to the TOML file; CLI paths resolve relative to the working directory. `--report data/work/report.json` selects the report path.

Reuse a work directory after interruption. Failed transient records retry on the next run, using the existing bounded HTTP policy of three attempts and a total operation deadline. Stable failures retry after source change or `--full`. Successful counterpart artifacts are reused when source, configuration and implementation fingerprints match. If artifacts have expired, a matching published stage row can be reused; otherwise the metadata needed for a complete replacement pair is retrieved. Completed record artifacts are reusable only when inputs, configuration and implementation fingerprints agree. Each artifact saves its new cache entries; the worker replays these in completion order after interruption and compresses the complete cache at stage boundaries. Pagination restarts. Use another work directory after a completed candidate build.

```sh
uv run python -m scripts.sync --stage inventory --work-directory data/work/run
uv run python -m scripts.sync --stage dataportalen --work-directory data/work/run
uv run python -m scripts.sync --stage researchdata --work-directory data/work/run
uv run python -m scripts.sync --stage build --work-directory data/work/run
```

Fetch stages write artifacts; build writes a candidate without replacing production. Interrupted directory replacement recovers automatically. A lock filename alone does not establish a running process. Do not delete lock/replacement directories as routine recovery.

INFO logs show progress. DEBUG adds redacted requests, attempts, cache decisions and parser outcomes. Reports contain per-stage success/failure and retry counts, redacted failure details, retained-stale and newly deferred counts, requests, timing and output sizes. Each failed record identifies its dataset, stage, operation, URL, category, reason and retryability. Credentials, raw response bodies and observation values are not logged.

## Output and semantic definitions

Consumers discover filenames through `manifest.json`. The default is exactly two Zstandard Parquet files. A table splits only when its compressed size reaches 100,000,000 bytes. Parts use consecutive identity ranges and `_part_1` suffixes; the builder uses the fewest successful equal-row parts.

| File | Content |
|---|---|
| `datasets.parquet` | Enriched metadata, associations, resources and verification evidence. |
| `dataportalen.parquet` | Registry identity and complete Dataportalen metadata and related entities. |
| `inventory.json` | Membership, modification signals, outcomes, retry decisions and associations. |
| `rdf-cache.json.gz` | Vocabulary/schema definitions and mutable metadata with validators/stale outcomes. |
| `manifest.json` | Schema version, coverage, filenames, counts, sizes and SHA-256 checksums. |

Join tables on `dataset_uri`; identities are identical and unique. Multilingual values and repeated agents, distributions and services use nested structures. `properties` preserves assertions with value type, language, datatype, source URL, entity, graph, predicate and path. Conflicting assertions stay separate. JSON paths preserve array positions and empty containers.

```python
import json
from pathlib import Path
import duckdb

root = Path("data/current")
manifest = json.loads((root / "manifest.json").read_text(encoding="utf-8"))
files = [str(root / name) for name, info in manifest["files"].items() if info.get("table") == "datasets"]
with duckdb.connect() as connection:
    print(
        connection.execute(
            "SELECT dataset_uri, titles[1].value AS title, availability FROM read_parquet(?) LIMIT 10",
            [files],
        ).fetchall()
    )
```

Keep tables, inventory, cache and manifest together. Independent edits break consistency.

`harvester/vocabulary.py` resolves RDF/JSON, JSON-LD, JSON Schema and schemas in OpenAPI 3.x JSON. It preserves unknown attributes and deterministic anonymous identities. Static schema references support pointers, anchors, relative identifiers and cycles. Dynamic/recursive evaluation is explicitly unsupported. It does not validate observations or execute API operations.

```python
from pathlib import Path
from harvester.runtime import Config, Http
from harvester.vocabulary import SemanticResolver


async def resolve(document, source_url):
    async with Http(Config()) as client:
        async with SemanticResolver(
            Path("data/current/rdf-cache.json.gz"),
            client,
            Path("data/work/semantic-updates.json.gz"),
        ) as resolver:
            return await resolver.resolve(document, source_url=source_url)
```

Use `format="json-schema"` for schemas without `$schema`, including boolean schemas. Missing caches start empty. Vocabulary definitions are reused; requested mutable entities are revalidated. Workers leave published cache files untouched. Build merges updates, rejects conflicts and prunes unreferenced mutable entries. The existing RDF filename also covers non-RDF definitions.

## Configuration

| Setting | Default | Allowed values and purpose |
|---|---|---|
| `current_directory` | `data/current` | Filesystem path for independent output/state. |
| `search_url` | `https://admin.dataportal.se/store/search` | Registry endpoint URL. |
| `context_url` | `https://admin.dataportal.se/store/818` | Exact catalog context URL. |
| `catalog_metadata_url` | `https://admin.dataportal.se/store/818/metadata/9198` | Catalog membership metadata URL. |
| `catalog_uri` | `https://researchdata.se/catalogue/` | Exact RDF catalog identity. |
| `oai_url` | `https://api.researchdata.se/oai-pmh` | Targeted current-record metadata endpoint. |
| `concurrency` | `4` | Integer 1-32, simultaneous requests per process. |
| `connect_timeout` | `10` | Positive seconds for connection establishment. |
| `read_timeout` | `30` | Positive seconds of response inactivity. |
| `attempts` | `3` | Integer 1-10 including the first attempt. |
| `operation_deadline` | `90` | Positive seconds including retries/backoff. |
| `verification_rows` | `100` | Integer 1-100, maximum inspected observations. |
| `verification_bytes` | `33554432` | Positive bytes, default 32 MiB. |
| `page_size` | `100` | Integer 1-100, registry page size. |

Change endpoints only for an intentional source migration or test. Lower concurrency for constrained sources. Adjust limits when diagnosing explicit outcomes. There is no implicit configuration overlay.

## GitHub operation

Fetch workflows restore completed same-run artifacts on a rerun and replace their uploaded artifacts. Artifacts expire after one day; published inventory retains retry decisions. Expected record failures do not fail a job. Run-wide failures still prevent publication, and diagnostic artifacts remain available.

Harvest runs daily at **03:17 UTC**. Use **Actions > Harvest > Run workflow** for a manual run and its full-refresh option.

Inventory feeds two reusable fetch workflows in parallel. Publication waits for both, validates the candidate and publishes only generated current data directly to `main`. Artifacts expire after one day. Runs are serialized without cancelling active work. Fetch jobs are read-only; only publication receives write permission.

```sh
uv run python -m scripts.publish --candidate data/work/run/candidate --expected-head "<base-main-commit>"
```

Publication requires a complete candidate, `main` at the expected base commit, a clean index and an unchanged remote head. If `main` advances, update and rerun. Never force-push or publish an old candidate over newer data.

## Validation and maintenance

These commands are offline:

```sh
uv run ruff check .
uv run ruff format --check .
uv run mypy
uv run pyright
uv run python -m pytest
uv run python -m scripts.sync --stage validate --destination data/current
```

Catalog validation requires a published generation. Default pytest excludes splitter tests. After changing the splitter, also run:

```sh
uv run python -m pytest tests/test_split_data_files.py
```

Manage dependencies with uv. Use `uv lock --upgrade`, then `uv sync --locked`, offline checks and scoped live checks for source-facing changes. Commit the dependency manifest and lockfile together.

Use English names, annotations and useful Google-style docstrings. Ruff uses 120-character lines, double quotes and spaces. Mypy is strict and Pyright also checks active code. PyLD's missing `py.typed` marker is narrowly exempted for mypy.

| Problem | Action |
|---|---|
| Inventory disagreement | Publication stops. Correct the source disagreement and rerun. |
| Individual dataset failure | Other datasets continue. Inspect its saved stage outcome; transient failures retry automatically. Existing rows remain stale/unavailable and new failed records remain inventory-only. |
| Corrupt artifacts or unexpected programming error | The job fails. Inspect the traceback and artifacts; do not hide it as an ordinary source outcome. |
| External source failure | Inspect its outcome; prior metadata may remain stale and transient failures retry. |
| Invalid configuration | Correct the named setting before network work. |
| Checksum failure | Restore a consistent generation; do not rewrite checksums to conceal corruption. |
| Concurrent local run | Let the lock holder finish or deliberately stop that process. |
| Publication race | Update from `main` and rerun. |
| Existing completed candidate | Inspect it, then use another work directory for a new build. |

## Oversized-file hook

The hook supports `.parquet`, `.jsonl`, `.parquet.gz` and `.jsonl.gz`. Files of 100 MB or larger split into `_part_1` filenames. Its 50 MB target estimates initial part count; the final check only requires each part below 100 MB. Configure `[tool.split-data-files]` in `pyproject.toml`; MB is decimal.

JSONL line bytes remain intact. DuckDB writes Parquet and gzip headers are deterministic. The hook rejects conflicting filenames and differences between staged and working copies. It stages only selected replacements and does not update manifests. The catalog builder therefore completes partitioning/checksums before staging, so publication triggers no further splitting.

## External documentation

- [Dataportalen registry API](https://docs.dataportal.se/registry/api/)
- [EntryStore search](https://entrystore.org/kb/search/)
- [Researchdata API](https://docs.researchdata.se/api/)
- [Researchdata JSON-LD](https://docs.researchdata.se/metadata/schema-org-json-ld/)
- [DuckDB Parquet](https://duckdb.org/docs/stable/data/parquet/overview)
