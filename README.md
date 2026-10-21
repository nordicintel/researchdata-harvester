# Researchdata harvester

Harvest the current [Researchdata catalog](https://researchdata.se/), its corresponding [Dataportalen](https://www.dataportal.se/) records, and accessible metadata from linked repositories. Publish JSON-compatible metadata and file descriptions. The published current files also supply the next run's state.

## Set up and run

Install Git, Python 3.12 or later, and [uv](https://docs.astral.sh/uv/). If Python is already installed, uv can be installed with:

```sh
python -m pip install uv
```

Clone and prepare the repository:

```sh
git clone https://github.com/nordicintel/researchdata-harvester.git
cd researchdata-harvester
uv sync --locked
uv run python -m scripts.sync
```

A fresh clone already contains current data. The default run reuses unchanged Researchdata records, retrieves changes with a one-day overlap, and checks complete catalog membership for additions and removals. Dataportalen metadata is refreshed independently in scoped batches. External resources use conditional requests where supported; otherwise they must be fetched to detect changes.

To refresh all current Researchdata representations:

```sh
uv run python -m scripts.sync --full
```

Only current catalog records are retained. Exact source identifiers can include revision suffixes; this does not trigger historical retrieval. Restricted datasets remain in the catalog. Normal harvesting discovers file metadata but does not download research files, establish download rights, or grant redistribution permission.

### Command options

| Option | Behavior |
| --- | --- |
| `--full` | Refresh all current Researchdata representations instead of using the previous successful boundary. |
| `--config PATH` | Read TOML settings, defaulting to `config.toml`. Relative configured output paths resolve against this file's directory. |
| `--destination PATH` | Override the output directory. Relative CLI paths resolve against the working directory. |
| `--report PATH` | Write the successful run's JSON report to this path, relative to the working directory. The report is also printed to standard output. |

An absent destination starts a fresh harvest and does not borrow state from `data/current/`. To test an incremental run without replacing tracked data, first copy the current directory to an ignored destination:

```sh
uv run python -c "import shutil; shutil.copytree('data/current', 'data/development/current')"
uv run python -m scripts.sync --destination data/development/current --report data/development/report.json
```

Run the copy command only when the development destination is absent. Subsequent runs reuse that destination. Output directories should have separate parent directories because locking and temporary replacement paths are shared within each parent.

## Configure retrieval

The checked-in `config.toml` contains operational defaults. For local overrides, copy it to ignored `config.local.toml`, edit the copy, and use `--config config.local.toml`. Unknown settings are rejected.

| Setting | Default | Allowed values and purpose |
| --- | --- | --- |
| `current_directory` | `data/current` | Filesystem path. Change for a separate output location. Relative paths resolve against the configuration file. |
| `oai_url` | `https://api.researchdata.se/oai-pmh` | Endpoint string. Change only for a verified replacement or test service. |
| `sitemap_url` | `https://researchdata.se/catalogue/sitemap.xml` | Endpoint string for current membership reconciliation. |
| `sparql_url` | `https://admin.dataportal.se/sparql` | Endpoint string for scoped Dataportalen descriptions. |
| `catalog_metadata_url` | `https://admin.dataportal.se/store/818/metadata/9198` | Endpoint string for explicit Researchdata catalog membership. |
| `catalog_uri` | `https://researchdata.se/catalogue/` | Catalog identity string used to establish scope. Verify upstream identity before changing. |
| `timeout` | `90` | Positive seconds per HTTP timeout phase. Connection timeout is capped at 15 seconds. Increase for persistently slow sources. |
| `concurrency` | `4` | Integer from 1 to 32. Maximum concurrent HTTP requests. Reduce when sources throttle requests. |
| `retries` | `5` | Integer from 0 to 12. Retries after the initial request for transient failures. Server `Retry-After` instructions are honored. |
| `identity_batch_size` | `100` | Integer from 1 to 500. Dataportalen batch size. Reduce if the service rejects large queries. |

URL settings are strings, not a guarantee that a supplied endpoint exists. Obtain replacement addresses and identities from upstream metadata or documentation.

## Read the published data

`data/current/manifest.json` lists the gzip JSONL shards, record counts, and SHA-256 checksums. Discover filenames through this manifest. Each decompressed line is one JSON object. Deterministic compression and identity-based partitions keep unrelated changes local; oversized partitions split to keep individual published files below 100 MB.

| Collection | Identity and relationships | Content |
| --- | --- | --- |
| `researchdata` | `identifier` is the exact current OAI ID. | Dataset ID, datestamp, identities, summary, and DataCite, DCAT, and JSON-LD representations. |
| `dataportalen` | `uri` is the exact dataset URI. | Identity evidence and the complete retrieved metadata graph. |
| `external` | `url` identifies a resource; `identifiers` lists its current Researchdata owners. | Retrieved metadata, access outcome, HTTP validators, and stale-metadata information. |
| `links` | `identifier` joins Researchdata; `uri` joins Dataportalen. Identity also includes `rule`. | Relationship, matching rule, status, and source evidence. |
| `files` | The pair `identifier`, `url` identifies a file association. | Advertised file attributes and provenance; availability is not proof of download access. |

Multiple links can be legitimate. `matched` and `ambiguous` statuses must remain distinguishable; ambiguous evidence is not a confirmed identity. External payloads retained after a failure have `metadata_stale`; `metadata_identifiers` records the owners of that retained payload. Files derived only from stale metadata are unverified.

Researchdata XML inputs become JSON trees that preserve namespaces, attributes, ordered children, repeated values, and mixed text. Embedded JSON-LD also has a parsed `json` value. Dataportalen metadata uses `root` and `nodes`; predicates contain lists of typed terms. Blank-node references resolve within the record using `_:` node keys. Literals retain source spelling, language, and datatype. Source values and conflicting representations remain separate.

The following standard-library example streams one collection without loading the complete catalog:

```python
import gzip
import json
from pathlib import Path

current = Path('data/current')
manifest = json.loads((current / 'manifest.json').read_text(encoding='utf-8'))
for shard in manifest['collections']['researchdata']['files']:
    with gzip.open(current / shard['name'], 'rt', encoding='utf-8') as stream:
        for line in stream:
            record = json.loads(line)
            print(record['identifier'])
```

`matches.json` stores confirmed indirect mappings. Direct matches and ambiguities are represented in `links`. `state.json` stores the last successful boundary and matching fingerprints. HTTP validators reside with external records. Keep the manifest, collections, mappings, and state together; do not hand-edit individual generated files.

## Sources and failure handling

The [OAI-PMH service](https://api.researchdata.se/oai-pmh) supplies the three Researchdata representations. Complete OAI membership is reconciled with the [sitemap](https://researchdata.se/catalogue/sitemap.xml), excluding collection pages. Dataportalen uses the explicit catalog membership document and its [SPARQL service](https://docs.dataportal.se/dcat/docs/sparql/), without scanning the global catalog.

External enrichment follows dataset identities and explicitly linked metadata resources, excluding citations, previous versions, and unrelated navigation. Zenodo and KTH use repository APIs; ICOS and SITES support structured requests. Other sources use structured landing-page metadata and advertised alternate metadata. Results are shared across current datasets referencing the same resource.

Required catalog failures prevent publication and successful-boundary advancement. External failures become explicit outcomes after retries and do not block publication. Previously retrieved metadata remains available only for the same current owners and is marked stale until recovery.

Interrupted runs restart pagination and reuse the last successful published state. There are no persistent page checkpoints. Candidate validation precedes replacement, and a later invocation automatically recovers an interrupted directory swap.

| Problem | Action |
| --- | --- |
| Required Researchdata or Dataportalen retrieval fails | Inspect the failing endpoint and error, then rerun after recovery. Keep the existing current data and state together. |
| External resource is restricted, unavailable, or unsupported | Inspect the resource outcome. The run can succeed; stale metadata and unverified files remain explicit. |
| Configuration is rejected | Correct the named setting using the table above. Check TOML syntax and path resolution. |
| Catalog checksum or relationship validation fails | Stop publication. Restore the complete current directory from a known valid Git commit, then validate and rerun. Do not regenerate checksums to hide unexplained changes. |
| Another process holds the catalog lock | Wait for that process or stop it deliberately before retrying. The lock filename alone does not indicate a live owner. |
| A run was interrupted | Rerun the same command. Do not manually delete lock or replacement directories as routine recovery. |
| Remote `main` advanced before publication | Update to the newer `main` and rerun against its current state. Never force-push the older candidate. |

The JSON run report includes collection counts, Researchdata reuse and reconciliation, linked and unlinked counts, link statuses, external outcomes, request totals, and elapsed seconds. Counts of linked records include ambiguous links; inspect `link_statuses` and the links collection before treating coverage as confirmed. A report file is written only after successful completion; use the process error or Actions log for failed runs.

## Operate GitHub Actions

The [Harvest workflow](https://github.com/nordicintel/researchdata-harvester/actions/workflows/harvest.yml) runs daily at **03:17 UTC**. To start it manually, open **Actions**, select **Harvest**, then **Run workflow** on `main`. Leave `full` unchecked for incremental operation or select it for a full refresh.

Scheduled and manual harvests are serialized without canceling active runs. Each run checks out current `main`, uses Python 3.12 and locked uv dependencies, and publishes validated current data in one data-only commit. The workflow has a 180-minute timeout. A normal push never overwrites a newer `main` commit.

The Validate workflow checks code changes and all pull requests, including proposed data changes. Routine data-only pushes skip this workflow because the harvester validates the candidate before publication.

### Publish manually

Prefer the Harvest workflow. For manual publication, start on up-to-date `main`, with committed code, a clean index, Git author identity, and permission to push. Record the starting commit before running the harvest. In PowerShell:

```powershell
git pull --ff-only
$harvestHead = git rev-parse HEAD
uv run python -m scripts.sync
if ($LASTEXITCODE -ne 0) { throw 'Harvest failed; do not publish.' }
uv run python -m scripts.publish --expected-head $harvestHead
```

The publisher validates `data/current/`, checks local and remote HEAD against the recorded commit, and commits only generated current data. If `main` advances, preserve any unrelated local work, restore the newer complete current state, and repeat the harvest. Do not force-push or mix an old candidate with new state. Ordinary Git history is retained; the harvester does not maintain historical snapshots.

## Maintain the code

| Directory | Responsibility |
| --- | --- |
| `harvester/` | Source retrieval, metadata conversion, matching, current-state persistence, and coordination. |
| `scripts/` | Executable sync and publication commands. |
| `tests/` | Offline behavior tests, including local Git publication tests. |
| `.github/workflows/` | Validation and scheduled or manual harvesting. |
| `data/current/` | Tracked generated metadata and the state needed for the next run. |

This is a directly executed repository with internal Python modules, not an installed library. Use English identifiers and documentation, type annotations, asynchronous httpx requests, and uv-managed dependencies. Keep source-owned fields and metadata fidelity intact.

Run these checks before committing:

```sh
uv run python -m pytest
uv run ruff check harvester scripts tests
uv run ruff format --check harvester scripts tests
uv run python -m scripts.sync --help
uv run python -m scripts.publish --help
uv run python -c "from pathlib import Path; from harvester.storage import validate_catalog; print(validate_catalog(Path('data/current')))"
```

These checks are offline. Source-facing changes also need a live harvest with coverage, matching outcomes, and failure behavior examined. Use the isolated development destination above for experiments, then verify the normal workflow. Keep code changes and generated data refreshes in separate commits.

To update dependencies, run `uv lock --upgrade`, then `uv sync --locked` and the checks above. For a targeted update, use `uv lock --upgrade-package PACKAGE`. Review and commit the lockfile diff. Changes to network, XML, or RDF dependencies also require live source verification.

Local downloads are outside normal harvesting and remain ignored under `data/downloads/`. Preserve their integrity manifest when cleaning local files. Local reports, configuration overrides, environments, and temporary candidates are not published.
