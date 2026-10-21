# Simplify the Researchdata harvester with DuckDB

## 1. Target structure

Keep six focused implementation modules, two executable scripts, four test modules and two workflows. Use DuckDB for schema enforcement, SQL updates, validation and Parquet output. Published Parquet files, the compact inventory and one gzipped JSON RDF cache remain the persisted state; no permanent DuckDB database is required.

```text
researchdata-harvester/
├── README.md
├── pyproject.toml
├── uv.lock
├── config.toml
├── .gitignore
├── .gitattributes
│
├── harvester/
│   ├── runtime.py
│   ├── dataportalen.py
│   ├── researchdata.py
│   ├── external.py
│   ├── verify.py
│   ├── output.py
│   ├── schema.sql
│   └── vocabulary.json
│
├── scripts/
│   ├── sync.py
│   └── publish.py
│
├── tests/
│   ├── test_inventory.py
│   ├── test_metadata.py
│   ├── test_verification.py
│   └── test_publication.py
│
├── .github/
│   └── workflows/
│       ├── harvest.yml
│       └── validate.yml
│
└── data/
    ├── current/
    │   ├── datasets/
    │   │   └── part-*.parquet
    │   ├── dataportalen/
    │   │   └── part-*.parquet
    │   ├── inventory.json
    │   ├── rdf-cache.json.gz
    │   └── manifest.json
    ├── work/                         # Ignored, temporary.
    └── downloads/                    # Ignored, preserved existing downloads.
```

The wildcard entries represent generated shards, not additional table types. Environments and tool caches are ignored and omitted from this tree.

### Root files

| File | Role |
|---|---|
| `README.md` | The operating guide covering setup, commands, inclusion rules, output schemas, maintenance and troubleshooting. Include external API documentation and examples for querying the Parquet tables with DuckDB. |
| `pyproject.toml` | Dependency-only Python 3.12+ project configuration, test settings and Ruff settings. Keep `package = false`; add no build configuration or installed CLI. |
| `uv.lock` | Reproducible dependency versions for local execution and Actions. |
| `config.toml` | Endpoints, paths, concurrency, timeouts, retry settings and verification limits. Default binary verification cap is 32 MiB. |
| `.gitignore` | Ignore temporary work, downloads, local configuration, environments and caches. |
| `.gitattributes` | Treat generated current data as binary to prevent newline conversion from invalidating checksums. |

### Implementation files

| File | Role |
|---|---|
| `harvester/runtime.py` | Load and validate configuration and provide the shared asynchronous HTTP client. Centralize concurrency limits, timeouts, retries and `Retry-After` handling. |
| `harvester/dataportalen.py` | Own the scoped inventory query, change detection and retrieval of complete Dataportalen metadata, including related entities. Reuse and revalidate cached RDF entities; confirm membership and exclusions before removal. |
| `harvester/researchdata.py` | Resolve current Researchdata identities and retrieve available dataset metadata. Reuse confirmed associations and exact identities without full-catalog OAI enumeration or title-based matching. |
| `harvester/external.py` | Retrieve metadata and resource descriptions from linked repositories, including existing Zenodo, KTH, ICOS and SITES cases. Share resource lookups and report unavailable outcomes. |
| `harvester/verify.py` | Inspect bounded samples of candidate data resources using format-specific libraries. Return verification evidence and explicit failure or unresolved outcomes. |
| `harvester/output.py` | Normalize retrieved metadata into the declared schemas, apply SQL replacements through DuckDB, validate the candidate and write Parquet plus inventory and manifest. Own local atomic replacement and interrupted-replacement recovery. |
| `harvester/schema.sql` | The authoritative DuckDB table definitions, including typed nested structures for repeated entities, resources and provenance. Avoid parallel schema definitions in Python. |
| `harvester/vocabulary.json` | Checked-in namespace and predicate-to-column mappings used during normalization. Keep fetched vocabulary definitions and labels in the RDF cache, and preserve unknown source properties rather than discarding them. |

### Executable scripts and tests

| File | Role |
|---|---|
| `scripts/sync.py` | A thin CLI coordinating inventory, parallel retrieval and output construction. Expose the same stages individually for Actions and collectively for local execution. |
| `scripts/publish.py` | Validate the candidate, verify the expected Git head and create a generated-data-only commit. Reject publication races and never force-push. |
| `tests/test_inventory.py` | Test filtering, pagination, timestamp comparison, additions, removals and incomplete inventories. |
| `tests/test_metadata.py` | Test identity resolution, related entities, description links, metadata fidelity and multiple legitimate associations. Cover RDF cache reuse, revalidation, refresh failures and preservation of RDF terms. |
| `tests/test_verification.py` | Test actual rows, supported formats, licence acknowledgements, byte limits, misleading responses and access failures. |
| `tests/test_publication.py` | Test DuckDB schemas, replacements, retained stale records, deterministic output, checksums, recovery and Git publication safeguards. |

### Workflows and generated files

| File | Role |
|---|---|
| `.github/workflows/harvest.yml` | Run daily at 03:17 UTC or manually, with inventory, two parallel fetch jobs and one dependent build/publication job. |
| `.github/workflows/validate.yml` | Run offline tests, Ruff, formatting checks and published-data validation for code and proposed data changes. |
| `data/current/datasets/part-*.parquet` | Typed metadata and verified resource descriptions for eligible datasets, including Researchdata and external enrichment. |
| `data/current/dataportalen/part-*.parquet` | Complete corresponding Dataportalen metadata, with typed descriptive columns and related entities. |
| `data/current/inventory.json` | Current source membership, modification indicators, confirmed associations and processing outcomes, including excluded or unresolved candidates. |
| `data/current/rdf-cache.json.gz` | A URI-keyed JSON cache of fetched vocabulary definitions and shared RDF entities, compressed with gzip. Preserve RDF term types, languages, datatypes, linked nodes, source URLs and available HTTP validators. |
| `data/current/manifest.json` | Schema version, output filenames, counts and checksums for a consistent published generation. |

## 2. Inventory and retrieval

Use Dataportalen’s Researchdata catalog as the sole inventory.

- Keep one predefined Solr query in `dataportalen.py`, scoped to the Researchdata registry context and excluding `NON_PUBLIC`.
- Page in stable entry-URI order, 100 records per request. Reuse dataset metadata already included in search results.
- Compare registry modification timestamps with previously stored registry timestamps. Refresh new or changed records, transient failures and all records under `--full`.
- Check explicit Dataportalen catalog membership before removing missing records. Confirm newly excluded access rights; inconsistent or incomplete inventories block publication.
- Keep current identifiers exactly, including revision suffixes, without fetching historical versions.
- Do not add periodic full refreshes or independent Researchdata membership scans.

The two fetch stages receive the same change list:

1. Retrieve complete Dataportalen metadata through explicit related-entity lookups, shared within the run.
2. Resolve Researchdata and external dataset resources, retrieve their available metadata and verify candidate data files.

Use landing/access URLs first, then explicit dataset links in descriptions. Preserve valid confirmed associations from the existing data and use exact identifiers or verified canonical identities for new associations. Do not match on titles or cited publication identifiers.

Retrieve dataset-owned structured metadata and metadata representations needed to preserve available attributes. Do not crawl unrelated navigation, citations or previous versions.

### RDF cache

Use one `rdf-cache.json.gz` file, handled with Python's standard-library `json` and `gzip`. This is a reusable metadata lookup cache, not another database or service; no dedicated caching module or dependency is needed.

- Cache fetched vocabulary definitions and labels, publishers, contacts, distributions and other referenced RDF entities by source URI. Store complete JSON-compatible entity descriptions, including linked blank nodes and source provenance.
- Reuse vocabulary definitions between normal runs. Fetch missing definitions when needed and refresh cached definitions during `--full`; an unavailable definition must not discard the original URI or property.
- When processing a new or changed dataset, revalidate each referenced mutable entity once per run using its available HTTP validators. If validators are unavailable, retrieve and compare the entity; do not assume that its parent dataset timestamp describes its changes.
- Share the fetched result across all datasets referencing that entity. Unchanged datasets do not trigger entity lookups merely because a cache exists.
- Keep the checked-out cache read-only in parallel jobs. Each job emits cache updates in its artifact; the build stage merges them and writes one sorted, deterministically gzipped candidate cache. Conflicting updates for the same source URI fail validation rather than choosing an arbitrary winner.
- A failed revalidation does not count as a fresh result. Apply the same required-source failure and external stale-metadata policies as uncached retrieval.
- Remove mutable entities no longer referenced by retained records; keep vocabulary definitions for reuse. Include the cache checksum in the manifest and publish it with the tables and inventory.
- Prefer this small file-based implementation. Adopt a library only if it demonstrably replaces code while preserving this format and refresh behavior; do not introduce a cache server or an additional persistent database.

## 3. Eligibility and verification

A dataset qualifies when at least one resource contains retrievable tabular data.

- Exclude `NON_PUBLIC`, account/API-key requirements, documents, metadata-only responses, imagery and map layers.
- Allow unspecified rights, missing distributions and simple licence acknowledgements.
- Accept CSV/TSV, spreadsheets, statistical files and JSON/API observation tables.
- Preserve licence terms without filtering for commercial permission.
- Prefer streamed samples or bounded API requests. Inspect up to 100 observation rows.
- Cap transferred and decompressed verification content at **32 MiB per resource**.
- For binary formats requiring a complete file, download temporarily within that cap and inspect rows with an established reader.
- Record format, column information, sampled row count, verification time, access method and outcome. Do not publish downloaded data or sample observations.
- Classify oversized, unsupported or inconclusive resources as unresolved, then check other resources belonging to the dataset.
- Retain previously verified datasets after transient failures with prior metadata and an unavailable status. Remove them when membership or eligibility explicitly ends.
- Retry transient failures on the next run. Revisit stable unresolved outcomes after source changes or `--full`.

Use standard-library CSV and JSON support where sufficient, spreadsheet readers for supported workbook formats and `pyreadstat` for statistical files. Keep HTTP transfers under the shared client’s limits.

## 4. DuckDB tables and state

Use explicit DuckDB schemas with `STRUCT` and list columns for repeated metadata. Read existing Parquet into temporary working tables, apply replacements with SQL, validate and export using `COPY ... FORMAT PARQUET` with Zstandard compression. DuckDB supports direct Parquet reading and writing. [DuckDB Parquet documentation](https://duckdb.org/docs/2.0/data/parquet/overview)

| Table | Primary identity | Main columns |
|---|---|---|
| `datasets` | Dataportalen dataset URI | Researchdata identifiers, multilingual titles/descriptions, publishers, subjects, languages, temporal/spatial descriptions, rights, licences, source URLs, resources, verification evidence, availability and additional source properties |
| `dataportalen` | Dataportalen dataset URI | Registry entry URI, modification values, multilingual descriptive fields, identifiers, publishers, contacts, distributions, services and complete related metadata |

- Join both tables on the exact Dataportalen dataset URI.
- Preserve multiple Researchdata associations as a list.
- Preserve additional properties with source, entity/path, property, value, value type, language and datatype fields. Do not serialize the whole record into an opaque payload column.
- Load previous rows, remove deleted or successfully replaced rows, and insert their replacements. Preserve prior rows when refreshing fails transiently.
- Validate uniqueness, matching table membership, nested references and inventory outcomes before export.
- Use stable identifier-based partitions and deterministic ordering. Split oversized partitions until every file is below 100 MB.
- Preserve unchanged shards and avoid timestamp-only rewrites.
- Publish both tables, inventory, RDF cache and manifest together. Keep temporary DuckDB working storage under the ignored work directory.

## 5. Execution and publication

Retain the local command:

```text
uv run python -m scripts.sync
uv run python -m scripts.sync --full
```

Keep `--config`, `--destination` and `--report`. Add stage selection for `inventory`, `dataportalen`, `researchdata` and `build`, with a shared work-directory argument so Actions invokes the same implementation.

The scheduled workflow runs inventory first, starts the two fetch jobs in parallel, then builds and publishes after both complete. Use run-specific artifacts, retained for one day; skip unnecessary jobs when nothing needs processing.

- Serialize harvest runs without cancelling an active run.
- Block publication after required-source failures.
- Record external failures under the retention policy.
- Verify the original `main` head before publishing. If it advanced, stop and rerun against the newer checkout.
- Commit only generated current data and state.
- Verify action versions when implementing the workflow.
- Keep `httpx`, Pydantic, useful parsing libraries and `tenacity`; use DuckDB for all Parquet operations and standard-library JSON/gzip for the RDF cache. Add no direct PyArrow dependency or adapter framework.

## 6. Replacement and acceptance

Implement the new path within the proposed structure, retaining old published data until acceptance passes.

1. Establish DuckDB schemas, output validation and the stage runner.
2. Implement the filtered registry inventory, changes and complete related metadata retrieval.
3. Port necessary Researchdata/external retrieval and implement bounded data verification.
4. Run a fresh live candidate harvest and classify every existing Dataportalen record under the new scope.
5. Test unchanged reruns, source changes, removals, failures and interrupted publication. Verify real examples of every format before claiming live support.
6. Replace the old published collections, delete superseded modules and tests, and update the README.

Remove full-catalog OAI enumeration, sitemap reconciliation, the general matching pipeline, old JSONL storage and redundant configuration. Preserve existing local downloads.

Acceptance requires passing pytest, Ruff, formatting checks and complete DuckDB output validation; verified table joins and metadata fidelity; no duplicate growth; no publication after incomplete inventories; and a successful scheduled/manual workflow. Report eligible, excluded, unresolved and unavailable counts with reasons. Commit implementation separately from generated data.
