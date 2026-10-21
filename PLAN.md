# Implement the simplified Researchdata harvester with DuckDB

## 1. Goal, scope and existing work

Build a small, directly executed harvester that publishes metadata for current Researchdata catalog entries whose data can actually be retrieved in tabular form.

Use Dataportalen’s explicitly identified Researchdata catalog as the inventory. Resolve its linked Researchdata and external resources, retrieve their available metadata, and verify actual tabular data before admitting a new dataset to the published tables.

The normal incremental run must refresh new or changed entries and retry transient failures. It must remove entries whose catalog membership or eligibility has explicitly ended.

Publish two typed Parquet tables, a compact inventory, one compressed semantic cache and a manifest. GitHub Actions runs the same Python commands used locally.

### Scope decisions

- Harvest the current catalog only.
- Preserve exact source identifiers, including revision suffixes, without maintaining historical versions or following previous-version links.
- Exclude datasets explicitly marked `NON_PUBLIC`.
- Exclude resources requiring an account, API key, application, payment or individual access approval.
- Allow unspecified access rights and simple licence acknowledgements.
- Do not require a Dataportalen distribution. A dataset can qualify through an explicitly linked repository containing retrievable tabular data.
- Require evidence of actual data. Public metadata, a download-looking URL or a declared file format is insufficient.
- Exclude documents, metadata-only responses, imagery and map layers.
- Preserve complete available metadata for processed datasets, including attributes not projected into familiar columns.
- Retrieve external metadata during harvesting. Do not defer enrichment until a consumer selects a dataset.
- Do not publish research observations, sample rows or downloaded research files.
- Preserve licence information without introducing a commercial-use filter.
- Do not add a server, installable package, adapter framework, persistent database, historical snapshots or persistent pagination checkpoints.

### Incremental freshness

Dataportalen’s registry modification value determines which unchanged catalog entries can be skipped.

For selected entries, revalidate linked mutable metadata independently. Their parent dataset’s timestamp does not prove that the linked resource is unchanged.

Unchanged entries do not trigger a fresh crawl of all their external resources. Consequently, an external change that does not propagate to Dataportalen is discovered when the entry is otherwise selected, a previous transient failure is retried, or `--full` is run.

Keep that limitation explicit. Do not introduce a periodic full refresh or independent Researchdata inventory behind the user’s back.

### Reuse the work already implemented

The repository already contains:

- `harvester/schema.sql`, with two typed DuckDB tables and a publication-validation view.
- `harvester/vocabulary.py`, with mappings, semantic resolution and a deterministic gzip JSON cache.
- Support for RDF/JSON, JSON-LD, JSON Schema and schemas embedded in OpenAPI JSON documents.
- `scripts/split_data_files.py`, its installed-hook support and separate tests.
- Locked runtime and development dependencies.
- Python 3.12 configuration, Ruff, mypy, Pyright and an offline validation workflow.

Integrate these components. Do not recreate them under different names.

The earlier implementation and published JSONL data currently reside under the ignored `legacy/` directory. Treat them as a read-only comparison source during implementation. The new code must not import `legacy`.

## 2. Final repository structure

Keep no more than **eight production Python files and twelve Python files including tests**.

The existing splitter and its test count toward these limits. Use five harvester modules, three executable scripts and four test modules. Do not add empty `__init__.py` files merely for convention.

The target structure is:

```text
researchdata-harvester/
├── README.md
├── PLAN.md
├── pyproject.toml
├── uv.lock
├── config.toml
├── .python-version
├── .gitignore
├── .gitattributes
│
├── .vscode/
│   └── settings.json
│
├── .githooks/
│   └── pre-commit
│
├── harvester/
│   ├── runtime.py
│   ├── vocabulary.py
│   ├── dataportalen.py
│   ├── researchdata.py
│   ├── output.py
│   └── schema.sql
│
├── scripts/
│   ├── sync.py
│   ├── publish.py
│   └── split_data_files.py
│
├── tests/
│   ├── test_inventory.py
│   ├── test_metadata.py
│   ├── test_pipeline.py
│   └── test_split_data_files.py
│
├── .github/
│   └── workflows/
│       ├── harvest.yml
│       ├── fetch-dataportalen.yml
│       ├── fetch-researchdata.yml
│       ├── publish.yml
│       └── validate.yml
│
└── data/
    ├── current/
    │   ├── datasets.parquet
    │   ├── dataportalen.parquet
    │   ├── inventory.json
    │   ├── rdf-cache.json.gz
    │   └── manifest.json
    ├── work/                         # Ignored temporary work and reports.
    └── downloads/                    # Ignored existing downloads, preserved.
```

`legacy/` remains temporarily during implementation and comparison. It is absent from the final active structure after acceptance.

### Root and configuration files

| File | Role |
|---|---|
| `README.md` | The operating guide for setup, harvesting, scoped diagnosis, output consumption, maintenance and publication. Describe implemented behavior and its limits. |
| `PLAN.md` | This implementation specification. Replace obsolete instructions with this version while preserving the agreed requirements. |
| `pyproject.toml` | Dependency-only project configuration, development checks and splitter settings. Keep `package = false` and no installed CLI entry point. |
| `uv.lock` | Locked dependencies shared by local execution and Actions. Update through uv. |
| `config.toml` | Checked-in endpoints, paths, concurrency, network limits and verification settings. Document every setting in the README. |
| `.python-version` | Select Python 3.12 for local execution and Actions. |
| `.gitignore` | Ignore temporary work, local reports, downloads, environments, caches and `config.local.toml`. Explicitly allow every published current file, including `rdf-cache.json.gz`. |
| `.gitattributes` | Preserve `data/current/** -text` so Git newline conversion cannot invalidate checksums. |
| `.vscode/settings.json` | Select the repository’s `.venv` as the default Python environment. Retain the existing editor configuration. |
| `.githooks/pre-commit` | Invoke the existing oversized-data-file splitter. Do not duplicate publication validation here. |

### Python and SQL files

| File | Role |
|---|---|
| `harvester/runtime.py` | Validate configuration and own shared HTTP behavior, concurrency, retries, deadlines and logging. Supply the same request behavior to source retrieval, verification and semantic resolution. |
| `harvester/vocabulary.py` | Own namespace mappings, semantic normalization and the compressed definition/entity cache. Retain the existing resolver and extend its integration only where necessary. |
| `harvester/dataportalen.py` | Own the scoped queries, inventory comparison, removal confirmation and complete Dataportalen metadata retrieval. Hide EntryStore response details and related-entity traversal from the coordinator. |
| `harvester/researchdata.py` | Resolve current dataset identities, retrieve Researchdata and external metadata, discover resources and verify tabular data. Keep repository-specific handling and format inspection behind this complete operation. |
| `harvester/output.py` | Convert retrieved metadata into the SQL schema, apply current-state replacements, validate and export the candidate. Own manifest generation, cache merging and safe local replacement. |
| `harvester/schema.sql` | The authoritative table and nested-value definitions. Reuse the existing schema and validation view instead of maintaining a second schema in Python. |
| `scripts/sync.py` | Parse CLI arguments and coordinate complete runs or individual stages. Keep retrieval, parsing and SQL logic inside the harvester modules. |
| `scripts/publish.py` | Validate a completed candidate, enforce Git publication prerequisites and publish a generated-data-only commit. Reject incomplete, scoped or outdated candidates. |
| `scripts/split_data_files.py` | Retain the existing general Parquet/JSONL splitter, including gzip support and hook installation. Its behavior remains independent of the catalog builder’s partitioning policy. |

### Tests

| File | Role |
|---|---|
| `tests/test_inventory.py` | Cover scoped selection, query filtering, pagination, modification comparison, additions, removals and incomplete inventories. |
| `tests/test_metadata.py` | Preserve and adapt existing schema, semantic resolver and cache tests. Add essential identity and metadata-preservation scenarios through the working interfaces. |
| `tests/test_pipeline.py` | Combine essential resource-verification, failure-retention, candidate-building and publication tests. Avoid separate files for every implementation module. |
| `tests/test_split_data_files.py` | Keep splitter tests outside the default suite. Run them explicitly when the splitter changes. |

### Workflows

| File | Role |
|---|---|
| `.github/workflows/harvest.yml` | Coordinate scheduled and manual harvesting, create the inventory artifact, and call both fetch workflows in parallel before publication. |
| `.github/workflows/fetch-dataportalen.yml` | Retrieve complete Dataportalen metadata for the shared change list and upload temporary results and cache updates. |
| `.github/workflows/fetch-researchdata.yml` | Retrieve Researchdata/external metadata, verify resources and upload temporary results and cache updates. |
| `.github/workflows/publish.yml` | Build and validate the candidate from both fetch artifacts, then publish it if the expected `main` head still matches. |
| `.github/workflows/validate.yml` | Run offline code checks and validate published data on repository changes. Keep pull-request validation enabled for proposed data changes. |

### Published files

| File | Role |
|---|---|
| `datasets.parquet` | Enriched metadata and verification evidence for eligible datasets. |
| `dataportalen.parquet` | Complete corresponding Dataportalen metadata for exactly the same dataset identities. |
| `inventory.json` | Current source membership, modification indicators, processing outcomes, retry decisions and confirmed associations. |
| `rdf-cache.json.gz` | Shared semantic definitions and reusable mutable metadata entities. Despite its existing name, it also stores JSON Schema definitions. |
| `manifest.json` | Output filenames, schema version, row counts, byte sizes and SHA-256 checksums for one consistent published generation. |

## 3. Dataportalen inventory and exact requests

### 3.1 Source endpoints

Use these configured endpoints:

```text
Registry search:
https://admin.dataportal.se/store/search

Researchdata context:
https://admin.dataportal.se/store/818

Researchdata catalog metadata:
https://admin.dataportal.se/store/818/metadata/9198

Catalog identity:
https://researchdata.se/catalogue/
```

Validate the catalog identity and type when loading catalog metadata. Do not silently continue against another context if configuration or upstream identifiers change.

The registry returns search entries containing RDF/JSON metadata and separate registry information. Related managed entities may require additional lookups. [Dataportalen registry API](https://docs.dataportal.se/registry/api/)

### 3.2 Candidate inventory query

Send a GET request to the registry search endpoint with:

```text
type=solr
limit=100
offset=0
sort=uri asc
```

Use this query, shown before HTTP URL encoding:

```text
public:true
AND rdfType:http\://www.w3.org/ns/dcat#Dataset
AND context:https\://admin.dataportal.se/store/818
AND NOT metadata.predicate.uri.23fc4665:http\://publications.europa.eu/resource/authority/access-right/NON_PUBLIC
```

The access-right field is derived from:

```text
Predicate:
http://purl.org/dc/terms/accessRights

Solr field:
metadata.predicate.uri.23fc4665
```

Keep predicate-field construction in one place. The suffix follows EntryStore’s truncated MD5 convention. Escape Solr values before passing request parameters to httpx; let httpx perform URL encoding.

`public:true` concerns access to metadata. It does not establish that the dataset’s data is public. The separate access-right condition is necessary. [EntryStore search documentation](https://entrystore.org/kb/search/)

This request was checked against the live service during planning. Do not hard-code the observed result counts.

Do not add a distribution-existence filter. Missing distributions do not prevent explicitly linked repository resources from containing usable data.

### 3.3 Pagination and inventory fields

Page by increasing `offset`, with a maximum page size of 100 and stable `uri asc` ordering.

For each candidate retain:

- Exact dataset resource URI.
- Registry entry URI.
- Context ID and entry ID.
- Metadata URL.
- Registry modification value from the entry’s `info` graph.
- Dataset-level modification values as separate descriptive metadata.
- Access-right values.
- Landing-page URLs.
- Distribution references and their count.
- Relevant identity assertions.
- Source metadata already supplied by the search response.

The registry modification value and the dataset’s descriptive `dct:modified` value are different fields. Compare the registry value with its previously stored registry value.

Do not replace source change comparison with the local scrape timestamp.

Validate page structure, ordering and duplicate identities. Detect repeated pages and inconsistent result totals. An inconsistent or incomplete required inventory fails the run without modifying published state.

Search counts are a consistency signal, not sufficient proof of completeness. Reconcile against the catalog membership document.

### 3.4 Explicit membership and removal confirmation

Fetch:

```http
GET https://admin.dataportal.se/store/818/metadata/9198
Accept: application/json
```

Read `dcat:dataset` membership from:

```text
https://researchdata.se/catalogue/
```

Use this small catalog document to distinguish an absent search result from a genuine membership removal.

For a previously retained entry missing from the candidate search:

1. Check whether its exact dataset URI remains in explicit catalog membership.
2. If absent, mark it for removal.
3. If still present, retrieve its current entry metadata and confirm whether its access rights now exclude it.
4. If it is still a candidate but absent from search, treat the search inventory as inconsistent and block publication.

For catalog members omitted from the candidate search, reconcile their exclusion rather than silently treating them as removed. Preserve previously confirmed exclusions when membership and the candidate query remain consistent; fetch unresolved differences.

Do not use disappearance from a filtered query alone to delete data.

### 3.5 Exact dataset and entity lookup

For an exact dataset URI, use the candidate query plus:

```text
AND resource:<escaped-exact-dataset-uri>
```

For a previously known entry, prefer its known metadata URL:

```http
GET https://admin.dataportal.se/store/{contextId}/metadata/{entryId}
Accept: application/json
```

For an unresolved related entity, use:

```text
public:true
AND context:https\://admin.dataportal.se/store/818
AND resource:<escaped-exact-entity-uri>
```

If the referenced entity is not managed in that context, perform an exact resource-URI lookup without the context restriction. Preserve provenance if multiple descriptions exist.

Do not invent a generic `fields=` projection or assume that search responses contain full distribution, publisher or contact descriptions.

Do not rely on `entry?...recursive=dcat&format=application/json` to return complete dataset metadata. The live check during planning returned entry information rather than the complete metadata graph for that JSON request. Use explicit metadata and related-entity retrieval.

### 3.6 Change detection

Schedule an entry when:

- It is new.
- Its registry modification value changed.
- Its registry entry identity changed while the dataset URI remained stable.
- Relevant identity or eligibility evidence changed.
- Its previous processing outcome requires a transient retry.
- Required persisted output is missing.
- `--full` is selected.

Use a canonical fingerprint of relevant source metadata already present in the inventory response to catch changed evidence without creating blank-node-driven differences.

An unchanged record with a completed outcome remains untouched.

Stable unresolved outcomes are retried after source change or `--full`. Transient failures are retried on the next normal run.

A full run uses the same scoped Dataportalen inventory and refreshes every candidate. It does not scan the entire national catalog, enumerate Researchdata independently or traverse historical versions.

## 4. Retrieval, identity resolution and semantic definitions

### 4.1 Shared retrieval behavior

Create one reusable asynchronous HTTP client per process.

`runtime.py` owns:

- Validated configuration.
- Connection pooling.
- Bounded concurrency.
- Retry policy.
- Timeouts and total request deadlines.
- Response-size accounting.
- Logging and request counters.

Use Tenacity for retries rather than maintaining separate retry loops in each source module.

Apply this behavior to semantic-cache requests too. The current cache calls an injected httpx client directly, so merely adding a helper used by other modules would leave cache requests outside the shared policy.

Integrate the resolver with the runtime’s request operation through one explicit dependency. Preserve the existing simple resolver usage; do not access its private attributes or add a second HTTP client inside it.

Default network settings:

| Setting | Default | Meaning |
|---|---:|---|
| Concurrency | 4 | Maximum simultaneous HTTP operations per process. |
| Connect timeout | 10 seconds | Maximum connection-establishment wait. |
| Read timeout | 30 seconds | Maximum period without response data. |
| Attempts | 3 | Total attempts, including the initial request. |
| Operation deadline | 90 seconds | Total request time, including retries and backoff. |
| Verification rows | 100 | Maximum observation rows inspected per resource. |
| Verification byte limit | 32 MiB | Maximum transferred and decompressed content per resource. |

Retry connection failures, timeouts, HTTP 429 and transient server errors.

Honor `Retry-After`, including HTTP-date values. If the requested delay cannot fit within the remaining deadline, record exhaustion instead of retrying early.

Keep streaming transfers within the same deadlines and byte limits. A response that continuously trickles bytes must not evade the total deadline.

### 4.2 Complete Dataportalen metadata

The Dataportalen stage receives the inventory change list and its already-fetched root metadata.

Retrieve dataset-owned related entities, including:

- Distributions.
- Publishers and creators.
- Contact points.
- Qualified attributions and their agents.
- Data services.
- Other explicitly linked descriptive entities required to retain the dataset’s metadata.

Traverse included blank nodes completely. Follow managed metadata relations, not every URI appearing anywhere in the record.

Do not crawl citations, previous versions, unrelated datasets or repository navigation.

Share repeated entity lookups within the worker. Revalidate mutable cached entities once per run when requested.

Preserve all source assertions, including unknown predicates and conflicting descriptions. Typed projections must not replace the complete property representation.

Required Dataportalen retrieval failures prevent publication.

### 4.3 Researchdata identities and metadata

Use, in order:

1. Explicit Researchdata landing-page links.
2. Dataset access URLs and verified canonical redirects.
3. Explicit dataset identity assertions.
4. Previously confirmed associations whose supporting evidence remains valid.
5. Explicit dataset links in descriptions when their context and target establish dataset identity.

Do not match on title similarity or treat a cited publication DOI as a dataset identifier.

Normalize DOI spelling and resolver forms for comparison while retaining original values. Preserve meaningful URL paths, query parameters and revision suffixes.

Keep legitimate multiple associations. Record conflicting evidence as ambiguous.

Use current landing-page structured metadata and targeted Researchdata metadata retrieval. The official service exposes OAI-PMH metadata and documents structured landing-page metadata. [Researchdata API](https://docs.researchdata.se/api/), [Researchdata JSON-LD](https://docs.researchdata.se/metadata/schema-org-json-ld/)

For a resolved current OAI identifier, retrieve the current DataCite, DCAT and JSON-LD representations where advertised. Inspect explicitly advertised additional metadata representations when they contain attributes not covered by those sources.

Do not reintroduce whole-catalog OAI listing or sitemap reconciliation.

Do not synthesize older identifiers or fetch previous versions. Preserve version text found in current metadata as a source attribute only.

If an entry points directly to an external repository and no Researchdata page can be established, retain that explicit outcome. Verified external data can still establish eligibility for an entry in Dataportalen’s Researchdata catalog.

### 4.4 External metadata

Keep external handling inside `researchdata.py`.

Port useful handling for:

- Zenodo.
- KTH’s data repository.
- ICOS.
- SITES.
- Generic structured dataset landing pages.

Prefer repository JSON APIs when the advertised identity supports them. Use structured landing-page metadata as fallback.

Retrieve additional metadata attributes as well as file URLs. Do not stop enrichment merely because one file was found.

Follow dataset identity links, landing pages and explicitly linked metadata resources. Deduplicate URLs and stop cycles. Do not impose an arbitrary three-URL limit.

Source-specific API discovery belongs in the source module. Semantic schema resolution must not execute arbitrary operations described by OpenAPI documents.

For optional external failures:

- Record the resource outcome and reason.
- Retain prior metadata when available.
- Mark retained metadata as stale.
- Do not advance that resource’s successful-refresh evidence.
- Continue other records.
- Retry transient failures on the next run.

Malformed optional external documents are explicit unresolved outcomes. Malformed required source metadata blocks publication.

Unexpected programming errors must fail visibly with dataset context rather than being converted into ordinary external failures.

### 4.5 Semantic resolver interface

Retain one main normalization operation:

```python
async with SemanticResolver(cache_path, client, updates_path) as resolver:
    result = await resolver.resolve(document, source_url=document_url)
```

Supported inputs remain:

- RDF/JSON.
- JSON-LD.
- JSON Schema.
- Schemas embedded in OpenAPI 3.x JSON documents.

Use explicit format selection for schemas that cannot be recognized automatically, including boolean schemas without `$schema`.

Retain the existing capabilities:

- Expanded RDF predicates.
- Languages and datatypes.
- Named graphs.
- Deterministic, source-scoped blank-node identities.
- Unknown source properties.
- Semantic definition outcomes.
- JSON Schema references, anchors, relative identifiers and cycles.
- Explicit unsupported outcomes for dynamic or recursive schema evaluation.
- Original parsed JSON evidence for attributes not expressible as RDF.

The resolver does not validate observation instances or execute OpenAPI requests.

The output builder must preserve returned schema and source-JSON attributes in the typed property representation. Do not discard them because they do not appear in the normalized RDF statements.

### 4.6 One compressed semantic cache

Keep `data/current/rdf-cache.json.gz`.

Use the existing standard-library JSON/gzip implementation. Do not replace it with Parquet, add a cache service or introduce a dedicated caching dependency.

The cache stores:

- Vocabulary definitions and labels.
- JSON-LD contexts.
- JSON Schema documents.
- Shared mutable metadata entities.
- Source and resolved URLs.
- Available HTTP validators.
- Availability and stale-data outcomes.

Reuse vocabulary definitions during normal runs. Fetch missing definitions when encountered and refresh requested definitions during `--full`.

Revalidate requested mutable entities independently with ETag or Last-Modified validators. Without validators, fetch and compare content.

Use one worker-owned cache instance for semantic resolution and mutable-entity retrieval. Make that sharing explicit through the internal constructor/interface rather than reaching into private resolver state.

Parallel jobs read the published cache without modifying it. Each emits its own update artifact.

At build time:

1. Load the published cache.
2. Merge worker updates.
3. Reject conflicting updates for the same cache key.
4. Remove mutable entities no longer referenced by retained output.
5. Retain vocabulary and schema definitions for reuse.
6. Write sorted JSON with deterministic gzip headers.
7. Include the resulting checksum in the manifest.

Missing optional definitions must not discard original terms.

A context required to interpret a document is different from an optional label definition. If required context resolution fails, that document has not been successfully normalized.

Preserve successful work from other records when an optional document fails. Adapt the current resolver’s artifact lifecycle so a caught optional-document failure does not erase unrelated successful worker updates. Failed documents must not contribute partial successful results.

## 5. Eligibility and bounded data verification

### 5.1 Admission rule

A new dataset enters the published tables only when at least one resource yields actual tabular observations.

The following do not establish eligibility:

- A `PUBLIC` label.
- A filename ending in `.csv`.
- A declared media type.
- HTTP 200.
- A metadata JSON document.
- A table describing files or catalog entries.
- An HTML page containing a download button.

Verification must establish a tabular structure and at least one observation row.

### 5.2 Supported data families

Implement verification using the existing dependencies:

| Data family | Verification |
|---|---|
| CSV and TSV | Parse a bounded stream with the standard-library CSV reader. Establish columns and observation rows. |
| JSON data | Inspect a bounded response containing records or an explicitly described observation table. Do not count catalog metadata arrays as data. |
| Observation APIs | Make a bounded request using source-advertised parameters or source-specific handling. Do not guess endpoints or required selectors. |
| XLSX | Inspect a temporary bounded file with `openpyxl`. |
| XLS | Inspect a temporary bounded file with `xlrd`. |
| Statistical files | Use `pyreadstat` for the formats it supports, with bounded file acquisition and row inspection. |
| Parquet | Use DuckDB to inspect a bounded complete file. Do not add PyArrow as a direct dependency. |

Handle gzip-wrapped supported resources with bounded decompression.

Do not build a general archive extraction system. Unsupported archives remain unresolved unless their repository exposes individual supported files.

### 5.3 Limits and evidence

Inspect at most 100 observation rows per resource.

Limit both transferred bytes and decompressed verification content to 32 MiB per resource.

For binary formats requiring a complete file:

- Download into ignored temporary storage.
- Stop when the byte limit is exceeded.
- Inspect with the established format reader.
- Remove the temporary file after verification.

Do not extract untrusted filenames outside the work directory.

Record:

- Advertised and resolved URLs.
- Source metadata URL.
- Distribution identity when present.
- Detected format and media type.
- Column names and detected types.
- Number of sampled rows.
- Transferred and decompressed byte counts.
- Access method.
- Verification time.
- Licence-acknowledgement URL when used.
- Outcome and reason.

Do not persist sampled values in output, cache artifacts or debug logs.

A complete-file checksum may be verified when the whole bounded file is available. Do not claim checksum verification from a partial sample.

### 5.4 Access and outcomes

Follow ordinary redirects and simple anonymous licence acknowledgements. Do not automate registration, credentials, CAPTCHA, payment or access applications.

Classify resources as verified, unavailable, excluded or unresolved, with a specific reason.

Examples of unresolved outcomes include:

- A supported format exceeds the verification limit.
- The format is unsupported.
- The endpoint needs selectors that cannot be established from metadata.
- The response is inconclusive.
- The resource cannot be reliably distinguished from metadata.

Continue checking other resources belonging to the dataset.

For dataset publication:

- **Eligible:** At least one resource has successful verification.
- **Unavailable:** A previously eligible dataset is retained with prior verification evidence after a current retrieval failure.
- **Excluded:** Explicit membership or access/eligibility evidence ends its inclusion.
- **Unresolved:** No resource has established eligibility, and the evidence does not justify exclusion.

New unresolved datasets remain in the inventory only.

Do not remove a previously verified dataset solely because one URL temporarily fails. Remove it when catalog membership ends or refreshed evidence explicitly establishes that it no longer qualifies.

## 6. Tables, inventory and candidate construction

### 6.1 Two typed tables

Use the existing SQL definitions as the authoritative schema.

Both tables use the exact Dataportalen dataset URI as their primary identity and contain the same dataset membership.

| Table | Contents |
|---|---|
| `datasets` | Current Researchdata associations, enriched descriptive metadata, resources, verification evidence, availability, source outcomes and complete properties. |
| `dataportalen` | Registry identity and modification values, complete dataset metadata, distributions, services, related entities and complete properties. |

Shared descriptive projections include:

- Identifiers.
- Multilingual titles and descriptions.
- Publishers, creators and contacts.
- Subjects and keywords.
- Languages.
- Temporal and spatial descriptions.
- Access rights, rights statements and licences.
- Landing pages.
- Issued and modified values.
- Distributions and services.

Use DuckDB `STRUCT` and list columns for repeated and nested data. Do not put the complete record in one opaque JSON column.

Preserve additional properties using the existing fields:

```text
value
value_type
language
datatype
source_url
entity_id
graph_id
property
path
```

Retain original lexical values. Preserve JSON array ordering, empty containers, XML attributes, repeated elements and namespace-qualified names during conversion.

Use JSON pointers or explicit namespace-expanded paths where necessary. No raw XML/RDF strings belong in the published tables.

Keep source dates as source values even when they are partial or invalid. Operational timestamps use the declared timestamp types.

### 6.2 Inventory state

`inventory.json` replaces separate matching, checkpoint and run-history stores.

For each current catalog identity retain the information needed for the next decision:

- Dataset URI and registry entry identity.
- Registry modification value.
- Canonical source/identity fingerprint.
- Eligibility and availability outcome.
- Outcome reason.
- Confirmed Researchdata associations and their evidence.
- Ambiguous or unresolved identity evidence.
- Whether a transient retry is required.
- Relevant external-resource validators and current outcomes.
- Reference information needed to retain cached mutable entities.

Persist confirmed indirect associations here. Do not add a separate `matches.json`.

Invalidate associations when membership or their supporting identity evidence changes.

Do not maintain historical inventory entries for removed datasets. Report removals in the run report, then remove their current-state entries.

### 6.3 Temporary artifacts

Stage raw retrieved metadata and processed record results under `data/work/<run>/`.

Artifacts contain:

- Base Git commit.
- Configuration and implementation fingerprints.
- Scope and full/incremental settings.
- Inventory/change list.
- Source payloads needed for candidate construction.
- Per-record processing outcomes.
- Worker cache updates.
- Request counters and timings.

Keep these artifacts temporary. Do not publish raw-response archives.

Write completed record results as processing proceeds so an interruption does not discard unrelated successful work.

Reuse results only when their input identity/fingerprint, configuration and implementation revision still match. Use atomic artifact writes and reject incomplete artifacts.

Actions artifacts expire after one day. Local work directories can be retained for diagnosis until explicitly cleaned.

### 6.4 Building a candidate

Use a temporary DuckDB connection and working storage inside the ignored work directory.

Build as follows:

1. Load and validate the existing manifest when a current generation exists.
2. Create the declared tables from `schema.sql`.
3. Insert existing Parquet rows into those declared tables so constraints are reapplied.
4. Remove confirmed obsolete identities.
5. Replace successfully refreshed records.
6. Retain previous source values and verification evidence where the defined failure policy requires them.
7. Merge cache updates and construct the next inventory.
8. Validate the complete candidate.
9. Export ordered Parquet files and supporting state.
10. Write the manifest last.

The builder must not publish half of a record pair. Every retained identity must exist in both tables.

Source outcomes may differ within a dataset. Preserve provenance and stale status so old external metadata is not presented as freshly retrieved.

### 6.5 Validation

Before publication, check:

- Primary-key uniqueness.
- Identical dataset membership between both tables.
- At least one successful verification record for every admitted dataset.
- Consistency between availability and current verification outcomes.
- Valid nested references and provenance.
- Inventory outcomes consistent with table membership.
- Cache references retained for unchanged records as well as refreshed ones.
- Counts and checksums matching actual files.
- No unsupported partial/scoped generation marked as complete.
- No required-source failure hidden inside an otherwise completed artifact.

Run the existing `publication_errors` view and reject any returned rows.

Validation is an offline operation and must not fetch new source metadata.

### 6.6 Parquet file count and size

The target is exactly:

```text
datasets.parquet
dataportalen.parquet
```

Write each complete table with DuckDB, Zstandard compression and stable dataset-URI ordering.

**Keep one file per table whenever its measured size is below 100,000,000 bytes.**

Only split a table whose complete compressed file reaches or exceeds that limit.

For an oversized table:

- Partition by consecutive dataset-URI ranges.
- Start with the smallest candidate part count.
- Divide rows approximately evenly.
- Measure every written part.
- Increase the part count only when a part still reaches the maximum.
- Keep the first valid minimal part count from that procedure.
- Use names such as `datasets_part_1.parquet`.
- Merge back to fewer parts whenever a later generation permits it.

Do not create parts for individual workers, incremental batches or small updates.

Do not produce many approximately 20 MB files when the table fits in fewer files.

The existing commit hook uses a 50 MB initial target for general file splitting. That target does not control the catalog builder. The builder must finish splitting and manifest generation before Git staging, leaving every published file below the hook’s 100 MB threshold.

The hook does not update manifests. It must make no changes to an already validated catalog publication.

### 6.7 Determinism and replacement

Sort stable identities and unordered metadata collections deterministically.

Use deterministic gzip serialization for the cache. Preserve existing output bytes when their logical contents are unchanged.

Do not rewrite metadata solely to record that another identical fetch occurred. Put routine request timings and attempt information in the run report.

Publish the tables, inventory, cache and manifest as one generation.

For local replacement, validate a sibling candidate before swapping it into the destination. Recover an interrupted directory replacement automatically on the next invocation.

Use an OS lock for concurrent local publication. A lock filename by itself does not prove that a process holds the lock.

Do not recommend manual deletion of lock or replacement directories as routine recovery.

## 7. CLI, configuration and diagnostics

### 7.1 Commands

Normal incremental execution:

```sh
uv run python -m scripts.sync
```

Explicit full refresh:

```sh
uv run python -m scripts.sync --full
```

Scoped development run:

```sh
uv run python -m scripts.sync \
  --dataset-uri "<exact Dataportalen dataset URI>" \
  --destination data/work/scoped-current \
  --work-directory data/work/scoped-run \
  --report data/work/scoped-report.json \
  --log-level DEBUG
```

Deterministic small sample:

```sh
uv run python -m scripts.sync \
  --limit 10 \
  --destination data/work/sample-current \
  --work-directory data/work/sample-run
```

Stage execution:

```sh
uv run python -m scripts.sync --stage inventory --work-directory data/work/run
uv run python -m scripts.sync --stage dataportalen --work-directory data/work/run
uv run python -m scripts.sync --stage researchdata --work-directory data/work/run
uv run python -m scripts.sync --stage build --work-directory data/work/run
```

Offline catalog validation:

```sh
uv run python -m scripts.sync --stage validate --destination data/current
```

Git publication:

```sh
uv run python -m scripts.publish \
  --candidate data/work/run/candidate \
  --expected-head "<base-main-commit>"
```

The default `sync` command runs the stages locally and safely replaces its destination. It does not commit or push.

Individual fetch stages write artifacts only. The individual build stage writes a candidate only.

`scripts.publish` is the separate operation that publishes a validated complete candidate to Git.

### 7.2 Arguments and path rules

Support:

- `--config`.
- `--destination`.
- `--report`.
- `--work-directory`.
- `--full`.
- `--stage`.
- Repeatable `--dataset-uri`.
- `--limit`.
- `--log-level`.

Use `--dataset-uri` and `--limit` as alternative scope selectors.

Configured paths resolve relative to the selected TOML file. Explicit CLI paths resolve relative to the current working directory.

`config.local.toml` is an ignored alternative selected with `--config`; do not add an implicit configuration-overlay system.

An empty destination starts a fresh harvest. It does not automatically borrow current state from another destination.

### 7.3 Scoped-run restrictions

Explicit dataset selection retrieves only the selected entries.

A limited run stops after collecting its deterministic sample. It must not enumerate the full catalog first.

Scoped runs:

- Must use an ignored destination.
- Are marked as incomplete catalog coverage.
- Cannot infer catalog-wide removals.
- Cannot advance production state.
- Cannot be published by `scripts.publish`.

A selected record can reuse valid temporary results from the same scoped work directory.

### 7.4 Configuration cleanup

Retain only settings used by the new implementation.

Remove sitemap and full-catalog OAI enumeration settings.

Keep a targeted Researchdata OAI endpoint only for current-record retrieval. Remove the general SPARQL endpoint if no implemented operation uses it.

Document units, defaults and valid ranges for all settings. Validate configuration before network work begins.

Keep byte units explicit:

- Verification limit uses MiB.
- Git file limits and splitter settings use decimal MB.

### 7.5 Logging and reports

Use standard-library logging.

INFO logs report stages, progress, counts and elapsed time.

DEBUG logs include:

- Dataset URI.
- Source and operation.
- Redacted URL.
- Request attempt.
- HTTP status or exception class.
- Elapsed time and retry delay.
- Cache reuse or refresh decision.
- Parser and verification outcome.

Do not log credentials, signed query values, response bodies or sampled observations.

Unexpected exceptions include a traceback and dataset context.

The JSON run report includes:

- Full or scoped coverage.
- Inventory, fetched and unchanged counts.
- Eligible, excluded, unresolved and unavailable counts.
- Reasons for exclusions and unresolved outcomes.
- Confirmed and ambiguous associations.
- Retried and retained-stale records.
- Required and optional source failures.
- Request attempts, including failures.
- Stage and total elapsed time.
- Table row counts, compressed sizes and file counts.
- Publication or no-change outcome.

Do not promise a sync duration before measuring a real run.

## 8. GitHub Actions and publication

### 8.1 Coordinator

`harvest.yml` runs:

- Daily at **03:17 UTC**.
- Manually through `workflow_dispatch`, with a full-refresh option.

Use one coordinator concurrency group with cancellation disabled.

The coordinator records the base `main` commit and creates the inventory artifact.

Both fetch workflows consume that same artifact and check out that same commit.

### 8.2 Parallel fetch workflows

`fetch-dataportalen.yml` and `fetch-researchdata.yml` expose `workflow_call`.

Run them in parallel after inventory.

Each workflow:

- Uses read-only repository permissions.
- Installs locked dependencies with uv.
- Executes the corresponding Python stage.
- Uploads distinctly named temporary artifacts.
- Leaves tracked current data unchanged.
- Retains diagnostic artifacts when a stage fails, without treating them as successful inputs.

Do not coordinate independent runs through `workflow_run` or dispatch chains.

### 8.3 Publication workflow

`publish.yml` exposes `workflow_call`.

The coordinator invokes it only after both fetch stages have completed successfully and there are changes to apply.

Removal-only and inventory-state-only changes must still reach the build stage. Fetch stages can produce valid empty result artifacts in those cases.

The publication workflow:

1. Downloads the inventory and both same-run fetch artifacts.
2. Verifies their base commit and fingerprints agree.
3. Builds the candidate.
4. Runs complete offline candidate validation.
5. Confirms the run has full catalog coverage.
6. Confirms no required-source failure occurred.
7. Confirms remote `main` still equals the expected base commit.
8. Stages only generated current data.
9. Creates one data-only commit when content changed.
10. Pushes without force.

Check the expected remote head again immediately before publication. A normal push rejection also fails publication safely.

If `main` advanced, stop. Update the checkout and rerun against the new state. Do not merge an old generated candidate over newer data.

Require a clean index before staging generated output.

Only the publication caller/job receives write permission. Fetch jobs do not receive it.

Keep artifact retention at one day and use job-level timeouts. Use a 90-minute initial timeout for harvest stages and retain the existing 10-minute validation timeout.

### 8.4 Validation workflow

Retain the current offline checks:

```sh
uv run ruff check .
uv run ruff format --check .
uv run mypy
uv run pyright
uv run python -m pytest
```

Add the offline catalog-validation command once published current files exist.

Keep the push exclusion for routine generated-data commits. Do not exclude data paths from pull-request validation.

Default pytest continues to exclude `test_split_data_files.py`.

Run the hook tests explicitly after changing the splitter:

```sh
uv run python -m pytest tests/test_split_data_files.py
```

Use the `github-actions-latest` skill whenever workflow files are created or edited. Verify action versions at implementation time rather than copying stale version references into new workflows.

## 9. Required implementation order

Follow these six steps in order.

### Step 1. Define the essential behavior tests

Retain existing useful metadata, schema and cache coverage.

Add a small number of end-to-end scenarios covering:

- Inventory filtering and change detection.
- Metadata preservation and identity evidence.
- Actual-data verification and its limits.
- Failure retention and safe publication.
- The two-file default.

Use public module operations as test entry points. Do not test every helper or add a large configuration matrix.

This step may be assigned to one subagent explicitly configured as `gpt-6.1-sol` with `reasoning_effort="high"` while the main agent begins step 2. Limit that agent to the agreed tests.

Keep four test files total, including the separate splitter test.

### Step 2. Build the working prototype

Implement the shortest complete path:

```text
Scoped Dataportalen inventory
    → complete metadata retrieval
    → Researchdata/external resolution
    → bounded tabular verification
    → two DuckDB-written Parquet tables
```

Use the implemented schema and semantic resolver.

Include scope selectors, request deadlines, byte limits and dataset-level error context immediately.

Run the scoped prototype and fix concrete failures. Defer broad linting, repeated full suites, exhaustive review and polishing until the path works.

Measure requests and elapsed time in the prototype so subsequent decisions use evidence.

### Step 3. Deepen the working modules

Use the `codebase-design` skill and its `DEEPENING.md` guidance.

Keep these complete operations behind small interfaces:

- Runtime and bounded HTTP behavior.
- Dataportalen inventory and complete metadata.
- Researchdata/external resolution and verification.
- Semantic normalization and cache handling.
- Candidate construction and validation.

Keep source-specific decisions local. Remove pass-through wrappers and duplicated state.

The coordinator must not manage parser internals, cache keys, RDF traversal or SQL row construction.

The proposed filenames can change if the prototype reveals a better grouping, but the final implementation must remain within eight production Python files and twelve total.

Do not add another implementation tree or permanent architecture reports.

### Step 4. Adapt and run the essential tests

Update tests to use the settled interfaces.

Preserve behavioral assertions. Replace obsolete tests instead of accumulating parallel old and new coverage.

Add a test only for an essential correctness risk or a reproduced defect not already covered.

The essential scenarios must demonstrate:

- Complete pagination and failure on inconsistent inventory.
- New, changed, unchanged, excluded and removed records.
- Rejection of citation-based identity matches.
- Legitimate multiple associations and ambiguity retention.
- Preservation of languages, datatypes, repeated values and unknown properties.
- Cache reuse, conditional refresh and worker-update merging.
- Verification of observations rather than metadata.
- Byte limits, interrupted responses and access restrictions.
- Retention of prior successful data after optional external failure.
- Rejection of publication after required-source failure.
- Matching table membership and deterministic output.
- Two Parquet files when both tables fit.
- Rejection of scoped or outdated publication candidates.

### Step 5. Finish diagnostic logging

Complete the INFO and DEBUG behavior in the existing modules.

Demonstrate that one failing dataset can be diagnosed using its identity, request attempts, parser outcome and final reason.

Keep logs in the console and temporary run artifacts. Do not introduce a logging framework or permanent log archive.

### Step 6. Complete acceptance and cutover

First demonstrate:

- An explicit selected-record run.
- A deterministic limited run.
- A bounded HTTP failure.
- Preservation of other completed record artifacts.
- A targeted rerun using the same work directory.
- No production mutation from scoped execution.

Then run:

1. The required offline checks.
2. One complete live candidate harvest.
3. Complete candidate validation.
4. An unchanged normal incremental rerun.
5. A manual coordinated workflow that successfully publishes current data.

Verify representative live resources for each claimed supported data family. Clearly distinguish fixture-tested support from formats actually exercised against live sources.

Do not run repeated full harvests between small fixes. Repeat only checks affected by a subsequent change.

## 10. Acceptance, cleanup and operating documentation

### 10.1 Acceptance criteria

The implementation is complete when:

- All eligible current Dataportalen candidates have a documented outcome.
- New published datasets have evidence of retrievable tabular observations.
- Both Parquet tables have identical unique dataset identities.
- Complete source properties and provenance survive normalization.
- Confirmed associations are reused and invalidated correctly.
- Optional external failures retain prior metadata with explicit stale outcomes.
- Required-source failures leave the published generation unchanged.
- Scoped runs cannot remove production records or publish.
- Interrupted work can reuse valid completed temporary results.
- An unchanged incremental run causes no duplicate growth or unnecessary source retrieval.
- Output consists of exactly two Parquet files whenever each table fits below 100 MB.
- Every published file remains below 100 MB.
- The cache, inventory and manifest agree with the tables.
- Ruff, formatting, mypy, Pyright and the default pytest suite pass.
- A coordinated manual Actions run completes and publishes correctly.
- Implementation changes and generated data are committed separately.

Report actual counts, requests, runtime, compressed sizes and omissions by reason.

Do not describe metadata completion as proof that every advertised resource is downloadable.

### 10.2 Compare and preserve existing data

Read the earlier manifest and files under `legacy/data/current/` for comparison.

Compare:

- Catalog identities.
- Confirmed associations.
- Retained metadata attributes.
- Resource descriptions.
- Previously available file evidence.
- Outcomes under the new eligibility rules.

The new scope intentionally excludes metadata-only and inaccessible datasets. Explain those differences by identity and reason rather than treating a reduced row count as automatic data loss.

Do not blindly copy old eligibility conclusions. New table admission requires the defined verification evidence.

Preserve `legacy/data/downloads/`, including its integrity manifest. Move it to `data/downloads/` only after checking destination conflicts and verifying copied/moved file integrity.

Do not create another permanent migration command.

### 10.3 Remove obsolete implementation after acceptance

After successful cutover:

- Remove obsolete code under `legacy/`.
- Remove old JSONL generations after their retained information has been accounted for.
- Remove full-catalog OAI enumeration and sitemap reconciliation.
- Remove the general catalog-wide matching pipeline.
- Remove obsolete SQLite, snapshot, run-history and persistent-checkpoint logic.
- Remove unused configuration and dependencies.
- Preserve downloaded research files.
- Keep temporary diagnostic artifacts only while they are useful.

Verify resolved deletion targets are inside the intended repository directories before recursive cleanup.

### 10.4 Complete the README

Replace the current placeholder with a self-contained operating guide covering:

- What qualifies for publication.
- What the two tables contain and how to join them.
- Setup with `uv sync --locked`.
- Incremental and full commands.
- Scoped development commands.
- Stage execution and artifact reuse.
- Configuration defaults, ranges and path rules.
- Semantic cache behavior.
- Source and external failure policies.
- Actual freshness guarantees.
- Manifest-based output discovery.
- DuckDB examples for reading both tables.
- Schedule and manual Actions execution.
- Publication prerequisites and remote-head conflicts.
- Offline validation commands.
- Splitter installation and its separate tests.
- Dependency maintenance.
- Troubleshooting request failures, incomplete inventories, stale metadata, locks and interrupted local replacement.
- Links to the external API documentation used by the implementation.

Keep changing catalog counts and lengthy dated acceptance narratives out of the README.

The final repository must have one active execution path, two well-defined data tables, a reusable semantic cache, and enough diagnostics to resolve a failed record without restarting an unbounded full-catalog scrape.
