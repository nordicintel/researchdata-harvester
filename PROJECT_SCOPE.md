# Researchdata harvester: agreed scope

Updated 2026-10-21. This document records the intended product and supersedes conflicting historical plans, README descriptions and earlier suggestions in this conversation. It describes requirements; it does not claim the current implementation already satisfies them.

## Purpose

Build a standalone Python harvester for the complete current Researchdata catalog and its corresponding Dataportalen records. Retrieve all accessible dataset metadata and attributes automatically, maintain current results incrementally, and publish JSON-compatible outputs through a scheduled GitHub Actions workflow.

Project folder: `C:\Users\ruben\NordicIntel\researchdata_harvester`.

Target repository: `nordicintel/researchdata-harvester`, public. The public-repository decision supersedes the earlier private-repository intention.

## Binding requirements

### 1. Complete metadata harvesting

- Resolve every accessible piece of metadata and every accessible attribute for every current dataset as part of the harvest, including records whose underlying research files are restricted.
- Include corresponding Dataportalen metadata and explicit links between the two catalogs.
- Metadata enrichment belongs in the scheduled harvest, not in an on-demand workflow triggered when someone selects a dataset.
- Follow relevant source-provided links when needed to obtain metadata missing from the catalog APIs. Bound retrieval to the dataset and its explicitly linked resources; do not crawl unrelated catalogs or entire external repositories.
- Preserve multilingual and repeated values, identifiers, publishers, contacts, licenses, access conditions, distributions, services, file metadata and other available attributes.
- Report inaccessible metadata and unresolved relationships explicitly. Restricted data files must not cause an otherwise valid metadata record to disappear.
- Harvesting file metadata and distribution links does not imply downloading all underlying research data. Bulk research-file storage and parsing those files into a common tabular interface are outside this cleanup.

### 2. Current catalog only, with no versioning features

- Keep only the latest/current record for each dataset.
- Replace superseded metadata and links; remove datasets that leave the current catalog.
- Remove historical-version retrieval, historical links, revision browsing, retained obsolete records and historical export snapshots.
- Preserve exact source identifiers and source-supplied attributes where needed to describe and match the current dataset. A source identifier containing a revision suffix is still an identifier, not a reason to implement a version-history system.
- Do not manufacture additional dataset entries from previous revisions or treat a dataset-level DOI and a current-release DOI as interchangeable without evidence.

### 3. Incremental, restartable harvesting

- Interpret resumable scraping as refreshing persisted current state between runs: fetch new datasets, refresh and replace datasets modified since the last successful run, and remove datasets no longer in the catalog.
- Enumerate lightweight current membership and available modification indicators as necessary to detect additions, updates and removals. Avoid refetching unchanged complete metadata unnecessarily.
- Apply the same principle to linked metadata where the source exposes a usable change indicator. Do not assume a Researchdata timestamp proves that Dataportalen or an external repository has not changed independently.
- Record a successful sync boundary only after successful completion. Never infer removals from an interrupted or incomplete inventory.
- Reprocessing after failure must be idempotent. Retrying a run must not duplicate records or links.
- Keep persistence and maintenance minimal. There is no requirement to resume at an exact page, preserve per-page checkpoints, store every past run, or maintain a general workflow recovery framework.
- Retain ordinary request timeouts, bounded concurrency, transient-error retries and handling of server retry instructions.

### 4. GitHub-hosted operation and publication

- The harvester will live in the public GitHub repository named above.
- A normal run will be a scheduled GitHub Actions workflow. Also allow manual workflow execution.
- Persist the current metadata and the minimal state needed for subsequent incremental runs across ephemeral Actions runners.
- Use GitHub-hosted file storage with individual published files kept within the requested 100 MB limit; gzip where necessary. If compression is insufficient, split outputs deterministically rather than exceeding the limit.
- Publish one current output set. Do not add dated snapshot directories, historical release archives or application-managed version retention.
- Public Git history is distinct from a versioning feature in the harvester; do not add custom historical-data functionality on top of it.
- The user's hosting premise is that public-repository Actions provide the needed execution minutes. Verify the applicable runner, storage and file-size limits when implementing the workflow; do not treat this document as a verified statement of unlimited usage for every GitHub service.
- Publish current data and incremental state on `main` in one commit, daily at 03:17 UTC and through manual workflow execution.
- Use JSON/JSONL files as both the published output and persisted current state. SQLite is used only for the one-time migration.

### 5. Minimal persistent matching map

- Store a JSON mapping from confirmed Researchdata identifiers to corresponding Dataportalen identifiers for cases that cannot be matched directly.
- Perform direct identifier matching without requiring stored mapping entries.
- Reuse confirmed indirect matches to avoid repeatedly running expensive discovery and matching work.
- Allow multiple confirmed target identifiers where the source relationships require them; do not force a one-to-one relationship.
- Add newly confirmed mappings, retain valid mappings and remove mappings whose source or target is no longer current. Re-evaluate affected mappings when relevant identity evidence changes.
- Do not persist guesses as confirmed mappings. Keep unresolved and ambiguous cases distinguishable.
- Update the mapping deterministically and safely, with minimal maintenance logic and no mapping history or elaborate cache framework.

### 6. JSON-compatible final metadata

- All final metadata must be JSON-compatible. Do not publish XML or RDF serialization as final metadata.
- Preserve the meaning of source values, repeated fields, language tags, types and relationships in the JSON representation.
- XML and RDF may still be parsed as upstream transport formats. Their use by the source does not require retaining duplicate raw-response archives or exposing those formats to consumers.
- Remove the accumulated raw OAI response-page archive. Retain the current information needed for metadata completeness and matching in JSON-compatible form.
- Keep deterministic output ordering and safe publication of complete files. Consistent publication does not require retaining previous export generations.

### 7. Keep this document as the scope reference

- This file captures the agreed requirements before further implementation.
- Subsequent implementation plans must follow it and explicitly identify any remaining decision rather than inventing a requirement.
- Do not reintroduce rejected features under different names.

## Clarifications established during the discussion

### Catalog scope and matching

- Researchdata catalog membership can be obtained directly from Dataportalen. Scanning the entire global Dataportalen catalog is unnecessary for this package.
- Remove the global audit mode and its unrelated stored catalog records.
- In the verified 2026-10-21 harvest, Researchdata contained 6,691 current records and its Dataportalen catalog contained 6,626 entries. These are observations, not constants.
- All 6,626 Dataportalen entries had links to current Researchdata records. Two Dataportalen entries retained ambiguous identity matches; none required retrieving an older Researchdata version.
- Of those entries, 5,039 matched through identifiers, including the two ambiguous entries. Another 1,587 needed compound metadata matching.
- Fifty Dataportalen entries represented multiple Researchdata records. Preserve legitimate relationships rather than equating record counts or forcing one-to-one matching.
- The existing compound matcher recovered real coverage and should not be discarded merely because the surrounding persistence and orchestration were overbuilt. The persistent confirmed mapping should reduce repeated matching work.

### Distribution links and external metadata

- The 246 datasets discussed were already present in the Researchdata API; landing-page inspection did not discover 246 additional datasets.
- Inspection supplied 1,593 individual file URLs absent from the stored Researchdata DataCite, DCAT and JSON-LD payloads and from their linked Dataportalen metadata.
- Their hosting repositories were Zenodo for 238 datasets, KTH Data Repository for six, ICOS for one and SITES for one.
- Among their Dataportalen records, 244 had distributions containing DOI access URLs. Two had no attached distribution. The inspected distributions advertised no download URL or access service.
- Consequently, Dataportalen already supplied routes to the equivalent datasets in 244 cases, but those distribution links did not enumerate the individual files.
- Equivalent access links must be assessed through their relationships and destinations, not solely through exact URL-string equality.
- The earlier claim that external metadata discovery had no purpose was too broad. The binding requirement is now complete automatic metadata retrieval. Where linked repositories provide additional accessible attributes, retrieve them during harvesting, preferably through a suitable source API when available.
- Do not infer that discovered files are tabular, useful, openly downloadable or eligible for redistribution merely because metadata exposes their URLs.

### Runtime and simplicity

- The last two measured incremental syncs took approximately 6.0 and 6.4 minutes. They already excluded historical-version harvesting.
- Those timings included reuse of prior file-discovery results. They do not establish the duration of a fresh full harvest or a run that revisits every external repository.
- Preserve minimal current change-detection state where it prevents unnecessary metadata requests. This does not justify retaining per-run discovery histories or general checkpoint machinery.
- Fetching all OAI sets on every sync has no demonstrated role in the current harvesting or matching flow; remove that automatic work unless a specific metadata requirement needs it.
- The standard harvest uses DataCite, DCAT and JSON-LD for complementary information. Arbitrary format-subset sync modes add no established product value and should be removed from the normal workflow.

## Cleanup and data handling

- The user selected cleanup of both code and existing stored data, not code-only changes.
- Remove obsolete metadata, unrelated Dataportalen rows, historical links, archived response pages and superseded generated export folders.
- Preserve the current catalog, valid current relationships and downloaded files during migration.
- Existing downloaded-file paths refer to the previous project location. Reconcile those paths against the moved files and verify them before updating references.
- Do not replace the removed features with a new generic adapter framework, HTTP service, historical database or elaborate orchestration system.
- Remove tests and documentation specific to deleted features. Keep tests that protect current-catalog completeness, accurate matching, incremental updates, removal safety, JSON fidelity and safe publication.

## Acceptance criteria for the revised implementation

- A fresh harvest retrieves the current catalog and all accessible metadata covered by the implemented source retrieval paths, with explicit outcomes for inaccessible or unresolved cases.
- An unchanged subsequent run avoids unnecessary full-metadata retrieval and produces no duplicate records, links or mapping entries.
- A changed dataset replaces its current metadata; a new dataset is added; a removed dataset and its obsolete relationships are removed after a successful complete membership check.
- A failed run neither advances the successful sync boundary nor removes records merely because they were not reached.
- Confirmed indirect mappings are reused, updated or removed according to current membership and identity evidence.
- Published metadata is JSON-compatible, contains current records only, and respects the chosen GitHub publication limits.
- The scheduled workflow restores the minimal previous state, performs the harvest and publishes the current result successfully.
- Migration preserves current coverage and existing downloaded files while removing the explicitly unwanted stored data.

## Implementation status

The new implementation lives in `harvester/`, with executable commands in `scripts/` and workflows in `.github/`. It runs directly through `uv run python -m scripts.sync`; it is not built or installed as a library. The existing `src/researchdata_harvester` remains unchanged as reference and is not imported by the new code.

Current JSON files live in `data/current/`. The local migration retains and verifies downloaded files separately. Completion of migration, live acceptance and GitHub workflow execution must be established by their actual results, not inferred from the presence of code.
