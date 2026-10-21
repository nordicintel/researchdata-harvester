# Researchdata harvester

An independent Python 3.12+ package and CLI for the **current Researchdata catalog**,
including restricted datasets. It stores metadata in SQLite and publishes separate
JSONL snapshots for Researchdata records, representations, Dataportalen records,
evidence links and files. It contains no server and imports no parent application.
SQLite is the supported backend. PostgreSQL is not implemented in this tool.

Source documentation is available for [Researchdata's OAI-PMH API](https://docs.researchdata.se/api/),
[landing-page JSON-LD](https://docs.researchdata.se/metadata/schema-org-json-ld/),
and [Dataportalen's SPARQL service](https://docs.dataportal.se/dcat/docs/sparql/).
The live endpoints are [OAI-PMH](https://api.researchdata.se/oai-pmh),
the [catalog sitemap](https://researchdata.se/catalogue/sitemap.xml), and
[SPARQL](https://admin.dataportal.se/sparql).

## Install and run

Copy this entire folder anywhere, then run commands from that folder. Manage the
installation with uv. `config.toml` is the editable configuration example and the
default configuration in a source checkout. Paths inside it resolve relative to
that file. Pass `--config /absolute/path/config.toml` when installing a wheel or
running from another working directory. No environment file is used or read.

```powershell
uv sync
uv run researchdata-harvester sync --full
uv run researchdata-harvester sync --incremental
uv run researchdata-harvester record oai:researchdata.se:2026-265/1
uv run researchdata-harvester search "environmental"
uv run researchdata-harvester files oai:researchdata.se:2026-265/1
uv run researchdata-harvester download oai:researchdata.se:2026-265/1 --destination data/downloads/example
uv run researchdata-harvester export --destination data/exports
uv run researchdata-harvester status
```

Global options precede the subcommand. For example,
`uv run researchdata-harvester --concurrency 4 --formats datacite --formats json_ld sync --full`.
The CLI writes stage/page progress to stderr and structured JSON to stdout.
The default `data/` directory, downloads and exports are gitignored.

## Python interface

```python
import asyncio
from pathlib import Path
from researchdata_harvester import Config, Harvester


async def main():
    async with Harvester(Config.load(Path("config.toml")), progress=print) as harvester:
        await harvester.sync(full=True)
        results = harvester.store.search("water")
        print(results)
        # Fetch only this explicitly requested version, without walking history.
        item = await harvester.fetch_record("oai:researchdata.se:2026-265/1")
        print(item["identifier"])
        snapshot = harvester.store.export(Path("data/exports"))
        print(snapshot)


asyncio.run(main())
```

`harvester.oai` exposes `identify`, `list_metadata_formats`, `list_sets`,
`list_identifiers`, `list_records` and `get_record`. Listing methods return a
`Page` with `items`, opaque `token`, optional reported `total` and original page
XML. ListRecords items are typed `Record` objects. `iter_records`,
`iter_identifiers`, `iter_sets` and `iter_pages` traverse complete streams and
reject repeated tokens. Record listings accept `metadata_prefix`, `set_spec`,
`from_`, `until` and `resumption_token`. The token is sent alone with the verb.
Arbitrary advertised metadata prefixes can be requested on demand.

`researchdata_harvester.oai.build_file_url(dataset_id, version, filename)` is a
safe construction helper for the undocumented file API. A constructed URL is
unverified. Downloads use only URLs actually advertised by source metadata.

## Harvesting and recovery

Each sync discovers the repository description, available formats and complete
set hierarchy. The default independently paginated streams are `datacite`,
`dcat` and `json_ld`. Original response XML is retained in `source_pages`.
Per-record XML, namespace-expanded ordered XML trees, repeated multilingual
fields, attributes and a SHA-256 content hash are stored per representation.
JSON-LD CDATA is parsed without flattening its original XML representation.

Each page and its next cursor are committed together. Replays upsert the same
record/format. An interrupted command resumes the latest incomplete run, using
its original interval and endpoint/format scope. An expired OAI token restarts
that same interval, at most twice. Repeated tokens, malformed responses or
exhausted retries leave the run incomplete. Run a later full sync periodically.
An incremental sync starts one day before the last successful **sync** boundary;
a single on-demand record fetch cannot advance that boundary.

Every sync also scans a complete, unfiltered `ListIdentifiers(oai_dc)` inventory,
independently of the selected metadata streams. This controls current version
membership even when an incremental metadata feed omits disappearing versions.
On-demand historical fetches preserve existing catalog presence.

Full scan presence is independent per source and per OAI format. Absence is
updated only after the relevant unfiltered scan completes. Absence does not set
a deletion marker. Only an explicit OAI deleted header does that. Encountered
versions remain stored; on-demand historical records do not enter current catalog
matching. This tool never recursively retrieves older versions.

The sitemap reader accepts plain/namespaced URL sets and same-host sitemap
indexes, follows each index once and counts collections separately. It compares
actual sitemap dataset IDs against OAI dataset IDs. Missing representations use
GetRecord. If a sitemap-only dataset cannot reveal an explicit current version
in its landing JSON-LD or same-host metadata export links, the scan fails visibly
instead of guessing a version. Recovered records remain current while the original
inventory discrepancy remains recorded.
Differences and source counts remain in the reconciliation checkpoint.

When JSON-LD supplies no file URLs, bounded landing discovery follows explicit
landing pages and DOI URLs, with at most eight HTTP redirects and three candidate
URLs per record. It reads JSON-LD only and does not crawl repository pages.
Individual discovery outcomes are checkpointed. A failed optional landing page
is reported as `landing_unavailable`; it does not turn a completed metadata
stream into a failed stream. Set `landing_fallback = false` to disable these
optional network requests explicitly.

## Dataportalen evidence

Every sync reads explicit Researchdata catalog membership from the configured
[EntryStore metadata resource](https://admin.dataportal.se/store/818/metadata/9198).
The expected root is `https://researchdata.se/catalogue/`, typed `dcat:Catalog`.
The required `dcat:dataset` array provides the inventory. Missing or malformed
membership fails visibly; an explicit empty array is valid. The sorted unique
members and source evidence are saved in a per-run inventory checkpoint before
paging. Interrupted scans resume that snapshot without refetching membership.
Identity fields are fetched in bounded batches, including identifier notation
inside ADMS blank nodes. Counts distinguish scanned, matched, unresolved catalog
members and records with no additional identifier. The URI is identity evidence. An anonymous
record can retain `no_identifier` while matching through compound metadata evidence.

`dataportalen_scope = "researchdata"` is the default. For an explicit global audit,
use `--dataportalen-scope all` before the subcommand, or change that TOML setting.
That mode scans all ordered Dataportalen dataset URIs using keyset pagination.
It also snapshots Researchdata membership and enriches those members, so a global
audit preserves metadata representation links under the same trusted scope.
Resuming requires the original source URLs, formats and scope. Completed global
runs remain valid history; an incompatible unfinished run cannot resume filtered.

Successful scans publish `in_scope` atomically. Historical global rows are retained,
and exclusion from this catalog does not mark them absent. Their previous
`present` value remains. Matching summaries include only present rows in scope.
Status reports total retained `dataportalen_records` separately from
`dataportalen_in_scope`. Exports retain every historical row with its `in_scope`
flag. A failed scan preserves previous scope flags; new rows start outside scope
until completion.

Matching uses normalized DOI forms and exact explicit identifiers/URLs from
harvested Researchdata metadata. General URLs preserve scheme, path encoding,
query and fragment. Citation/reference DOIs are excluded. Primary DOI matches
are version relations; explicit DataCite `IsVersionOf` identifiers give dataset
relations. All matching evidence is retained. Multiple versions may legitimately
link to one dataset-level identity. Direct identity evidence linking different Researchdata
dataset IDs is marked ambiguous. Titles alone never establish identity, and a shared
file download/access URL alone does not establish dataset equivalence.

After full metadata retrieval, a second matcher considers only verified catalog
members without direct identity matches and current, nondeleted source DCAT
records whose top-level dataset is anonymous. An exact language-tagged title
selects candidates. `exact_dcat_fields` requires every direct literal/URI source
fact to occur on the Dataportalen root, including descriptions, an explicit
publisher URI and nonempty dates. Language, datatype and original lexical values
are compared exactly. Source temporal periods must also agree. Nested contacts,
authors and publisher enrichment do not substitute for root evidence.

`corroborated_self_citation` permits a changed description only when all other
source root facts and temporal periods agree. The source dataset's own primary
HTTP URL, from DataCite `resource/identifier` or JSON-LD Dataset `identifier`, must
occur as a whole URL token after the exact title in the same description
paragraph. Root title and publisher must corroborate it. Related publications,
citation identifiers, download links, URL prefixes and titles alone cannot
supply this evidence. Exact field matches take precedence.
For JSON-LD graphs, the top-level DataCite primary identifier must identify one
Dataset node before its identifiers can support a citation. DOI resolver variants
are normalized for that ownership check. Without a DataCite anchor, only a single
Dataset node is accepted; related Dataset nodes never supply primary identifiers.

These links use `catalog_representation`, rather than declaring global dataset
equivalence. A catalog entry may legitimately represent multiple distinctive
source records. Candidates with indistinguishable supporting facts are marked
ambiguous. Evidence retains compared facts, temporal qualifiers, the source DCAT
content hash and source identifier, catalog snapshot provenance, and any primary
URL and citation text used. The original direct identity ambiguity rules remain.
The matching policy is part of the resume scope, so incompatible unfinished runs
cannot silently mix matching policies or counters.

Full RDF metadata is retrieved for every Researchdata catalog member, including
unmatched members, in batches of up to 50. Global audits enrich trusted catalog members, matched records
and explicit unresolved Researchdata references. The endpoint's DESCRIBE response
provides one graph containing root triples, known publisher/contact/distribution/
service/provenance relationships and blank-node descendants. RDF term type,
language, datatype, original literal spelling and graph relationships are
preserved. The endpoint must return the requested N-Triples format; other RDF
formats fail explicitly rather than normalize source values. Invalid typed
literals retain their original value and datatype with `ill_typed: true`; one
warning summarizes their count per response. Blank IDs are scoped to the response. Missing blank-node descriptions fail enrichment explicitly.
DESCRIBE behavior is endpoint-defined; unrelated named-resource graphs are not
crawled. No catalog membership or named graph is inferred from a dataset URL.
Researchdata and Dataportalen payloads remain separate and links carry the rule,
relation, status and exact evidence.

## Files and exports

Downloads are explicit CLI/Python operations, never a side effect of sync.
Use `download <identifier> --url <advertised-url>` to select an individual file;
repeat `--url` for several files, or omit it for all current files in that record.
The Python equivalent is `harvester.download(identifier, urls=[url])`.
Advertised names/subpaths are checked for traversal, unsafe components and
Windows reserved names. Bytes stream to a temporary file, then advertised size
and SHA-256 are checked before atomic publication without replacing an existing
file. Existing matching files are reused only with advertised integrity evidence.
Different existing content is a conflict. Interrupted transfers discard partial
files and may retry. HTTP access restrictions, unavailable URLs, checksum failure
and metadata completion are separate outcomes. File entries retain history and an independent current-presence flag. Refreshed
JSON-LD retires removed URLs; file listing and download use current URLs by default.

A download without advertised size
or hash is labelled `downloaded_unverified`. HTML returned for an advertised data
format is rejected. Access to restricted files is not bypassed.

Exports use a consistent SQLite read transaction and stable row order. Each
immutable snapshot directory contains `researchdata.jsonl`, `representations.jsonl`,
`dataportalen.jsonl`, `links.jsonl`, `files.jsonl`, `sitemap.jsonl` and `manifest.json`.
`CURRENT` atomically selects the completed generation. Readers should read
`CURRENT` once and then open that directory. Previous generations remain for
readers; users manage their retention. JSON-bearing SQLite columns are declared
`JSON`. The packaged schema is `src/researchdata_harvester/schema.sql`.

Use an external scheduler. Avoid overlapping processes writing the same database.
There is no built-in daemon or scheduler.

## Validation

```powershell
uv run python -m pytest
uv run ruff check .
uv run ruff format --check .
```

Default tests use deterministic HTTP fixtures. On 2026-10-21, all 95 package tests
passed, including from a separately installed copy outside the parent repository.
Wheel and source distribution builds, Ruff checks, the parent
repository's 107 tests and its 207-entry API audit also passed.

The catalog-scoped live sync with compound metadata pairing produced these results
on 2026-10-21:

| Check                                  | Observed result                                                               |
| -------------------------------------- | ----------------------------------------------------------------------------- |
| Current Researchdata catalog           | 6,691 records, reconciled against OAI identifiers and sitemap dataset entries |
| Dataportalen catalog                   | All 6,626 members retrieved with full metadata and linked; none unresolved    |
| Researchdata with a confirmed link     | 6,688                                                                         |
| Researchdata with only ambiguous links | 2                                                                             |
| Researchdata without any link          | 1                                                                             |
| Evidence links                         | 6,692; all 5,041 earlier links retained and 1,651 added                       |
| Added matching evidence                | 1,556 exact DCAT field matches and 95 corroborated primary self-citations     |
| Combined Dataportalen entries          | 50 entries represent multiple distinctive Researchdata records                |

The CLI summary gives ambiguity precedence and reports 6,686 matched, 4 ambiguous
and 1 unmatched. Two of those four also have confirmed representation links.
The ambiguous DOI links involve two Researchdata records per DOI, for
`10.17197/SUMFOLKDB_V01_03` and `10.17197/FOLKNETDB_V01`. Their separate source
records and conflicting identity evidence are retained.

The only unlinked record is `oai:researchdata.se:2026-265/1`,
[the saproxylic beetle diversity dataset](https://doi.org/10.5878/f467-k877),
with source datestamp 2026-10-20. Live Dataportalen queries for its version DOI,
dataset DOI and exact titles returned no record. This does not establish why
Dataportalen lacks it.

Two successive pairing syncs retained the same 6,692 links without duplicate
growth in any exported entity. The final run used matching policy
`anonymous-catalog-representation-v2`. SQLite integrity and foreign-key checks
passed, all JSONL identities/counts/relationships agreed with SQLite, and the
eight existing downloads still matched their advertised SHA-256 checksums.
The ignored report is `data/pairing-acceptance.json`; the database remains
`data/catalog.sqlite` and `data/exports/CURRENT` selects the verified export.
All 17,114 historical Dataportalen records outside Researchdata membership were
retained with unchanged presence flags and `in_scope = 0`.

The historical global-scan baseline passed a complete live full sync and
subsequent incremental sync on 2026-10-21, before catalog filtering was added:

| Check                        | Observed result                                                                                                |
| ---------------------------- | -------------------------------------------------------------------------------------------------------------- |
| Current Researchdata catalog | 6,691 identifiers, exactly matching independent OAI and sitemap inventories                                    |
| Metadata representations     | 20,073, comprising all 6,691 records in each default format                                                    |
| Sitemap collections          | 63, counted separately from datasets                                                                           |
| Dataportalen inventory       | 23,740 records scanned in each run; 5,039 matched catalog entries                                              |
| Researchdata matching        | 5,037 unambiguous, 4 ambiguous, 1,650 without a match                                                          |
| Evidence links               | 5,041, including the four ambiguous links                                                                      |
| File discovery               | 23,436 advertised file associations across 2,682 records                                                       |
| Optional landing failures    | 221, recorded separately from successful catalog enumeration                                                   |
| Downloads                    | Eight real files verified against advertised SHA-256 and reused without modification                           |
| Recovery                     | Interrupted full sync resumed the same run and retained the complete catalog                                   |
| Second sync                  | No duplicate growth in records, representations, links, files or sitemap entries                               |
| SQLite and JSONL             | Integrity check passed, no foreign-key violations, and exported identities/counts/relationships matched SQLite |

The local acceptance artifacts are `data/catalog.sqlite`, the generation selected
by `data/exports/CURRENT`, and `data/acceptance.json`; all are gitignored.
The global baseline recorded 2,134 malformed RDF literal occurrences in
964 Dataportalen records, preserved verbatim and flagged rather than repaired.
These are observations from that run, not fixed catalog totals or promises that
all research files are openly downloadable.

Before compound metadata pairing was introduced, a live incremental sync with
the default catalog filter also completed on
2026-10-21. It read 6,626 catalog members, retrieved full metadata for every member,
and retained all 5,039 previously matched Dataportalen records and 5,041 links.
The other 1,587 catalog members lacked matching identity fields and remained
explicitly unresolved under the then identity-only matching policy. The 17,114 records outside the catalog were retained without
refreshing them or changing their presence flags. SQLite integrity, foreign keys,
and the new JSONL export passed validation. The ignored comparison report is
`data/catalog-scope-acceptance.json`.

Source data can change during a scan. OAI intervals and replay protect updates,
but Dataportalen metadata and sitemap endpoints do not offer a transaction shared
across pages. Catalog membership is frozen per run; a later sync refreshes it.
