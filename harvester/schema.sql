-- SPDX-License-Identifier: NOASSERTION
-- Copyright (c) 2026 NordicIntel
-- Current-catalog output contract. Execute once in a fresh DuckDB connection:
-- connection.execute(Path("harvester/schema.sql").read_text(encoding="utf-8"))
-- Insert named columns; omitted collections default to []. Export each table
-- ordered by dataset_uri with COPY (... ) TO ... (FORMAT PARQUET, COMPRESSION ZSTD).
-- There are exactly two output tables, with the same Dataportalen dataset_uri
-- membership. Excluded/unresolved candidates belong in inventory.json instead.
-- No historical records, raw response payloads or sampled observations belong here.
--
-- All descriptive projections retain their source evidence. Source values must
-- also be retained in properties, including known predicates and linked entities;
-- projections never replace the complete metadata. Preserve conflicting values.
-- Blank-node IDs must be deterministic and scoped to source_url. The output
-- builder owns canonicalization, reference checks and cross-table validation.
--
-- Source dates remain lexical metadata values (including partial/invalid dates).
-- Only operational timestamps and the parsed registry change signal use TIMESTAMPTZ.
-- On reload, INSERT Parquet rows into these declared tables to restore constraints;
-- Parquet itself does not enforce keys, defaults or CHECK constraints.
-- Query publication_errors before export and reject a nonempty result. DuckDB
-- cannot express cross-table membership or nested list predicates in CHECKs.

BEGIN TRANSACTION;

-- -----------------------------------------------------------------------------
-- Shared metadata structures
-- -----------------------------------------------------------------------------

-- RDF terms use uri/bnode/literal. Non-RDF JSON uses string/number/boolean/null/
-- object/array, with scalar lexical values and NULL values for container markers.
-- path is an RFC 6901 JSON pointer or an indexed, namespace-expanded XML path;
-- it preserves array order, XML attributes and repeated elements. It is NULL for
-- RDF statements. property is the exact expanded predicate/element/attribute name.
-- JSON containers (including empty ones) get markers; no serialized JSON/XML
-- subtrees are placed in value. graph_id is NULL for the default RDF graph.
CREATE TYPE metadata_value AS STRUCT (
    value VARCHAR,
    value_type VARCHAR,
    language VARCHAR,
    datatype VARCHAR,
    source_url VARCHAR,
    entity_id VARCHAR,
    graph_id VARCHAR,
    property VARCHAR,
    path VARCHAR
);

CREATE TYPE agent_metadata AS STRUCT (
    entity_id VARCHAR,
    source_url VARCHAR,
    names metadata_value[],
    identifiers metadata_value[],
    urls metadata_value[],
    emails metadata_value[]
);

-- Typed projections of source-advertised distributions, not proof of download
-- access. All advertised size/checksum values stay lexical and source-attributed.
CREATE TYPE distribution_metadata AS STRUCT (
    entity_id VARCHAR,
    source_url VARCHAR,
    titles metadata_value[],
    descriptions metadata_value[],
    access_urls metadata_value[],
    download_urls metadata_value[],
    media_types metadata_value[],
    formats metadata_value[],
    byte_sizes metadata_value[],
    checksums metadata_value[],
    access_rights metadata_value[],
    rights metadata_value[],
    licenses metadata_value[],
    issued metadata_value[],
    modified metadata_value[],
    access_services metadata_value[]
);

CREATE TYPE service_metadata AS STRUCT (
    entity_id VARCHAR,
    source_url VARCHAR,
    titles metadata_value[],
    descriptions metadata_value[],
    endpoint_urls metadata_value[],
    endpoint_descriptions metadata_value[],
    serves_datasets metadata_value[],
    conforms_to metadata_value[],
    access_rights metadata_value[],
    licenses metadata_value[]
);

-- Only the latest successful bounded inspection, never the sample observations.
-- Keep this evidence after a transient failure; outcome/reason/checked_at on the
-- resource describe the latest attempt independently. Limits are configured by
-- the verifier, not baked into the storage schema.
CREATE TYPE verification_evidence AS STRUCT (
    verified_at TIMESTAMPTZ,
    resolved_url VARCHAR,
    format VARCHAR,
    media_type VARCHAR,
    access_method VARCHAR,
    sampled_rows UINTEGER,
    columns STRUCT(name VARCHAR, data_type VARCHAR)[],
    transferred_bytes UBIGINT,
    decompressed_bytes UBIGINT,
    license_acknowledgement_url VARCHAR
);

CREATE TYPE data_resource AS STRUCT (
    url VARCHAR,
    source_url VARCHAR,
    entity_id VARCHAR,
    distribution_id VARCHAR,
    filename VARCHAR,
    outcome VARCHAR,
    reason VARCHAR,
    checked_at TIMESTAMPTZ,
    verification verification_evidence
);

-- Associations describe current identities, including exact revision suffixes.
-- rule identifies the confirmed matching method; evidence holds the actual
-- compared source assertions. Unconfirmed associations stay in the inventory.
CREATE TYPE researchdata_association AS STRUCT (
    identifier VARCHAR,
    landing_page VARCHAR,
    rule VARCHAR,
    evidence metadata_value[]
);

-- One current retrieval outcome per metadata resource. Failed external refreshes
-- retain prior properties with stale=true; they must not acquire a new success
-- timestamp. HTTP validators and source change indicators live in the inventory
-- and RDF cache, not a second operational-state table.
CREATE TYPE source_outcome AS STRUCT (
    url VARCHAR,
    outcome VARCHAR,
    stale BOOLEAN,
    reason VARCHAR
);

-- -----------------------------------------------------------------------------
-- Complete corresponding Dataportalen records
-- -----------------------------------------------------------------------------

CREATE TABLE dataportalen (
    dataset_uri VARCHAR PRIMARY KEY CHECK (length(trim(dataset_uri)) > 0),
    registry_entry_uri VARCHAR NOT NULL,
    metadata_url VARCHAR NOT NULL,
    registry_modified TIMESTAMPTZ NOT NULL,
    registry_modified_value VARCHAR NOT NULL,
    identifiers metadata_value[] NOT NULL DEFAULT [],
    titles metadata_value[] NOT NULL DEFAULT [],
    descriptions metadata_value[] NOT NULL DEFAULT [],
    publishers agent_metadata[] NOT NULL DEFAULT [],
    creators agent_metadata[] NOT NULL DEFAULT [],
    contacts agent_metadata[] NOT NULL DEFAULT [],
    subjects metadata_value[] NOT NULL DEFAULT [],
    keywords metadata_value[] NOT NULL DEFAULT [],
    languages metadata_value[] NOT NULL DEFAULT [],
    temporal metadata_value[] NOT NULL DEFAULT [],
    spatial metadata_value[] NOT NULL DEFAULT [],
    access_rights metadata_value[] NOT NULL DEFAULT [],
    rights metadata_value[] NOT NULL DEFAULT [],
    licenses metadata_value[] NOT NULL DEFAULT [],
    landing_pages metadata_value[] NOT NULL DEFAULT [],
    issued metadata_value[] NOT NULL DEFAULT [],
    modified metadata_value[] NOT NULL DEFAULT [],
    distributions distribution_metadata[] NOT NULL DEFAULT [],
    services service_metadata[] NOT NULL DEFAULT [],
    properties metadata_value[] NOT NULL DEFAULT []
);

-- -----------------------------------------------------------------------------
-- Eligible datasets with Researchdata and external enrichment
-- -----------------------------------------------------------------------------

CREATE TABLE datasets (
    dataset_uri VARCHAR PRIMARY KEY CHECK (length(trim(dataset_uri)) > 0),
    researchdata researchdata_association[] NOT NULL DEFAULT [],
    identifiers metadata_value[] NOT NULL DEFAULT [],
    titles metadata_value[] NOT NULL DEFAULT [],
    descriptions metadata_value[] NOT NULL DEFAULT [],
    publishers agent_metadata[] NOT NULL DEFAULT [],
    creators agent_metadata[] NOT NULL DEFAULT [],
    contacts agent_metadata[] NOT NULL DEFAULT [],
    subjects metadata_value[] NOT NULL DEFAULT [],
    keywords metadata_value[] NOT NULL DEFAULT [],
    languages metadata_value[] NOT NULL DEFAULT [],
    temporal metadata_value[] NOT NULL DEFAULT [],
    spatial metadata_value[] NOT NULL DEFAULT [],
    access_rights metadata_value[] NOT NULL DEFAULT [],
    rights metadata_value[] NOT NULL DEFAULT [],
    licenses metadata_value[] NOT NULL DEFAULT [],
    landing_pages metadata_value[] NOT NULL DEFAULT [],
    issued metadata_value[] NOT NULL DEFAULT [],
    modified metadata_value[] NOT NULL DEFAULT [],
    distributions distribution_metadata[] NOT NULL DEFAULT [],
    services service_metadata[] NOT NULL DEFAULT [],
    resources data_resource[] NOT NULL CHECK (len(resources) > 0),
    availability VARCHAR NOT NULL CHECK (availability IN ('available', 'unavailable')),
    sources source_outcome[] NOT NULL DEFAULT [],
    properties metadata_value[] NOT NULL DEFAULT []
);

-- New metadata-only records cannot enter the published dataset table. A retained
-- unavailable record must still carry its previous success evidence. This view
-- is validation only; it is not a third output table or a persisted cache.
CREATE VIEW publication_errors AS
SELECT coalesce(d.dataset_uri, p.dataset_uri) AS dataset_uri,
    'table_membership_mismatch' AS reason
FROM datasets d FULL OUTER JOIN dataportalen p USING (dataset_uri)
WHERE d.dataset_uri IS NULL OR p.dataset_uri IS NULL
UNION ALL
SELECT dataset_uri, 'no_verified_tabular_resource'
FROM datasets
WHERE len(list_filter(resources, lambda r:
    r.verification.verified_at IS NOT NULL
    AND r.verification.sampled_rows > 0
    AND len(r.verification.columns) > 0
)) = 0
UNION ALL
SELECT dataset_uri, 'available_without_current_verification'
FROM datasets
WHERE availability = 'available' AND len(list_filter(resources, lambda r:
    r.outcome = 'verified'
    AND r.verification.verified_at IS NOT NULL
    AND r.verification.sampled_rows > 0
    AND len(r.verification.columns) > 0
)) = 0;

COMMIT;
