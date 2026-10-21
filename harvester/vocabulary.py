# SPDX-License-Identifier: NOASSERTION
# Copyright (c) 2026 NordicIntel
"""Checked-in RDF names and projection mappings for schema.sql.

Use PREDICATE_COLUMNS[context].get(expanded_predicate) to find a descriptive
column. Contexts are dataset (both tables), agent, distribution and service.
Values target columns or STRUCT fields declared in schema.sql. Scalar metadata
is always a list of metadata_value objects; agent/distribution/service relations
must be resolved into their typed structures by the normalizer. No schema is
duplicated in Python, and importing this module performs no I/O.

These are projections, not a whitelist: preserve EVERY source assertion in the
owning table's properties, including known predicates, linked entities, unknown
attributes, source paths and original RDF term spelling. Do not coerce source
dates, sizes or identifiers destructively. Keep RDF blank nodes scoped to their
source and canonicalize them before output. Namespace prefixes below are only
local conveniences; predicates in published metadata remain expanded names.

JSON-LD must be expanded using its actual context before lookup. Schema.org's
HTTP and HTTPS forms have explicit mapping aliases; this does not authorize
rewriting source URIs. DataCite XML and repository-specific JSON need their own
source parsers; unrecognized fields remain properties, never guessed RDF terms.

Registry info is a separate graph: REGISTRY_PREDICATES locates its resource,
metadata URL and modification time. Dataset dct:modified is descriptive metadata
and MUST NOT become the registry change signal. ACCESS_RIGHTS names terms only:
missing rights and PUBLIC do not prove access, and a distribution URL does not
prove tabular data. Eligibility still requires resource verification.

Only explicit equivalence predicates are listed for identity assertions.
Identifiers require context-aware comparison; titles, citations, dct:relation,
isVersionOf and previousVersion never establish identity through this module.
Fetched vocabulary labels/definitions belong in data/current/rdf-cache.json.gz,
alongside fetched shared entities; this file contains no cache implementation.

Sources:
    https://docs.dataportal.se/registry/api/
    https://www.w3.org/TR/vocab-dcat-3/
    https://www.dublincore.org/specifications/dublin-core/dcmi-terms/
    https://www.w3.org/TR/vcard-rdf/
    https://docs.researchdata.se/metadata/schema-org-json-ld/
"""

# ------------------------------------------------------------------------------
# Namespaces and normalization projections
# ------------------------------------------------------------------------------

NAMESPACES: dict[str, str] = {
    "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
    "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
    "xsd": "http://www.w3.org/2001/XMLSchema#",
    "dct": "http://purl.org/dc/terms/",
    "dcat": "http://www.w3.org/ns/dcat#",
    "foaf": "http://xmlns.com/foaf/0.1/",
    "vcard": "http://www.w3.org/2006/vcard/ns#",
    "skos": "http://www.w3.org/2004/02/skos/core#",
    "adms": "http://www.w3.org/ns/adms#",
    "owl": "http://www.w3.org/2002/07/owl#",
    "prov": "http://www.w3.org/ns/prov#",
    "spdx": "http://spdx.org/rdf/terms#",
    "schema": "https://schema.org/",
    "es": "http://entrystore.org/terms/",
}

_PROJECTIONS: dict[str, dict[str, str]] = {
    "dataset": {
        "dct:identifier": "identifiers",
        "adms:identifier": "identifiers",
        "dct:title": "titles",
        "dct:description": "descriptions",
        "dct:publisher": "publishers",
        "dct:creator": "creators",
        "dcat:contactPoint": "contacts",
        "dct:subject": "subjects",
        "dcat:theme": "subjects",
        "dcat:keyword": "keywords",
        "dct:language": "languages",
        "dct:temporal": "temporal",
        "dct:spatial": "spatial",
        "dct:accessRights": "access_rights",
        "dct:rights": "rights",
        "dct:license": "licenses",
        "dcat:landingPage": "landing_pages",
        "dct:issued": "issued",
        "dct:modified": "modified",
        "dcat:distribution": "distributions",
        "schema:identifier": "identifiers",
        "schema:name": "titles",
        "schema:description": "descriptions",
        "schema:publisher": "publishers",
        "schema:creator": "creators",
        "schema:contactPoint": "contacts",
        "schema:keywords": "keywords",
        "schema:inLanguage": "languages",
        "schema:temporalCoverage": "temporal",
        "schema:spatialCoverage": "spatial",
        "schema:license": "licenses",
        "schema:url": "landing_pages",
        "schema:datePublished": "issued",
        "schema:dateModified": "modified",
        "schema:distribution": "distributions",
    },
    "agent": {
        "foaf:name": "names",
        "rdfs:label": "names",
        "skos:prefLabel": "names",
        "vcard:fn": "names",
        "vcard:organization-name": "names",
        "dct:identifier": "identifiers",
        "adms:identifier": "identifiers",
        "foaf:homepage": "urls",
        "vcard:hasURL": "urls",
        "foaf:mbox": "emails",
        "vcard:hasEmail": "emails",
        "schema:name": "names",
        "schema:identifier": "identifiers",
        "schema:url": "urls",
        "schema:email": "emails",
    },
    "distribution": {
        "dct:title": "titles",
        "dct:description": "descriptions",
        "dcat:accessURL": "access_urls",
        "dcat:downloadURL": "download_urls",
        "dcat:mediaType": "media_types",
        "dct:format": "formats",
        "dcat:byteSize": "byte_sizes",
        "spdx:checksum": "checksums",
        "dct:accessRights": "access_rights",
        "dct:rights": "rights",
        "dct:license": "licenses",
        "dct:issued": "issued",
        "dct:modified": "modified",
        "dcat:accessService": "access_services",
        "schema:name": "titles",
        "schema:description": "descriptions",
        "schema:url": "access_urls",
        "schema:contentUrl": "download_urls",
        "schema:encodingFormat": "formats",
        "schema:contentSize": "byte_sizes",
        "schema:sha256": "checksums",
        "schema:license": "licenses",
        "schema:datePublished": "issued",
        "schema:dateModified": "modified",
    },
    "service": {
        "dct:title": "titles",
        "dct:description": "descriptions",
        "dcat:endpointURL": "endpoint_urls",
        "dcat:endpointDescription": "endpoint_descriptions",
        "dcat:servesDataset": "serves_datasets",
        "dct:conformsTo": "conforms_to",
        "dct:accessRights": "access_rights",
        "dct:license": "licenses",
    },
}

PREDICATE_COLUMNS: dict[str, dict[str, str]] = {
    context: {
        NAMESPACES[prefix] + local_name: column
        for name, column in fields.items()
        for prefix, local_name in [name.split(":", 1)]
    }
    for context, fields in _PROJECTIONS.items()
}
for _fields in PREDICATE_COLUMNS.values():
    _fields.update(
        {
            "http://schema.org/" + predicate.removeprefix(NAMESPACES["schema"]): column
            for predicate, column in list(_fields.items())
            if predicate.startswith(NAMESPACES["schema"])
        }
    )

# ------------------------------------------------------------------------------
# Registry fields, access-right terms and explicit identity assertions
# ------------------------------------------------------------------------------

REGISTRY_PREDICATES: dict[str, str] = {
    "dataset_uri": NAMESPACES["es"] + "resource",
    "metadata_url": NAMESPACES["es"] + "metadata",
    "registry_modified": NAMESPACES["dct"] + "modified",
}

ACCESS_RIGHTS: dict[str, str] = {
    "http://publications.europa.eu/resource/authority/access-right/" + name: name
    for name in ("PUBLIC", "NON_PUBLIC", "RESTRICTED")
}

IDENTITY_PREDICATES: frozenset[str] = frozenset(
    {
        NAMESPACES["owl"] + "sameAs",
        NAMESPACES["schema"] + "sameAs",
        "http://schema.org/sameAs",
    }
)
