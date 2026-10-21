# SPDX-License-Identifier: NOASSERTION
# Copyright (c) 2026 NordicIntel
"""RDF vocabulary mappings and the shared, gzipped JSON metadata cache.

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
Cache usage (inside the async harvest, with its shared HTTP client):

    cache = RdfCache.load(Path("data/current/rdf-cache.json.gz"))
    entry = await cache.fetch(uri, client, kind="entity", required=True)
    cache.save(Path("data/work/candidate/rdf-cache.json.gz"), referenced={uri})

load() starts empty if the file is absent; save() creates it and its directory.
Neither importing nor loading a missing cache writes to disk. Save only a
successful candidate, then publish it with the tables and manifest. An unchanged
save leaves the existing file untouched. Corrupt caches fail explicitly.

fetch() accepts JSON metadata objects (including RDF/JSON and JSON-LD) without
altering terms, language tags, datatypes, arrays or linked nodes. For other RDF
representations, pass a decoder returning a JSON-compatible dictionary; the
source parser owns RDF conversion and deterministic blank-node identities.
source_url can point to a metadata API or vocabulary document instead of the
entity URI. Use one cache key per shared vocabulary document, not one copy of
an entire ontology per term. No recursive crawling is performed here.

Vocabulary entries are reused unless full=True or their prior retrieval failed.
Mutable entities are conditionally requested once per cache instance/run, only
when a caller requests them. Without validators, fetch the complete response.
The supplied HTTP client owns transport configuration/retries; this module adds
no competing retry layer. Failures retain prior data with outcome='unavailable'
and stale=True. required=True raises CacheFetchError instead of allowing the
caller to treat that outcome as fresh metadata. Failed entries retry next run.

Parallel jobs load the same read-only baseline and serialize changes() into
their temporary artifacts. The build job loads the baseline, merge()s all job
updates (conflicts fail), then save()s once. Pass the retained entity URI set as
referenced to remove unused mutable entries while keeping vocabulary definitions.
No raw XML/RDF strings, observation data or retrieval-time history are stored.

Sources:
    https://docs.dataportal.se/registry/api/
    https://www.w3.org/TR/vocab-dcat-3/
    https://www.dublincore.org/specifications/dublin-core/dcmi-terms/
    https://www.w3.org/TR/vcard-rdf/
    https://docs.researchdata.se/metadata/schema-org-json-ld/
"""

import asyncio
import gzip
import io
import json
import logging
import os
import tempfile
from collections.abc import Callable, Collection, Mapping
from pathlib import Path
from typing import Literal

import httpx
from pydantic import BaseModel, ConfigDict, JsonValue, TypeAdapter, model_validator

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

# ------------------------------------------------------------------------------
# Shared RDF metadata cache
# ------------------------------------------------------------------------------

logger = logging.getLogger(__name__)


class CacheEntry(BaseModel):
    """One source description and the outcome of its latest attempted refresh."""

    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)

    kind: Literal["vocabulary", "entity"]
    source_url: str
    data: dict[str, JsonValue] | None = None
    etag: str | None = None
    last_modified: str | None = None
    outcome: Literal["available", "unavailable"] = "available"
    error: str | None = None

    @model_validator(mode="after")
    def require_available_data(self) -> "CacheEntry":
        """Reject a successful cache entry that contains no representation."""
        if self.outcome == "available" and self.data is None:
            raise ValueError("An available cache entry must contain metadata")
        return self

    @property
    def stale(self) -> bool:
        """Whether a failed refresh is retaining an earlier source description."""
        return self.outcome == "unavailable" and self.data is not None


class CacheFetchError(RuntimeError):
    """A required source could not be refreshed; publication must not proceed."""


_ENTRIES = TypeAdapter(dict[str, CacheEntry])
_METADATA = TypeAdapter(dict[str, JsonValue])


class RdfCache:
    """Keep a run's shared lookups in memory and explicitly save a candidate."""

    def __init__(self) -> None:
        self._entries: dict[str, CacheEntry] = {}
        self._baseline: dict[str, CacheEntry] = {}
        self._fetched: dict[str, str] = {}
        self._locks: dict[str, asyncio.Lock] = {}

    @classmethod
    def load(cls, path: Path) -> "RdfCache":
        """Read a URI-keyed gzip JSON file; missing means empty, corrupt raises."""
        cache = cls()
        if path.exists():
            with gzip.open(path, "rt", encoding="utf-8") as stream:
                cache._entries = _ENTRIES.validate_python(json.load(stream))
            cache._baseline = cache._entries.copy()
        return cache

    async def fetch(
        self,
        uri: str,
        client: httpx.AsyncClient,
        *,
        kind: Literal["vocabulary", "entity"] = "entity",
        source_url: str | None = None,
        full: bool = False,
        required: bool = False,
        decoder: Callable[[httpx.Response], dict[str, JsonValue]] | None = None,
    ) -> CacheEntry:
        """Retrieve/revalidate once per run; share concurrent requests by URI.

        A decoder should raise ValueError for malformed or unsupported metadata.
        Unexpected programming errors propagate. Returned entries are independent
        copies so a consumer cannot mutate metadata held by another consumer.
        """
        async with self._locks.setdefault(uri, asyncio.Lock()):
            previous = self._entries.get(uri)
            url = source_url or (previous.source_url if previous else uri)
            if previous and previous.kind != kind:
                raise ValueError(f"Cache kind changed for {uri}; use a consistent key.")
            if uri in self._fetched and self._fetched[uri] != url:
                raise ValueError(f"Conflicting metadata URLs for {uri} in one run.")
            reusable = (
                previous is not None
                and previous.source_url == url
                and previous.outcome == "available"
                and previous.data is not None
                and kind == "vocabulary"
                and not full
            )
            if uri not in self._fetched and not reusable:
                headers = {
                    "Accept": "application/rdf+json, application/ld+json, application/json"
                }
                if (
                    previous
                    and previous.data is not None
                    and previous.source_url == url
                ):
                    if previous.etag:
                        headers["If-None-Match"] = previous.etag
                    if previous.last_modified:
                        headers["If-Modified-Since"] = previous.last_modified
                try:
                    response = await client.get(
                        url, headers=headers, follow_redirects=True
                    )
                    if response.status_code == 304:
                        if (
                            not previous
                            or previous.data is None
                            or not (
                                "If-None-Match" in headers
                                or "If-Modified-Since" in headers
                            )
                        ):
                            raise ValueError(
                                "Received 304 without a cached representation"
                            )
                        entry = previous.model_copy(
                            update={
                                "outcome": "available",
                                "error": None,
                                "etag": response.headers.get("etag", previous.etag),
                                "last_modified": response.headers.get(
                                    "last-modified", previous.last_modified
                                ),
                            }
                        )
                    else:
                        response.raise_for_status()
                        data = _METADATA.validate_python(
                            decoder(response) if decoder else response.json()
                        )
                        entry = CacheEntry(
                            kind=kind,
                            source_url=url,
                            data=data,
                            etag=response.headers.get("etag"),
                            last_modified=response.headers.get("last-modified"),
                        )
                    logger.debug("RDF cache refresh: HTTP %s", response.status_code)
                except (httpx.HTTPError, ValueError) as error:
                    reason = (
                        f"HTTP {error.response.status_code}"
                        if isinstance(error, httpx.HTTPStatusError)
                        else type(error).__name__
                    )
                    entry = (
                        previous
                        or CacheEntry(kind=kind, source_url=url, outcome="unavailable")
                    ).model_copy(update={"outcome": "unavailable", "error": reason})
                    logger.debug("RDF cache refresh failed: %s", reason)
                self._entries[uri] = entry
                self._fetched[uri] = url
            entry = self._entries[uri]
            if required and entry.outcome != "available":
                raise CacheFetchError(f"Required RDF resource {uri}: {entry.error}")
            return entry.model_copy(deep=True)

    def changes(self) -> dict[str, dict[str, JsonValue]]:
        """Return JSON-compatible updates for a fetch job's temporary artifact."""
        return {
            uri: entry.model_dump(mode="json")
            for uri, entry in sorted(self._entries.items())
            if entry != self._baseline.get(uri)
        }

    def merge(self, *updates: Mapping[str, object]) -> None:
        """Merge worker updates atomically; differing updates for one URI fail."""
        pending = _ENTRIES.validate_python(self.changes())
        for update in updates:
            for uri, entry in _ENTRIES.validate_python(update).items():
                if uri in pending and pending[uri] != entry:
                    raise ValueError(f"Conflicting RDF cache updates for {uri}")
                pending[uri] = entry
        self._entries.update(pending)

    def save(self, path: Path, *, referenced: Collection[str] | None = None) -> None:
        """Create/replace a candidate atomically with deterministic gzip JSON.

        Omit referenced to retain all entries, including when writing an empty
        initial cache. Supply it during the final build to prune unused entities.
        """
        entries = {
            uri: entry.model_dump(mode="json")
            for uri, entry in self._entries.items()
            if referenced is None or uri in referenced or entry.kind == "vocabulary"
        }
        payload = (
            json.dumps(
                entries,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            ).encode("utf-8")
            + b"\n"
        )
        buffer = io.BytesIO()
        with gzip.GzipFile(
            filename="", mode="wb", fileobj=buffer, mtime=0, compresslevel=9
        ) as archive:
            archive.write(payload)
        compressed = buffer.getvalue()
        if path.exists() and path.read_bytes() == compressed:
            return
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary: Path | None = None
        try:
            with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as stream:
                temporary = Path(stream.name)
                stream.write(compressed)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, path)
        finally:
            if temporary is not None:
                temporary.unlink(missing_ok=True)
