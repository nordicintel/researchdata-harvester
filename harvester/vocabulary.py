# SPDX-License-Identifier: NOASSERTION
# Copyright (c) 2026 NordicIntel
"""Resolve RDF, JSON-LD, JSON Schema and OpenAPI definitions through one operation.

Use the harvest's shared, configured httpx client:

    async with SemanticResolver(cache_path, client, updates_path) as resolver:
        resolved = await resolver.resolve(metadata, source_url=metadata_url)

resolve() detects JSON-LD/RDF, JSON Schema with $schema, and OpenAPI 3.x JSON.
For schemas without $schema (including boolean schemas), pass format="json-schema".
Schema results retain complete schemas and explicit reference outcomes separately
from RDF properties. The referencing library resolves static $ref, JSON Pointers,
anchors and nested $id bases. Cycles remain references; examples and arbitrary URLs
are not fetched. OpenAPI component and inline schemas are collected. This is not
instance validation or OpenAPI request execution; dynamic/recursive evaluation
references are preserved with an explicit unsupported outcome. OpenAPI documents
must already be parsed JSON, not YAML. Cache files may retain their existing
rdf-cache.json.gz name: the payload now also holds schema documents.

resolve() returns schema.sql-compatible properties, semantic labels/definitions
with explicit outcomes, unmapped property names, and the original JSON evidence.
PyLD expands declared contexts and canonicalizes blank nodes; blank-node IDs are
scoped to source_url. Named graphs, language tags and literal datatypes remain
explicit. Source JSON is retained because JSON-LD annotations and undefined
properties cannot all be expressed as RDF. The output builder must account for
these attributes rather than publish the source as an opaque Parquet column.

The resolver retrieves predicate definitions, rdf:type class definitions and
URI-valued controlled-vocabulary terms using an explicit predicate list. It does
not fetch arbitrary object URLs, dataset identities, citations, older versions
or navigation. Vocabulary documents are shared by namespace and are not crawled
recursively. Unavailable definitions leave original terms intact and appear in
semantics as unavailable. Remote contexts are required for correct expansion;
missing or invalid contexts fail resolution rather than guessing their meaning.

Importing does no I/O. Context entry loads rdf-cache.json.gz or starts empty if
it is absent. Successful context exit atomically writes a deterministic gzip
JSON update artifact, creating its directory/file if necessary. No updates are
saved after an exception, cancellation or failed resolve call. The published
cache remains read-only; use a different, run-specific updates_path for each job.
Await all resolve calls before leaving the context. The caller owns client
lifetime, transport timeouts and retries; the resolver limits concurrent lookups.

The build stage combines worker artifacts with the lower-level cache operations:

    cache = RdfCache.load(cache_path)
    with gzip.open(updates_path, "rt", encoding="utf-8") as stream:
        cache.merge(json.load(stream))
    cache.save(candidate_path, referenced=retained_entity_uris)

merge() rejects conflicting worker updates. save() retains vocabulary documents,
prunes unreferenced mutable entities when referenced is supplied, and leaves
unchanged files untouched. Corrupt caches fail explicitly. Vocabulary documents
are reused unless full=True or their previous retrieval failed. Mutable entities
requested through RdfCache.fetch() are independently revalidated once per run.
ETag/Last-Modified validators support 304 responses; otherwise retrieve content.
Failures retain prior data as stale, with required-source failures raising.

PREDICATE_COLUMNS maps expanded RDF names to schema.sql fields. It is a
projection, never a whitelist: retain all source assertions, including unknown
properties and linked entities. Registry dct:modified belongs to the registry
info graph; it is distinct from descriptive dataset modification dates. Neither
PUBLIC rights nor distribution URLs establish retrievable tabular data.

Sources:
    https://docs.dataportal.se/registry/api/
    https://www.w3.org/TR/vocab-dcat-3/
    https://docs.researchdata.se/metadata/schema-org-json-ld/
    https://github.com/digitalbazaar/pyld
"""

import asyncio
import gzip
import hashlib
import io
import json
import logging
import os
import tempfile
from collections.abc import Callable, Collection, Mapping
from copy import deepcopy
from pathlib import Path
from typing import Any, Literal, cast
from urllib.parse import urldefrag, urljoin, urlsplit
from xml.sax import SAXException

import httpx
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    JsonValue,
    TypeAdapter,
    model_validator,
)
from pyld import jsonld
from pyld.context_resolver import ContextResolver
from rdflib import BNode, Graph, URIRef
from rdflib import Literal as RdfLiteral
from rdflib.compare import to_canonical_graph
from rdflib.exceptions import ParserError
from referencing import Registry, Resource
from referencing._core import Resolver
from referencing.exceptions import Unresolvable
from referencing.jsonschema import DRAFT202012

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
    resolved_url: str | None = None
    data: dict[str, JsonValue] | bool | None = None
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
_METADATA = TypeAdapter(dict[str, JsonValue] | bool)


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
        accept: str = "application/rdf+json, application/ld+json, application/json",
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
                headers = {"Accept": accept}
                if previous and previous.data is not None and previous.source_url == url:
                    if previous.etag:
                        headers["If-None-Match"] = previous.etag
                    if previous.last_modified:
                        headers["If-Modified-Since"] = previous.last_modified
                try:
                    response = await client.get(url, headers=headers, follow_redirects=True)
                    if response.status_code == 304:
                        if (
                            not previous
                            or previous.data is None
                            or not ("If-None-Match" in headers or "If-Modified-Since" in headers)
                        ):
                            raise ValueError("Received 304 without a cached representation")
                        entry = previous.model_copy(
                            update={
                                "outcome": "available",
                                "error": None,
                                "etag": response.headers.get("etag", previous.etag),
                                "last_modified": response.headers.get("last-modified", previous.last_modified),
                            }
                        )
                    else:
                        response.raise_for_status()
                        data = _METADATA.validate_python(decoder(response) if decoder else response.json())
                        entry = CacheEntry(
                            kind=kind,
                            source_url=url,
                            resolved_url=str(response.url),
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
                    entry = (previous or CacheEntry(kind=kind, source_url=url, outcome="unavailable")).model_copy(
                        update={"outcome": "unavailable", "error": reason}
                    )
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
        _write_cache(path, entries)


def _write_cache(path: Path, entries: Mapping[str, object]) -> None:
    """Atomically write deterministic JSON gzip, preserving unchanged files."""
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
    with gzip.GzipFile(filename="", mode="wb", fileobj=buffer, mtime=0, compresslevel=9) as archive:
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


# ------------------------------------------------------------------------------
# Workflow-facing semantic resolution
# ------------------------------------------------------------------------------


class _RdfTerm(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    type: Literal["uri", "bnode", "literal"]
    value: str
    lang: str | None = None
    datatype: str | None = None


class ResolvedMetadata(BaseModel):
    """Normalized assertions plus semantic lookups and original JSON evidence.

    properties follows schema.sql's metadata_value structure. source preserves
    JSON-LD annotations and unmapped attributes that RDF cannot represent; the
    output builder must handle these explicitly, not publish an opaque payload
    column. semantics annotates terms without replacing their original URIs.
    """

    source: dict[str, JsonValue] | list[dict[str, JsonValue]] | bool
    properties: list[dict[str, JsonValue]]
    semantics: dict[str, dict[str, JsonValue]]
    unmapped_properties: list[str]
    schemas: dict[str, JsonValue] = Field(default_factory=dict)
    schema_references: list[dict[str, JsonValue]] = Field(default_factory=list)


_DOCUMENT = TypeAdapter(dict[str, JsonValue] | list[dict[str, JsonValue]])
_RDF_JSON = TypeAdapter(dict[str, dict[str, list[_RdfTerm]]])
_CONTROLLED_PROPERTIES = frozenset(
    NAMESPACES[prefix] + name
    for prefix, name in (
        ("dct", "accessRights"),
        ("dct", "language"),
        ("dct", "license"),
        ("dct", "rights"),
        ("dct", "subject"),
        ("dct", "format"),
        ("dcat", "theme"),
        ("dcat", "mediaType"),
        ("dcat", "hadRole"),
        ("schema", "inLanguage"),
        ("schema", "license"),
    )
)
_LABELS = {
    NAMESPACES["rdfs"] + "label",
    NAMESPACES["skos"] + "prefLabel",
    NAMESPACES["schema"] + "name",
    "http://schema.org/name",
}
_DEFINITIONS = {
    NAMESPACES["rdfs"] + "comment",
    NAMESPACES["skos"] + "definition",
    NAMESPACES["dct"] + "description",
}
_RDF_ACCEPT = "application/ld+json, text/turtle;q=0.9, application/rdf+xml;q=0.8, application/rdf+json;q=0.7, application/json;q=0.6"


def _decode_definition(response: httpx.Response) -> dict[str, JsonValue]:
    """Read a semantic document without letting a parser perform network I/O."""
    media_type = response.headers.get("content-type", "").split(";")[0].strip()
    formats = {
        "text/turtle": "turtle",
        "application/x-turtle": "turtle",
        "application/rdf+xml": "xml",
        "application/n-triples": "nt",
    }
    if media_type not in formats:
        document = _DOCUMENT.validate_python(response.json())
        return {"@graph": list(document)} if isinstance(document, list) else document
    try:
        graph = to_canonical_graph(
            Graph().parse(
                data=response.content,
                format=formats[media_type],
                publicID=str(response.url),
            )
        )
    except (SyntaxError, ParserError, SAXException) as error:
        raise ValueError("Malformed RDF vocabulary document") from error
    result: dict[str, dict[str, list[dict[str, str]]]] = {}
    for subject, predicate, obj in graph:
        entity = "_:" + str(subject) if isinstance(subject, BNode) else str(subject)
        term = {"type": "literal", "value": str(obj)}
        if isinstance(obj, URIRef):
            term["type"] = "uri"
        elif isinstance(obj, BNode):
            term = {"type": "bnode", "value": "_:" + str(obj)}
        elif isinstance(obj, RdfLiteral):
            if obj.language:
                term["lang"] = obj.language
            if obj.datatype:
                term["datatype"] = str(obj.datatype)
        result.setdefault(entity, {}).setdefault(str(predicate), []).append(term)
    for fields in result.values():
        for terms in fields.values():
            terms.sort(key=lambda value: json.dumps(value, sort_keys=True))
    return cast(dict[str, JsonValue], result)


def _normalize(
    document: dict[str, Any] | list[dict[str, Any]], source_url: str, loader: Callable[..., dict[str, Any]]
) -> tuple[list[dict[str, Any]], list[str]]:
    """Canonicalize RDF/JSON or JSON-LD in a worker, with a cache-backed loader."""
    dropped: set[str] = set()
    options = {
        "algorithm": "URDNA2015",
        "base": source_url,
        "documentLoader": loader,
        "contextResolver": ContextResolver({}, loader),
        "rdfDirection": "i18n-datatype",
    }
    rdf_json = isinstance(document, dict) and all(
        not entity.startswith("@")
        and isinstance(fields, dict)
        and all(
            isinstance(terms, list)
            and all(isinstance(term, dict) and "type" in term and "value" in term for term in terms)
            for terms in fields.values()
        )
        for entity, fields in document.items()
    )
    if rdf_json:
        triples = []
        for subject, fields in _RDF_JSON.validate_python(document).items():
            for predicate, terms in fields.items():
                if not urlsplit(predicate).scheme or not (subject.startswith("_:") or urlsplit(subject).scheme):
                    raise ValueError("RDF/JSON subjects and predicates must be absolute IRIs or subject blank nodes")
                for term in terms:
                    obj = {
                        "type": {
                            "uri": "IRI",
                            "bnode": "blank node",
                            "literal": "literal",
                        }[term.type],
                        "value": term.value,
                    }
                    if term.type == "literal":
                        obj["datatype"] = term.datatype or NAMESPACES["rdf" if term.lang else "xsd"] + (
                            "langString" if term.lang else "string"
                        )
                        if term.lang:
                            obj["language"] = term.lang
                    triples.append(
                        {
                            "subject": {
                                "type": "blank node" if subject.startswith("_:") else "IRI",
                                "value": subject,
                            },
                            "predicate": {"type": "IRI", "value": predicate},
                            "object": obj,
                        }
                    )
        normalized = jsonld.normalize(
            jsonld.JsonLdProcessor.to_nquads({"@default": triples}),
            {**options, "inputFormat": "application/n-quads"},
        )
    else:
        expanded = jsonld.expand(
            document,
            options=options,
            on_property_dropped=lambda name: dropped.add(name or "<unmapped>"),
        )
        normalized = jsonld.normalize(expanded, options)
    scope = hashlib.sha256(source_url.encode()).hexdigest()[:16]

    def scoped(value: str) -> str:
        return f"_:{scope}_{value[2:]}" if value.startswith("_:") else value

    properties = []
    for graph, triples in sorted(normalized.items()):
        for triple in triples:
            obj = triple["object"]
            properties.append(
                {
                    "source_url": source_url,
                    "entity_id": scoped(triple["subject"]["value"]),
                    "graph_id": None if graph == "@default" else scoped(graph),
                    "property": triple["predicate"]["value"],
                    "value": scoped(obj["value"]) if obj["type"] == "blank node" else obj["value"],
                    "value_type": {
                        "IRI": "uri",
                        "blank node": "bnode",
                        "literal": "literal",
                    }[obj["type"]],
                    "language": obj.get("language"),
                    "datatype": obj.get("datatype"),
                    "path": None,
                }
            )
    properties.sort(key=lambda value: json.dumps(value, sort_keys=True))
    return properties, sorted(dropped)


class SemanticResolver:
    """Resolve metadata and save only a worker's cache updates on successful exit.

    async with SemanticResolver(cache_path, client, updates_path) as resolver:
        result = await resolver.resolve(metadata, source_url=metadata_url)

    The paths must differ. The published cache is read-only; updates_path is a
    run-specific gzip JSON artifact for RdfCache.merge() in the build job. No
    artifact is saved after an exception, cancellation, or failed resolve(), even
    if the caller catches that error. Await all resolve calls before leaving.
    The caller owns the shared client and retry policy. concurrency bounds this
    resolver's simultaneous HTTP lookups. Remote contexts are required; semantic
    definitions are optional and their failures are returned with original URIs.
    documents maps vocabulary namespace IRIs to explicit retrieval URLs.
    """

    def __init__(
        self,
        cache_path: Path,
        client: httpx.AsyncClient,
        updates_path: Path,
        *,
        full: bool = False,
        concurrency: int = 4,
        documents: Mapping[str, str] | None = None,
    ) -> None:
        if concurrency < 1:
            raise ValueError("concurrency must be positive")
        if cache_path.resolve() == updates_path.resolve():
            raise ValueError("Worker updates must not overwrite the published cache")
        self._path, self._updates, self._client = cache_path, updates_path, client
        self._full = full
        self._limit = asyncio.Semaphore(concurrency)
        self._documents = {
            namespace: namespace.rstrip("#").replace("http://", "https://", 1) for namespace in NAMESPACES.values()
        }
        self._documents[NAMESPACES["schema"]] = "https://schema.org/version/latest/schemaorg-current-https.jsonld"
        self._documents.update(documents or {})
        self._definitions: dict[str, asyncio.Task[tuple[CacheEntry, list[dict[str, Any]]]]] = {}
        self._active = False
        self._failed = False
        self._pending = 0

    async def __aenter__(self) -> "SemanticResolver":
        if self._active or hasattr(self, "_cache"):
            raise RuntimeError("Use a new SemanticResolver for each run")
        self._cache = RdfCache.load(self._path)
        self._active = True
        return self

    async def __aexit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self._active = False
        if self._pending:
            raise RuntimeError("Await all resolve calls before closing the resolver")
        if exc_type is not None or self._failed:
            for task in self._definitions.values():
                task.cancel()
            await asyncio.gather(*self._definitions.values(), return_exceptions=True)
            return
        if exc_type is None and not self._failed:
            _write_cache(self._updates, self._cache.changes())

    async def _fetch(self, uri: str, source_url: str, *, context: bool = False) -> CacheEntry:
        async with self._limit:
            return await self._cache.fetch(
                uri,
                self._client,
                kind="vocabulary",
                source_url=source_url,
                full=self._full,
                required=context,
                decoder=None if context else _decode_definition,
                accept="application/ld+json, application/json" if context else _RDF_ACCEPT,
            )

    async def _properties(
        self, document: dict[str, Any] | list[dict[str, Any]], source_url: str
    ) -> tuple[list[dict[str, Any]], list[str]]:
        loop = asyncio.get_running_loop()

        def loader(url: str, options: dict[str, Any] | None = None) -> dict[str, Any]:
            document_url = (
                "https://schema.org/docs/jsonldcontext.jsonld"
                if url.rstrip("/") in {"https://schema.org", "http://schema.org"}
                else url
            )
            future = asyncio.run_coroutine_threadsafe(self._fetch(document_url, document_url, context=True), loop)
            try:
                entry = future.result(timeout=90)
            except TimeoutError:
                future.cancel()
                raise CacheFetchError("JSON-LD context retrieval exceeded 90 seconds") from None
            return {
                "contextUrl": None,
                "documentUrl": entry.resolved_url or document_url,
                "document": entry.data,
            }

        return await asyncio.to_thread(_normalize, document, source_url, loader)

    def _document(self, uri: str) -> tuple[str, str]:
        lookup = uri.replace("http://schema.org/", "https://schema.org/", 1)
        for namespace in sorted(self._documents, key=len, reverse=True):
            if lookup.startswith(namespace):
                # A JSON-LD context and an ontology may share a namespace but
                # are different documents; cache by the actual retrieval URL.
                return self._documents[namespace], self._documents[namespace]
        return urldefrag(uri).url, urldefrag(uri).url

    async def _definition(self, key: str, url: str) -> tuple[CacheEntry, list[dict[str, Any]]]:
        previous = self._cache._entries.get(key)
        entry = await self._fetch(key, url)
        if not isinstance(entry.data, dict):
            return entry, []
        try:
            properties, _ = await self._properties(entry.data, entry.resolved_url or url)
            return entry, properties
        except (ValueError, jsonld.JsonLdError, CacheFetchError) as error:
            entry = (previous or entry).model_copy(update={"outcome": "unavailable", "error": type(error).__name__})
            self._cache._entries[key] = entry
            return entry, []

    async def _schemas(
        self, metadata: dict[str, Any] | list[dict[str, Any]] | bool, source_url: str, format: str
    ) -> ResolvedMetadata:
        """Collect schema definitions and resolve explicit references without inlining."""
        source = TypeAdapter(dict[str, JsonValue] | bool).validate_python(metadata)
        source = deepcopy(source)
        loop = asyncio.get_running_loop()

        def retrieve(uri: str) -> Resource:
            if urlsplit(uri).scheme not in {"http", "https"}:
                raise ValueError("Only HTTP(S) schema resources can be retrieved")
            future = asyncio.run_coroutine_threadsafe(self._fetch(uri, uri, context=True), loop)
            try:
                entry = future.result(timeout=90)
            except TimeoutError:
                future.cancel()
                raise CacheFetchError("Schema retrieval exceeded 90 seconds") from None
            return Resource.from_contents(entry.data, default_specification=DRAFT202012)

        def collect() -> tuple[dict[str, JsonValue], list[dict[str, JsonValue]]]:
            root = Resource.from_contents(source, default_specification=DRAFT202012)
            # referencing uses an attrs wrapper whose constructor mypy cannot infer.
            registry = Registry(retrieve=retrieve).with_resource(source_url, root).crawl()  # type: ignore[call-arg]
            resolver = registry.resolver(source_url)
            schemas: dict[str, JsonValue] = {}
            references: list[dict[str, JsonValue]] = []
            seen: set[tuple[str, str]] = set()

            def visit(resource: Resource, current: Resolver[Any], identity: str) -> None:
                # Keep recursive schemas as references, never infinitely expanded trees.
                signature = (identity, json.dumps(resource.contents, sort_keys=True))
                if signature in seen:
                    return
                seen.add(signature)
                schemas[identity] = resource.contents
                if not isinstance(resource.contents, dict):
                    return
                for keyword in ("$ref", "$dynamicRef", "$recursiveRef"):
                    reference = resource.contents.get(keyword)
                    if not isinstance(reference, str):
                        continue
                    evidence = {
                        "source": identity,
                        "keyword": keyword,
                        "reference": reference,
                    }
                    if keyword != "$ref":
                        references.append(
                            {
                                **evidence,
                                "outcome": "unsupported",
                                "error": "Dynamic evaluation scope requires a schema validator",
                            }
                        )
                        continue
                    try:
                        resolved = current.lookup(reference)
                    except Unresolvable as error:
                        references.append({**evidence, "outcome": "unavailable", "error": str(error)})
                        continue
                    target = urljoin(urldefrag(identity).url, reference)
                    references.append({**evidence, "target": target, "outcome": "available"})
                    child = Resource.from_contents(resolved.contents, default_specification=DRAFT202012)
                    visit(child, resolved.resolver, target)
                for child in resource.subresources():
                    child_id = child.id()
                    child_identity = (
                        urljoin(identity, child_id)
                        if child_id
                        else urldefrag(identity).url
                        + "#subschema/"
                        + hashlib.sha256(json.dumps(child.contents, sort_keys=True).encode()).hexdigest()[:16]
                    )
                    visit(child, current.in_subresource(child), child_identity)

            is_openapi = format == "openapi" or isinstance(source, dict) and "openapi" in source
            if is_openapi:
                if not isinstance(source, dict) or not str(source.get("openapi", "")).startswith("3."):
                    raise ValueError("Expected an OpenAPI 3.x JSON document")

                def embedded(value: JsonValue, path: str = "") -> None:
                    if isinstance(value, dict):
                        for key, child in value.items():
                            pointer = path + "/" + key.replace("~", "~0").replace("/", "~1")
                            if key == "schema" or path == "/components/schemas":
                                resolved = resolver.lookup("#" + pointer)
                                resource = Resource.from_contents(child, default_specification=DRAFT202012)
                                visit(
                                    resource,
                                    resolved.resolver,
                                    source_url + "#" + pointer,
                                )
                            elif key not in {
                                "example",
                                "examples",
                                "default",
                                "enum",
                            } and not key.startswith("x-"):
                                embedded(child, pointer)
                    elif isinstance(value, list):
                        for index, child in enumerate(value):
                            embedded(child, path + "/" + str(index))

                embedded(source)
            else:
                identity = urljoin(source_url, root.id() or "")
                visit(root, resolver.in_subresource(root), identity)
            return dict(sorted(schemas.items())), sorted(references, key=lambda row: json.dumps(row, sort_keys=True))

        schemas, references = await asyncio.to_thread(collect)
        return ResolvedMetadata(
            source=source,
            properties=[],
            semantics={},
            unmapped_properties=[],
            schemas=schemas,
            schema_references=references,
        )

    async def resolve(
        self,
        metadata: dict[str, Any] | list[dict[str, Any]] | bool,
        *,
        source_url: str,
        format: Literal["auto", "rdf", "json-schema", "openapi"] = "auto",
    ) -> ResolvedMetadata:
        """Expand/canonicalize metadata and annotate only its semantic resources.

        Automatically retrieve predicates, rdf:type classes and URI objects of
        the explicit controlled-vocabulary predicates above. Never follow other
        URI objects, dataset identities, citations, version links or navigation.
        RDF definition documents are not recursively crawled. JSON Schema and
        OpenAPI schema references use the same cache, with explicit outcomes;
        schemas without $schema require format="json-schema".
        """
        if format not in {"auto", "rdf", "json-schema", "openapi"}:
            raise ValueError("Unknown metadata format")
        if not self._active:
            raise RuntimeError("Use resolve() inside async with SemanticResolver(...)")
        self._pending += 1
        try:
            schema_format = format in {"json-schema", "openapi"} or (
                format == "auto" and isinstance(metadata, dict) and ("$schema" in metadata or "openapi" in metadata)
            )
            if schema_format:
                return await self._schemas(metadata, source_url, format)
            source = deepcopy(_DOCUMENT.validate_python(metadata))
            properties, dropped = await self._properties(source, source_url)
            terms = {row["property"] for row in properties}
            terms.update(
                row["value"]
                for row in properties
                if row["value_type"] == "uri"
                and row["property"] in _CONTROLLED_PROPERTIES | {NAMESPACES["rdf"] + "type"}
            )
            documents = {term: self._document(term) for term in terms if urlsplit(term).scheme in {"http", "https"}}
            for key, url in documents.values():
                if key not in self._definitions:
                    self._definitions[key] = asyncio.create_task(self._definition(key, url))
            await asyncio.gather(*(self._definitions[key] for key in sorted({key for key, _ in documents.values()})))
            semantics: dict[str, dict[str, JsonValue]] = {}
            for term, (key, url) in sorted(documents.items()):
                entry, rows = self._definitions[key].result()
                lookup = term.replace("http://schema.org/", "https://schema.org/", 1)
                descriptions = [row for row in rows if row["entity_id"] in {term, lookup}]
                semantics[term] = {
                    "source_url": entry.resolved_url or url,
                    "outcome": entry.outcome
                    if descriptions
                    else "unavailable"
                    if entry.outcome == "unavailable"
                    else "undefined",
                    "stale": entry.stale,
                    "error": entry.error,
                    "labels": [row for row in descriptions if row["property"] in _LABELS],
                    "definitions": [row for row in descriptions if row["property"] in _DEFINITIONS],
                }
            return ResolvedMetadata(
                source=source,
                properties=properties,
                semantics=semantics,
                unmapped_properties=dropped,
            )
        except BaseException:
            self._failed = True
            raise
        finally:
            self._pending -= 1
