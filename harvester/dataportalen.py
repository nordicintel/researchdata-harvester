# SPDX-License-Identifier: NOASSERTION
# Copyright (c) 2026 NordicIntel
"""Dataportalen inventory pagination, evidence matching and bounded RDF closure."""

import hashlib
import json
import logging
from dataclasses import dataclass
from threading import get_ident
from typing import Any
from typing import Literal as LiteralType
from urllib.parse import urlsplit

from pydantic import BaseModel, Field, field_validator
from rdflib import BNode, Graph, Literal, URIRef
from rdflib.compare import to_canonical_graph
from rdflib.plugins.parsers.ntriples import (
    W3CNTriplesParser,
    r_literal,
    unquote,
    uriquote,
)

from harvester.config import Config
from harvester.metadata import normalize_identity
from harvester.transport import Transport

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class LexicalLiteral:
    """Preserve source spelling even for datatypes RDFLib always normalizes."""

    value: str
    language: str | None
    datatype: URIRef | None
    ill_typed: bool | None


class ConversionWarningFilter(logging.Filter):
    """Replace only this thread's RDFLib conversion tracebacks with diagnostics."""

    def __init__(self) -> None:
        super().__init__()
        self.thread = get_ident()

    def filter(self, record: logging.LogRecord) -> bool:
        return not (
            record.thread == self.thread
            and record.levelno == logging.WARNING
            and record.funcName == "_castLexicalToPython"
            and record.getMessage().startswith(
                "Failed to convert Literal lexical form to value. Datatype="
            )
        )


class LexicalNTriplesParser(W3CNTriplesParser):
    """Use RDFLib syntax parsing while retaining original lexical literal values."""

    def literal(self) -> LexicalLiteral | bool:
        if not self.peek('"'):
            return False
        lexical, language, datatype = self.eat(r_literal).groups()
        value = unquote(lexical)
        datatype = URIRef(uriquote(unquote(datatype))) if datatype else None
        literal = Literal(value, lang=language, datatype=datatype, normalize=False)
        return LexicalLiteral(value, language, datatype, literal.ill_typed)


class MetadataTriples:
    """Index triples by subject without converting lexical literals to values."""

    def __init__(self) -> None:
        self.outgoing: dict[
            URIRef | BNode, set[tuple[URIRef, URIRef | BNode | LexicalLiteral]]
        ] = {}

    def triple(
        self,
        subject: URIRef | BNode,
        predicate: URIRef,
        obj: URIRef | BNode | LexicalLiteral,
    ) -> None:
        self.outgoing.setdefault(subject, set()).add((predicate, obj))


PREFIXES = """
PREFIX dcat: <http://www.w3.org/ns/dcat#>
PREFIX dct: <http://purl.org/dc/terms/>
PREFIX adms: <http://www.w3.org/ns/adms#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
"""
LINK_PREDICATES = [
    "http://www.w3.org/ns/dcat#distribution",
    "http://www.w3.org/ns/dcat#accessService",
    "http://purl.org/dc/terms/publisher",
    "http://www.w3.org/ns/dcat#contactPoint",
    "http://www.w3.org/ns/prov#qualifiedAttribution",
    "http://www.w3.org/ns/prov#agent",
]


def iri(uri: str) -> str:
    if urlsplit(uri).scheme not in {"http", "https", "urn"} or any(
        char in uri for char in '<>"{}|^`\\\n\r'
    ):
        raise ValueError(f"Invalid RDF IRI: {uri!r}.")
    return "<" + uri + ">"


class CatalogMember(BaseModel):
    """A source-owned catalog membership must identify an RDF resource."""

    type: LiteralType["uri"]
    value: str

    @field_validator("value")
    @classmethod
    def validate_uri(cls, value: str) -> str:
        iri(value)
        parsed = urlsplit(value)
        if any(character.isspace() for character in value) or (
            parsed.scheme in {"http", "https"} and not parsed.hostname
        ):
            raise ValueError(f"Catalog member is not an absolute RDF URI: {value!r}.")
        return value


class CatalogMetadata(BaseModel):
    """Validate required RDF/JSON predicates without changing other metadata."""

    types: list[CatalogMember] = Field(
        alias="http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
    )
    members: list[CatalogMember] = Field(alias="http://www.w3.org/ns/dcat#dataset")


class DataportalenClient:
    def __init__(self, transport: Transport, url: str) -> None:
        self.transport = transport
        self.url = url

    async def catalog_inventory(self, source: str, catalog: str) -> dict[str, Any]:
        """Read explicit catalog membership and retain reproducible source evidence."""
        response = await self.transport.request(
            "GET", source, headers={"Accept": "application/json"}
        )
        document = response.json()
        if not isinstance(document, dict) or set(document) != {catalog}:
            raise ValueError(
                f"Catalog metadata must contain exactly the expected root {catalog}."
            )
        metadata = CatalogMetadata.model_validate(document[catalog])
        if "http://www.w3.org/ns/dcat#Catalog" not in {
            term.value for term in metadata.types
        }:
            raise ValueError(
                f"Catalog metadata root {catalog} has no dcat:Catalog type."
            )
        return {
            "members": sorted({term.value for term in metadata.members}),
            "source": source,
            "catalog": catalog,
            "response_url": str(response.url),
            "sha256": hashlib.sha256(response.content).hexdigest(),
            "etag": response.headers.get("etag"),
            "last_modified": response.headers.get("last-modified"),
        }

    async def select(self, query: str) -> list[dict[str, Any]]:
        response = await self.transport.request(
            "POST",
            self.url,
            data={"query": PREFIXES + query},
            headers={"Accept": "application/sparql-results+json"},
        )
        document = response.json()
        if not isinstance(document.get("results", {}).get("bindings"), list):
            raise ValueError(
                "SPARQL response has no bindings; inventory is incomplete."
            )
        return document["results"]["bindings"]

    async def identities(self, uris: list[str]) -> dict[str, list[dict[str, str]]]:
        result = {
            uri: [
                {
                    "value": uri,
                    "normalized": normalize_identity(uri) or uri,
                    "field": "datasetURI",
                }
            ]
            for uri in uris
        }
        if not uris:
            return result
        branches = [
            "{ ?dataset ?field ?value . FILTER(?field IN (dct:identifier,adms:identifier,owl:sameAs,dcat:landingPage)) }"
        ]
        branches.append(
            "{ ?dataset adms:identifier ?node . ?node skos:notation ?value . BIND(skos:notation AS ?field) }"
        )
        branches.append(
            "{ ?dataset dcat:distribution ?distribution . ?distribution ?field ?value . FILTER(?field IN (dcat:accessURL,dcat:downloadURL)) }"
        )
        rows = await self.select(
            "SELECT DISTINCT ?dataset ?field ?value WHERE { VALUES ?dataset { "
            + " ".join(map(iri, uris))
            + " } { "
            + " UNION ".join(branches)
            + " } FILTER(!isBlank(?value)) }"
        )
        for row in rows:
            uri, value = row["dataset"]["value"], row["value"]["value"]
            normalized = normalize_identity(value)
            if uri in result and normalized:
                result[uri].append(
                    {
                        "value": value,
                        "normalized": normalized,
                        "field": row["field"]["value"],
                    }
                )
        return result

    async def metadata(self, uris: list[str]) -> dict[str, dict[str, Any]]:
        """Fetch endpoint concise bounded descriptions in one response.

        Follow named resources only for known metadata relations and preserve all
        available blank-node descendants with response-scoped identities.
        """
        path = "(" + "|".join(map(iri, LINK_PREDICATES)) + ")*"
        query = (
            "DESCRIBE ?dataset ?resource WHERE { VALUES ?dataset { "
            + " ".join(map(iri, uris))
            + " } ?dataset "
            + path
            + " ?resource }"
        )
        response = await self.transport.request(
            "POST",
            self.url,
            data={"query": PREFIXES + query},
            headers={"Accept": "application/n-triples"},
        )
        content_type = response.headers.get("content-type", "").split(";", 1)[0]
        if content_type != "application/n-triples":
            raise ValueError(
                f"Expected application/n-triples to preserve RDF lexical values, received {content_type}."
            )
        graph = MetadataTriples()
        conversion_logger = logging.getLogger("rdflib.term")
        warning_filter = ConversionWarningFilter()
        conversion_logger.addFilter(warning_filter)
        try:
            LexicalNTriplesParser(sink=graph).parsestring(response.text)
        finally:
            conversion_logger.removeFilter(warning_filter)
        invalid_count = sum(
            isinstance(obj, LexicalLiteral) and obj.ill_typed is True
            for outgoing in graph.outgoing.values()
            for _, obj in outgoing
        )
        if invalid_count:
            logger.warning(
                "Dataportalen metadata contains %d ill-typed literal triples; "
                "original lexical values and datatypes are preserved with ill_typed=true.",
                invalid_count,
            )

        def term(value: URIRef | BNode | LexicalLiteral) -> dict[str, Any]:
            if isinstance(value, BNode):
                return {"type": "bnode", "value": str(value)}
            if isinstance(value, LexicalLiteral):
                result = {"type": "literal", "value": value.value}
                if value.language:
                    result["xml:lang"] = value.language
                if value.datatype:
                    result["datatype"] = str(value.datatype)
                if value.ill_typed is True:
                    result["ill_typed"] = True
                return result
            return {"type": "uri", "value": str(value)}

        result = {}
        for uri in uris:
            pending, visited, triples = [URIRef(uri)], set(), []
            while pending:
                subject = pending.pop()
                if subject in visited:
                    continue
                visited.add(subject)
                outgoing = graph.outgoing.get(subject, set())
                if isinstance(subject, BNode) and not outgoing:
                    raise ValueError(
                        f"Dataportalen omitted blank-node description for {uri}; metadata is incomplete."
                    )
                for predicate, obj in outgoing:
                    triples.append(
                        {
                            "s": term(subject),
                            "p": term(predicate),
                            "o": term(obj),
                        }
                    )
                    if isinstance(obj, BNode) or (
                        str(predicate) in LINK_PREDICATES and isinstance(obj, URIRef)
                    ):
                        pending.append(obj)
            if not triples:
                raise ValueError(f"Dataportalen omitted requested metadata root {uri}.")
            result[uri] = triples_to_metadata(triples, uri)
        return result


def identity_index(
    research: dict[str, list[dict[str, str]]],
) -> dict[str, list[tuple[str, dict[str, str]]]]:
    result: dict[str, list[tuple[str, dict[str, str]]]] = {}
    for identifier, evidence in sorted(research.items()):
        for item in sorted(evidence, key=lambda item: json.dumps(item, sort_keys=True)):
            result.setdefault(item["normalized"], []).append((identifier, item))
    return result


def match_identities(
    research: dict[str, list[dict[str, str]]],
    external: list[dict[str, str]],
    *,
    index: dict[str, list[tuple[str, dict[str, str]]]] | None = None,
) -> list[dict[str, Any]]:
    """Retain exact primary matches, flag conflicting dataset identity evidence."""
    by_value = index if index is not None else identity_index(research)
    matches: dict[str, dict[str, Any]] = {}
    for item in sorted(external, key=lambda item: json.dumps(item, sort_keys=True)):
        if item.get("field") in {
            "http://www.w3.org/ns/dcat#accessURL",
            "http://www.w3.org/ns/dcat#downloadURL",
        }:
            continue
        for identifier, research_item in by_value.get(item["normalized"], []):
            match = matches.setdefault(
                identifier,
                {
                    "identifier": identifier,
                    "rule": "exact_identity",
                    "relation": research_item.get("relation", "dataset"),
                    "status": "matched",
                    "evidence": [],
                },
            )
            match["evidence"].append(
                {"researchdata": research_item, "dataportalen": item}
            )
            if research_item.get("relation") == "version":
                match["relation"] = "version"
    datasets = {identifier.rsplit("/", 1)[0] for identifier in matches}
    if len(datasets) > 1:
        for match in matches.values():
            match["status"] = "ambiguous"
    return [matches[key] for key in sorted(matches)]


def triples_to_metadata(triples: list[dict[str, Any]], root: str) -> dict[str, Any]:
    """Build a canonical JSON graph while retaining exact source literal terms.

    Literal surrogates keep RDFLib's blank-node canonicalization independent of
    its datatype coercion. The returned node keys use ``_:`` for blank nodes.
    Blank-node labels are local to this metadata graph.
    """
    graph = Graph()
    literals: dict[str, dict[str, Any]] = {}

    def encode(term: dict[str, Any]) -> URIRef | BNode:
        if term["type"] == "bnode":
            return BNode(term["value"].removeprefix("_:"))
        if term["type"] == "literal":
            encoded = json.dumps(term, sort_keys=True, ensure_ascii=False)
            key = (
                "urn:harvester:lexical:" + hashlib.sha256(encoded.encode()).hexdigest()
            )
            literals[key] = dict(term)
            return URIRef(key)
        # Tag URI terms too, avoiding collisions with lexical surrogate URIs.
        return URIRef("urn:harvester:uri:" + term["value"].encode().hex())

    for triple in triples:
        graph.add((encode(triple["s"]), encode(triple["p"]), encode(triple["o"])))

    def decode(term: URIRef | BNode) -> dict[str, Any]:
        if isinstance(term, BNode):
            return {"type": "bnode", "value": str(term)}
        if str(term) in literals:
            return dict(literals[str(term)])
        return {
            "type": "uri",
            "value": bytes.fromhex(
                str(term).removeprefix("urn:harvester:uri:")
            ).decode(),
        }

    nodes: dict[str, dict[str, list[dict[str, Any]]]] = {}
    for subject, predicate, obj in to_canonical_graph(graph):
        subject_term = decode(subject)
        node_id = (
            "_:" + subject_term["value"]
            if subject_term["type"] == "bnode"
            else subject_term["value"]
        )
        nodes.setdefault(node_id, {}).setdefault(decode(predicate)["value"], []).append(
            decode(obj)
        )
    return {
        "root": root,
        "nodes": {
            node_id: {
                predicate: sorted(
                    terms, key=lambda term: json.dumps(term, sort_keys=True)
                )
                for predicate, terms in sorted(predicates.items())
            }
            for node_id, predicates in sorted(nodes.items())
        },
    }


def metadata_to_triples(metadata: dict[str, Any]) -> list[dict[str, Any]]:
    """Expand the published root/nodes graph into matcher-internal triples."""
    return [
        {
            "s": {
                "type": "bnode" if node_id.startswith("_:") else "uri",
                "value": node_id.removeprefix("_:")
                if node_id.startswith("_:")
                else node_id,
            },
            "p": {"type": "uri", "value": predicate},
            "o": dict(term),
        }
        for node_id, predicates in sorted(metadata["nodes"].items())
        for predicate, terms in sorted(predicates.items())
        for term in terms
    ]


async def harvest_dataportalen(
    transport: Transport, config: Config, previous: dict[str, dict[str, Any]]
) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    """Refresh complete metadata for explicit Researchdata catalog membership.

    Dataportalen has no reliable per-record modification indicator here. Each
    run refreshes every scoped description and identity batch. Failed inventory
    or metadata requests propagate before returning a replacement collection.
    ``previous`` is accepted for the coordinator's common source interface.
    """
    client = DataportalenClient(transport, config.sparql_url)
    catalog = await client.catalog_inventory(
        config.catalog_metadata_url, config.catalog_uri
    )
    records: dict[str, dict[str, Any]] = {}
    batch_size = config.identity_batch_size
    if batch_size < 1:
        raise ValueError("identity_batch_size must be positive.")
    for offset in range(0, len(catalog["members"]), batch_size):
        members = catalog["members"][offset : offset + batch_size]
        identities = await client.identities(members)
        metadata = await client.metadata(members)
        for uri in members:
            evidence = {
                json.dumps(item, sort_keys=True): item for item in identities[uri]
            }
            records[uri] = {
                "uri": uri,
                "identities": [evidence[key] for key in sorted(evidence)],
                "metadata": metadata[uri],
            }
    return records, catalog
