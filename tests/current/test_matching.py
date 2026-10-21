# SPDX-License-Identifier: NOASSERTION
# Copyright (c) 2026 NordicIntel
"""Verify conservative compound matching and confirmed-map invalidation."""

from copy import deepcopy
from typing import Any
from xml.etree.ElementTree import fromstring

import pytest

from harvester.dataportalen import triples_to_metadata
from harvester.matching import DCAT, DCT, RDF, MetadataIndex, match_records

URI = "https://data.test/a"
IDENTIFIER = "oai:researchdata.se:one/1"
PUBLISHER = "https://publisher.test/organisation"
PRIMARY = "https://doi.org/10.1234/own"
XSD = "http://www.w3.org/2001/XMLSchema#"


def tree(xml: str) -> dict[str, Any]:
    def convert(node: Any) -> dict[str, Any]:
        return {
            "tag": node.tag,
            "attributes": dict(node.attrib),
            "text": node.text,
            "children": [convert(child) for child in node],
        }

    return convert(fromstring(xml))


def source(
    *, title: str = "Title", description: str = "Abstract", identifier: str = IDENTIFIER
) -> dict[str, Any]:
    return {
        "identifier": identifier,
        "identities": [],
        "representations": {
            "dcat": tree(
                f'''<rdf:RDF xmlns:rdf="{RDF}" xmlns:dcat="{DCAT}" xmlns:dct="{DCT}"><dcat:Dataset><dct:title xml:lang="en">{title}</dct:title><dct:description xml:lang="en">{description}</dct:description><dct:publisher rdf:resource="{PUBLISHER}"/><dct:issued rdf:datatype="{XSD}date">2026-01-01</dct:issued></dcat:Dataset></rdf:RDF>'''
            ),
            "datacite": tree(
                f'<resource xmlns="http://datacite.org/schema/kernel-4"><identifier>{PRIMARY}</identifier><relatedIdentifiers><relatedIdentifier>https://paper.test/citation</relatedIdentifier></relatedIdentifiers></resource>'
            ),
        },
    }


def target(
    *,
    uri: str = URI,
    title: str = "Title",
    description: str = "Abstract",
    publisher: str = PUBLISHER,
) -> dict[str, Any]:
    fields = {
        "title": {"type": "literal", "value": title, "xml:lang": "en"},
        "description": {"type": "literal", "value": description, "xml:lang": "en"},
        "publisher": {"type": "uri", "value": publisher},
        "issued": {"type": "literal", "value": "2026-01-01", "datatype": XSD + "date"},
    }
    triples = [
        {
            "s": {"type": "uri", "value": uri},
            "p": {"type": "uri", "value": DCT + key},
            "o": value,
        }
        for key, value in fields.items()
    ]
    return {"uri": uri, "identities": [], "metadata": triples_to_metadata(triples, uri)}


def run(research: dict[str, Any], dataportalen: dict[str, Any], previous: Any = None):
    return match_records(
        research,
        dataportalen,
        *(previous or ([], {}, {})),
        {
            "members": sorted(dataportalen),
            "catalog": "https://researchdata.test/catalog",
        },
    )


def test_compound_matches_are_reused_and_unrelated_changes_keep_cache(monkeypatch):
    research = {IDENTIFIER: source()}
    dataportalen = {URI: target()}
    first = run(research, dataportalen)
    assert first[1] == {IDENTIFIER: [URI]}

    def unexpected(*args, **kwargs):
        raise AssertionError("Unchanged confirmed mapping should be reused")

    monkeypatch.setattr(MetadataIndex, "match", unexpected)
    assert run(research, dataportalen, first) == first
    research["other"] = source(title="Different title", identifier="other")
    second = run(research, dataportalen, first)
    assert second == first


def test_new_same_title_peer_invalidates_mapping_and_preserves_ambiguity():
    research = {IDENTIFIER: source()}
    dataportalen = {URI: target()}
    first = run(research, dataportalen)
    research["other"] = source(identifier="other")
    second = run(research, dataportalen, first)
    assert len(second[0]) == 2
    assert {link["status"] for link in second[0]} == {"ambiguous"}
    assert second[1] == {}
    del research["other"]
    assert run(research, dataportalen, second)[1] == {IDENTIFIER: [URI]}


def test_changed_relevant_evidence_and_removed_members_discard_stale_maps():
    research = {IDENTIFIER: source()}
    dataportalen = {URI: target()}
    first = run(research, dataportalen)
    assert (
        run({IDENTIFIER: source(description="Changed")}, dataportalen, first)[1] == {}
    )
    assert run(research, {URI: target(publisher="https://other.test/")}, first)[1] == {}
    assert run({}, dataportalen, first)[1] == {}
    assert run(research, {}, first)[1] == {}


def test_direct_identity_takes_precedence_and_requires_no_persistent_mapping():
    research = {IDENTIFIER: source()}
    research[IDENTIFIER]["identities"] = [
        {
            "normalized": "doi:10.1234/own",
            "value": PRIMARY,
            "field": "identifier",
            "relation": "version",
        }
    ]
    dataportalen = {URI: target(description="Changed")}
    dataportalen[URI]["identities"] = [
        {"normalized": "doi:10.1234/own", "value": PRIMARY, "field": DCT + "identifier"}
    ]
    links, mappings, _ = run(research, dataportalen)
    assert links[0]["rule"] == "exact_identity"
    assert links[0]["relation"] == "version"
    assert mappings == {}


def test_many_to_one_distinctive_sources_and_one_to_many_targets_are_retained():
    other_uri = "https://data.test/b"
    other_source = "oai:researchdata.se:two/1"
    research = {
        IDENTIFIER: source(),
        other_source: source(identifier=other_source, description="Second"),
    }
    dataportalen = {URI: target(), other_uri: target(uri=other_uri)}
    dataportalen[URI]["metadata"]["nodes"][URI][DCT + "description"].append(
        {"type": "literal", "value": "Second", "xml:lang": "en"}
    )
    links, mappings, _ = run(research, dataportalen)
    assert len(links) == 3
    assert {link["status"] for link in links} == {"matched"}
    assert mappings == {IDENTIFIER: [URI, other_uri], other_source: [URI]}


@pytest.mark.parametrize(
    "description,expected",
    [
        (f"Author. Title [Data set]. {PRIMARY}", True),
        (f"Other title. {PRIMARY}", False),
        (f"{PRIMARY} Title", False),
        (f"Title\n\nOther paragraph {PRIMARY}", False),
        (f"Title {PRIMARY}extra", False),
        (f"Title {PRIMARY}?version=2", False),
        ("Title https://paper.test/citation", False),
    ],
)
def test_self_citation_requires_own_url_after_exact_title_in_same_paragraph(
    description, expected
):
    _, mappings, _ = run({IDENTIFIER: source()}, {URI: target(description=description)})
    assert bool(mappings) is expected


def test_title_alone_and_distribution_access_url_never_match():
    research = {IDENTIFIER: source()}
    research[IDENTIFIER]["identities"] = [
        {"normalized": PRIMARY, "value": PRIMARY, "field": "identifier"}
    ]
    dataportalen = {URI: target(description="Wrong abstract")}
    dataportalen[URI]["identities"] = [
        {"normalized": PRIMARY, "value": PRIMARY, "field": DCAT + "accessURL"}
    ]
    assert run(research, dataportalen)[0] == []


def test_fingerprints_and_inputs_are_deterministic_and_unmodified():
    research, dataportalen = {IDENTIFIER: source()}, {URI: target()}
    original = deepcopy((research, dataportalen))
    first = run(research, dataportalen)
    assert (research, dataportalen) == original
    assert run(research, dataportalen) == first
