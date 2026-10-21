# SPDX-License-Identifier: NOASSERTION
# Copyright (c) 2026 NordicIntel
"""Offline source fixtures protect current membership and metadata fidelity."""

import json
from copy import deepcopy
from types import SimpleNamespace
from xml.sax.saxutils import escape

import httpx
import pytest
from defusedxml.ElementTree import fromstring

from harvester.metadata import jsonld_documents
from harvester.oai import OAIClient, OAIError, xml_tree
from harvester.researchdata import FORMATS, harvest_researchdata, make_record

NAMESPACE = 'xmlns="http://www.openarchives.org/OAI/2.0/"'
CONFIG = SimpleNamespace(
    oai_url="https://api.test/oai", sitemap_url="https://site.test/sitemap.xml"
)


def header(identifier, stamp="2026-10-19", deleted=False):
    status = ' status="deleted"' if deleted else ""
    return f"<header{status}><identifier>{identifier}</identifier><datestamp>{stamp}</datestamp><setSpec>catalog</setSpec></header>"


def record(identifier, prefix, stamp="2026-10-19"):
    payload = '<resource xmlns="http://datacite.org/schema/kernel-4"><identifier identifierType="DOI">10.1234/identity</identifier><titles><title xml:lang="sv">Dataset</title><title xml:lang="en">Data</title></titles></resource>'
    if prefix == "json_ld":
        payload = (
            "<jsonLdString>"
            + escape(
                json.dumps(
                    {
                        "@type": "Dataset",
                        "identifier": "10.1234/identity",
                        "name": "Data",
                    }
                )
            )
            + "</jsonLdString>"
        )
    return f"<record>{header(identifier, stamp)}<metadata>{payload}</metadata></record>"


def envelope(verb, content, token=None, total=None):
    cursor = (
        ""
        if token is None and total is None
        else f'<resumptionToken completeListSize="{total}">{token or ""}</resumptionToken>'
    )
    return f"<OAI-PMH {NAMESPACE}><{verb}>{content}{cursor}</{verb}></OAI-PMH>"


def sitemap(*identifiers):
    return (
        '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">'
        + "".join(
            f"<url><loc>https://site.test/catalogue/dataset/{identifier.split(':')[-1].split('/')[0]}</loc></url>"
            for identifier in identifiers
        )
        + "</urlset>"
    )


class FixtureTransport:
    def __init__(self, handler):
        self.handler = handler
        self.calls = []

    async def request(self, method, url, **kwargs):
        params = kwargs.get("params", {})
        self.calls.append((str(url), dict(params)))
        result = self.handler(str(url), params)
        if isinstance(result, Exception):
            raise result
        return httpx.Response(
            200, text=result, request=httpx.Request(method, str(url), params=params)
        )


@pytest.mark.asyncio
async def test_full_three_streams_paginate_and_fetch_missing_format():
    first, second = "oai:researchdata.se:a/1", "oai:researchdata.se:b/2"

    def handler(url, params):
        if url == CONFIG.sitemap_url:
            return sitemap(first, second)
        verb = params["verb"]
        if verb == "GetRecord":
            return envelope(
                verb, record(params["identifier"], params["metadataPrefix"])
            )
        if verb == "ListIdentifiers":
            if params.get("resumptionToken"):
                return envelope(verb, header(second), "", 2)
            return envelope(verb, header(first), "inventory-next", 2)
        prefix = params.get("metadataPrefix")
        if params.get("resumptionToken"):
            prefix = params["resumptionToken"].split(":")[0]
            return envelope(verb, record(second, prefix), "", 2)
        if prefix == "dcat":
            return envelope(verb, record(first, prefix), "", 1)
        return envelope(verb, record(first, prefix), prefix + ":next", 2)

    transport = FixtureTransport(handler)
    records, report = await harvest_researchdata(transport, CONFIG, {}, None, full=True)
    assert set(records) == {first, second}
    assert set(records[second]["representations"]) == set(FORMATS)
    assert (
        jsonld_documents(records[first]["representations"]["json_ld"])[0]["identifier"]
        == "10.1234/identity"
    )
    assert report["get_records"] == 1
    assert report["streams"] == {"datacite": 2, "dcat": 1, "json_ld": 2}
    continuation = [
        params for _, params in transport.calls if "resumptionToken" in params
    ]
    assert all(set(params) == {"verb", "resumptionToken"} for params in continuation)


@pytest.mark.asyncio
async def test_incremental_overlap_reuses_unchanged_adds_old_timestamp_and_removes_absent():
    unchanged, added, removed = (
        "oai:researchdata.se:a/1",
        "oai:researchdata.se:b/1",
        "oai:researchdata.se:removed/1",
    )
    previous = {
        key: make_record(
            key,
            "2026-10-19",
            [],
            {prefix: xml_tree(fromstring("<resource/>")) for prefix in FORMATS},
        )
        for key in (unchanged, removed)
    }
    before = deepcopy(previous)

    def handler(url, params):
        if url == CONFIG.sitemap_url:
            return sitemap(unchanged, added)
        verb = params["verb"]
        if verb == "ListRecords":
            assert params["from"] == "2026-10-19"
            return (
                "<OAI-PMH "
                + NAMESPACE
                + '><error code="noRecordsMatch">None</error></OAI-PMH>'
            )
        if verb == "ListIdentifiers":
            assert "from" not in params
            return envelope(verb, header(unchanged) + header(added))
        assert params["identifier"] == added
        return envelope(verb, record(added, params["metadataPrefix"]))

    transport = FixtureTransport(handler)
    records, report = await harvest_researchdata(
        transport, CONFIG, previous, "2026-10-20"
    )
    assert set(records) == {unchanged, added}
    assert report["reused"] == 1
    assert report["get_records"] == 3
    assert report["removed"] == [removed]
    assert previous == before


@pytest.mark.asyncio
async def test_incomplete_inventory_never_returns_candidate_or_mutates_previous():
    identifier = "oai:researchdata.se:a/1"
    previous = {identifier: {"datestamp": "2026-10-19", "sentinel": [1]}}
    before = deepcopy(previous)

    def handler(url, params):
        if params["verb"] == "ListRecords":
            return envelope("ListRecords", record(identifier, params["metadataPrefix"]))
        if params.get("resumptionToken"):
            return RuntimeError("Source interrupted")
        return envelope("ListIdentifiers", header(identifier), "next", 2)

    with pytest.raises(RuntimeError, match="Source interrupted"):
        await harvest_researchdata(FixtureTransport(handler), CONFIG, previous, None)
    assert previous == before


@pytest.mark.asyncio
async def test_repeated_token_fails_and_expired_token_restarts_with_bounded_attempts():
    identifier = "oai:researchdata.se:a/1"
    transport = FixtureTransport(
        lambda url, params: envelope("ListIdentifiers", header(identifier), "repeat", 2)
    )
    with pytest.raises(ValueError, match="repeated a resumption token"):
        async for _ in OAIClient(transport, CONFIG.oai_url).iter_pages(
            "ListIdentifiers"
        ):
            pass
    starts = 0

    def expired(url, params):
        nonlocal starts
        if params["verb"] == "ListRecords":
            return envelope("ListRecords", "")
        if "resumptionToken" not in params:
            starts += 1
            return envelope("ListIdentifiers", header(identifier), "expired", 2)
        return (
            "<OAI-PMH "
            + NAMESPACE
            + '><error code="badResumptionToken">Expired</error></OAI-PMH>'
        )

    with pytest.raises(OAIError, match="badResumptionToken"):
        await harvest_researchdata(FixtureTransport(expired), CONFIG, {}, None)
    assert starts == 3


@pytest.mark.asyncio
async def test_sitemap_missing_inventory_recovers_only_explicit_current_identifier():
    identifier = "oai:researchdata.se:a/7"

    def handler(url, params):
        if url == CONFIG.sitemap_url:
            return sitemap(identifier)
        if url.startswith("https://site.test/catalogue/"):
            return '<a href="/catalogue/dataset/a/7/export/datacite">Export</a>'
        if params["verb"] == "GetRecord":
            return envelope("GetRecord", record(identifier, params["metadataPrefix"]))
        return envelope(params["verb"], "")

    records, report = await harvest_researchdata(
        FixtureTransport(handler), CONFIG, {}, None
    )
    assert set(records) == {identifier}
    assert report["recovered"] == [identifier]


def test_json_identity_excludes_citations_creators_and_distributions_and_preserves_xml():
    document = {
        "@type": "Dataset",
        "identifier": "10.1234/primary",
        "citation": {"identifier": "10.1234/cited"},
        "creator": {"@id": "https://person.test/id"},
        "distribution": {"@id": "https://files.test/data.csv"},
    }
    tree = xml_tree(
        fromstring(
            "<root><jsonLdString>"
            + escape(json.dumps(document))
            + '</jsonLdString><relatedIdentifier relationType="IsCitedBy">10.1234/citation</relatedIdentifier><relatedIdentifier relationType="IsVersionOf">10.1234/base</relatedIdentifier><title xml:lang="sv">Titel</title><title xml:lang="en">Title</title></root>'
        )
    )
    tree["children"][0].pop("json")
    current = make_record(
        "oai:researchdata.se:abc/9", "2026-10-19", ["z", "a"], {"json_ld": tree}
    )
    assert current["dataset_id"] == "abc"
    assert {item["normalized"] for item in current["identities"]} == {
        "doi:10.1234/primary",
        "doi:10.1234/base",
    }
    assert current["representations"]["json_ld"]["children"][0]["json"] == document
    assert current["summary"]["titles"] == [
        {"value": "Titel", "language": "sv"},
        {"value": "Title", "language": "en"},
    ]
    assert "json" not in tree["children"][0]


@pytest.mark.asyncio
async def test_unavailable_representation_is_explicit_and_keeps_current_record():
    identifier = "oai:researchdata.se:a/1"

    def handler(url, params):
        if url == CONFIG.sitemap_url:
            return sitemap(identifier)
        verb = params["verb"]
        prefix = params.get("metadataPrefix")
        if verb == "ListIdentifiers":
            return envelope(verb, header(identifier))
        if prefix == "dcat":
            if verb == "GetRecord":
                return (
                    "<OAI-PMH "
                    + NAMESPACE
                    + '><error code="cannotDisseminateFormat">Not available</error></OAI-PMH>'
                )
            return envelope(verb, "")
        return envelope(verb, record(identifier, prefix))

    records, report = await harvest_researchdata(
        FixtureTransport(handler), CONFIG, {}, None
    )
    assert records[identifier]["representations"]["dcat"] is None
    assert report["source_outcomes"] == [
        {
            "identifier": identifier,
            "format": "dcat",
            "status": "unavailable",
            "code": "cannotDisseminateFormat",
        }
    ]


@pytest.mark.asyncio
async def test_inventory_modification_refreshes_all_formats_when_changed_feed_misses_it():
    identifier = "oai:researchdata.se:a/1"
    previous = {
        identifier: make_record(
            identifier,
            "2026-10-18",
            [],
            {prefix: xml_tree(fromstring("<old/>")) for prefix in FORMATS},
        )
    }

    def handler(url, params):
        if url == CONFIG.sitemap_url:
            return sitemap(identifier)
        verb = params["verb"]
        if verb == "ListRecords":
            return envelope(verb, "")
        if verb == "ListIdentifiers":
            return envelope(verb, header(identifier))
        return envelope(verb, record(identifier, params["metadataPrefix"]))

    records, report = await harvest_researchdata(
        FixtureTransport(handler), CONFIG, previous, "2026-10-20"
    )
    assert records[identifier]["datestamp"] == "2026-10-19"
    assert report["get_records"] == 3
    assert all(
        tree["tag"] != "old" for tree in records[identifier]["representations"].values()
    )


@pytest.mark.asyncio
async def test_expired_inventory_discards_partial_membership_on_restart():
    old, current = "oai:researchdata.se:old/1", "oai:researchdata.se:current/1"
    starts = 0

    def handler(url, params):
        nonlocal starts
        if url == CONFIG.sitemap_url:
            return sitemap(current)
        verb = params["verb"]
        if verb == "ListRecords":
            return envelope(verb, record(current, params["metadataPrefix"]))
        if params.get("resumptionToken"):
            return (
                "<OAI-PMH "
                + NAMESPACE
                + '><error code="badResumptionToken">Expired</error></OAI-PMH>'
            )
        starts += 1
        if starts == 1:
            return envelope(verb, header(old), "expired", 2)
        return envelope(verb, header(current))

    records, report = await harvest_researchdata(
        FixtureTransport(handler), CONFIG, {}, None
    )
    assert set(records) == {current}
    assert report["records"] == 1
    assert starts == 2


@pytest.mark.parametrize("owner_field", ["identifier", "@id"])
def test_graph_identity_uses_datacite_owner_and_preserves_complete_metadata(
    owner_field: str,
):
    document = {
        "@context": "https://schema.org",
        "@graph": [
            {
                "@type": "Dataset",
                "identifier": "10.1234/cited",
                "url": "https://cited.test/dataset",
                "name": "Cited dataset",
            },
            {
                "@type": ["Dataset", "CreativeWork"],
                owner_field: {"@id": "https://doi.org/10.1234/OWNER"},
                "sameAs": ["https://owner.test/dataset"],
                "name": [
                    {"@value": "Data", "@language": "en"},
                    {"@value": "Data", "@language": "sv"},
                ],
                "keywords": ["temperature", "temperature", "climate"],
                "citation": {"identifier": "10.1234/cited"},
            },
        ],
    }
    datacite = xml_tree(
        fromstring(
            '<resource><identifier identifierType="DOI">10.1234/owner</identifier>'
            '<titles><title xml:lang="en">Data</title>'
            '<title xml:lang="sv">Data</title></titles>'
            "<subjects><subject>climate</subject><subject>climate</subject></subjects>"
            '<relatedIdentifiers><relatedIdentifier relationType="IsCitedBy">'
            "10.1234/cited</relatedIdentifier></relatedIdentifiers></resource>"
        )
    )
    representations = {
        "datacite": datacite,
        "json_ld": xml_tree(
            fromstring(
                "<root><jsonLdString>"
                + escape(json.dumps(document))
                + "</jsonLdString></root>"
            )
        ),
    }
    before = deepcopy(representations)
    current = make_record(
        "oai:researchdata.se:owner/1", "2026-10-19", [], representations
    )
    assert {item["normalized"] for item in current["identities"]} == {
        "doi:10.1234/owner",
        "https://owner.test/dataset",
    }
    assert jsonld_documents(current["representations"]["json_ld"]) == [document]
    assert current["representations"]["datacite"] == datacite
    assert current["summary"]["titles"] == [
        {"value": "Data", "language": "en"},
        {"value": "Data", "language": "sv"},
    ]
    assert representations == before


@pytest.mark.parametrize(
    "datacite_identifier", [None, "10.1234/unmatched", "10.1234/shared"]
)
def test_unresolved_graph_datasets_do_not_supply_identity(
    datacite_identifier: str | None,
):
    document = {
        "@graph": [
            {"@type": "Dataset", "identifier": "10.1234/shared", "url": url}
            for url in ["https://first.test/dataset", "https://second.test/dataset"]
        ]
    }
    representations = {
        "json_ld": xml_tree(
            fromstring(
                "<root><jsonLdString>"
                + escape(json.dumps(document))
                + "</jsonLdString></root>"
            )
        )
    }
    if datacite_identifier:
        representations["datacite"] = xml_tree(
            fromstring(
                f"<resource><identifier>{datacite_identifier}</identifier></resource>"
            )
        )
    current = make_record(
        "oai:researchdata.se:owner/1", "2026-10-19", [], representations
    )
    assert all(item["format"] == "datacite" for item in current["identities"])
    assert jsonld_documents(current["representations"]["json_ld"]) == [document]
