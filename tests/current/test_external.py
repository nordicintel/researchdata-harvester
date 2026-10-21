# SPDX-License-Identifier: NOASSERTION
# Copyright (c) 2026 NordicIntel
"""Exercise external metadata refresh and dataset-scoped file discovery."""

import copy

import httpx
import pytest

from harvester.external import enrich_records


class FakeTransport:
    def __init__(self, handler):
        self.client = httpx.AsyncClient(
            transport=httpx.MockTransport(handler), follow_redirects=True
        )
        self.calls = []

    async def request(self, method, url, *, allowed_statuses, **kwargs):
        self.calls.append((url, kwargs))
        response = await self.client.request(method, url, **kwargs)
        if response.status_code not in allowed_statuses:
            response.raise_for_status()
        return response


def research(url, identifier="dataset"):
    return {
        identifier: {
            "identities": [{"value": url, "relation": "version"}],
            "representations": {},
        }
    }


async def test_api_metadata_is_complete_and_shared_url_is_fetched_once():
    payload = {
        "id": 1,
        "metadata": {"custom": ["sv", "en"], "access_right": "restricted"},
        "files": [
            {
                "key": "data.csv",
                "size": 30,
                "checksum": "md5:abc",
                "links": {
                    "self": "https://zenodo.org/api/records/1/files/data.csv/content"
                },
            }
        ],
    }
    transport = FakeTransport(lambda request: httpx.Response(200, json=payload))
    records = research("10.5281/zenodo.1") | research(
        "https://zenodo.org/records/1", "other"
    )
    external, files = await enrich_records(transport, records, {}, [], [])
    assert len(transport.calls) == 1
    assert external[0]["metadata"] == payload
    assert external[0]["identifiers"] == ["dataset", "other"]
    assert len(files) == 2
    assert files[0]["metadata"]["checksum"] == "md5:abc"


async def test_conditional_304_reuses_metadata_without_mutating_previous():
    previous = [
        {
            "url": "https://example.org/dataset/1",
            "status": "available",
            "metadata": {"files": []},
            "validators": {
                "etag": '"abc"',
                "last_modified": "Wed, 21 Oct 2026 10:00:00 GMT",
            },
            "identifiers": ["old"],
            "related_metadata": [],
        }
    ]
    original = copy.deepcopy(previous)

    def handler(request):
        assert request.headers["if-none-match"] == '"abc"'
        assert "if-modified-since" in request.headers
        return httpx.Response(304)

    external, _ = await enrich_records(
        FakeTransport(handler), research(previous[0]["url"]), {}, [], previous
    )
    assert external[0]["metadata"] == previous[0]["metadata"]
    assert previous == original


async def test_no_validator_resources_are_refetched_and_identical_output_is_stable():
    transport = FakeTransport(lambda request: httpx.Response(200, json={"x": [1, 2]}))
    first, _ = await enrich_records(
        transport, research("https://example.org/dataset/1"), {}, [], []
    )
    second, _ = await enrich_records(
        transport, research("https://example.org/dataset/1"), {}, [], first
    )
    assert len(transport.calls) == 2
    assert first == second


@pytest.mark.parametrize(
    "status, outcome",
    [
        (401, "restricted"),
        (403, "restricted"),
        (404, "unavailable"),
        (410, "unavailable"),
    ],
)
async def test_permanent_access_outcomes_are_explicit(status, outcome):
    external, files = await enrich_records(
        FakeTransport(lambda request: httpx.Response(status)),
        research("https://example.org/dataset/1"),
        {},
        [],
        [],
    )
    assert external[0]["status"] == outcome
    assert external[0]["http_status"] == status
    assert files == []


async def test_transient_failure_propagates_instead_of_reporting_no_files():
    with pytest.raises(httpx.HTTPStatusError):
        await enrich_records(
            FakeTransport(lambda request: httpx.Response(503)),
            research("https://example.org/dataset/1"),
            {},
            [],
            [],
        )


async def test_redirect_to_repository_uses_api_and_retains_landing_metadata():
    def handler(request):
        if request.url.host == "doi.org":
            return httpx.Response(
                302, headers={"location": "https://datarepository.kth.se/records/abc"}
            )
        if request.url.path == "/records/abc":
            return httpx.Response(
                200,
                text='<meta name="x" content="value"><script type="application/ld+json">{"name":"Dataset"}</script>',
                headers={"content-type": "text/html"},
            )
        assert request.url.path == "/api/records/abc"
        return httpx.Response(
            200,
            json={
                "custom": "full",
                "files": {
                    "entries": {
                        "a": {"key": "a", "links": {"content": "https://example.org/a"}}
                    }
                },
            },
        )

    external, files = await enrich_records(
        FakeTransport(handler), research("10.1234/example"), {}, [], []
    )
    assert len(external) == 2
    assert any(item["metadata"].get("custom") == "full" for item in external)
    assert files[0]["url"] == "https://example.org/a"


async def test_sources_do_not_crawl_citations_creators_or_historical_identity():
    record = research("https://example.org/dataset/1")
    record["dataset"]["identities"].append(
        {"value": "https://example.org/history", "relation": "dataset"}
    )
    payload = {
        "@type": "Dataset",
        "citation": {"url": "https://citation.org/"},
        "creator": {"url": "https://person.org"},
        "distribution": {"contentUrl": "https://example.org/data.csv", "name": "data"},
    }
    transport = FakeTransport(lambda request: httpx.Response(200, json=payload))
    _, files = await enrich_records(transport, record, {}, [], [])
    assert len(transport.calls) == 1
    assert files[0]["url"] == "https://example.org/data.csv"


async def test_icos_structured_metadata_supplies_file_and_retains_all_fields():
    payload = {
        "accessUrl": "https://data.icos-cp.eu/objects/a",
        "fileName": "data.zip",
        "coverageGeo": {"coordinates": [1, 2]},
        "references": {"authors": [{"name": "Author"}]},
    }
    external, files = await enrich_records(
        FakeTransport(lambda request: httpx.Response(200, json=payload)),
        research("https://meta.icos-cp.eu/objects/a"),
        {},
        [],
        [],
    )
    assert external[0]["metadata"] == payload
    assert files[0]["url"] == payload["accessUrl"]


async def test_dataportalen_distributions_and_research_files_both_survive():
    records = {
        "dataset": {
            "representations": {
                "json_ld": {
                    "@type": "Dataset",
                    "distribution": {"contentUrl": "https://example.org/a.csv"},
                }
            }
        }
    }
    dataportalen = {
        "urn:d": {
            "metadata": {
                "root": "urn:d",
                "nodes": {
                    "urn:d": {
                        "http://www.w3.org/ns/dcat#distribution": [{"value": "_:dist"}]
                    },
                    "_:dist": {
                        "http://www.w3.org/ns/dcat#downloadURL": [
                            {"value": "https://example.org/b.csv"}
                        ],
                        "http://www.w3.org/ns/dcat#accessURL": [
                            {"value": "https://example.org/dataset/1"}
                        ],
                    },
                },
            }
        }
    }
    transport = FakeTransport(
        lambda request: httpx.Response(200, json={"attributes": [1, 2]})
    )
    external, files = await enrich_records(
        transport,
        records,
        dataportalen,
        [{"identifier": "dataset", "uri": "urn:d"}],
        [],
    )
    assert [entry["url"] for entry in files] == [
        "https://example.org/a.csv",
        "https://example.org/b.csv",
    ]
    assert external[0]["metadata"] == {"attributes": [1, 2]}


async def test_linked_alternate_rdf_is_retrieved_without_crawling_its_relationships():
    def handler(request):
        if request.url.path == "/dataset/1":
            return httpx.Response(
                200,
                text='<link rel="alternate" type="text/turtle" href="/dataset/1.ttl">',
                headers={"content-type": "text/html"},
            )
        assert request.url.path == "/dataset/1.ttl"
        return httpx.Response(
            200,
            text='<https://example.org/dataset/1> <https://schema.org/name> "Name"@en; <https://schema.org/citation> <https://example.org/citation>.',
            headers={"content-type": "text/turtle"},
        )

    transport = FakeTransport(handler)
    first, _ = await enrich_records(
        transport, research("https://example.org/dataset/1"), {}, [], []
    )
    second, _ = await enrich_records(
        transport, research("https://example.org/dataset/1"), {}, [], first
    )
    assert len(first) == 2
    assert first == second
    assert len(transport.calls) == 4
    assert first[1]["metadata"][0]["https://schema.org/name"] == [
        {"@language": "en", "@value": "Name"}
    ]


async def test_researchdata_owned_pages_and_restricted_data_files_are_not_fetched():
    records = (
        research("https://researchdata.se/catalogue/dataset/1")
        | research("10.5878/example", "own-doi")
        | research("https://example.org/data.zip", "file")
    )
    transport = FakeTransport(
        lambda request: pytest.fail("Unexpected metadata request")
    )
    external, _ = await enrich_records(transport, records, {}, [], [])
    assert external == []


async def test_304_without_previous_successful_metadata_is_an_error():
    with pytest.raises(ValueError, match="without reusable metadata"):
        await enrich_records(
            FakeTransport(lambda request: httpx.Response(304)),
            research("https://example.org/dataset/1"),
            {},
            [],
            [],
        )
