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

    async def request(
        self, method, url, *, allowed_statuses, metadata_only=False, **kwargs
    ):
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


@pytest.mark.parametrize("format", ["json", "rdf"])
async def test_external_graph_files_belong_only_to_current_dataset(format):
    current = "https://example.org/current"
    if format == "json":
        response = httpx.Response(
            200,
            json={
                "@graph": [
                    {
                        "@id": current,
                        "@type": "Dataset",
                        "distribution": {"@id": "_:file"},
                    },
                    {
                        "@id": "_:file",
                        "@type": "DataDownload",
                        "contentUrl": "https://example.org/current.csv",
                    },
                    {
                        "@id": "https://example.org/old",
                        "@type": "Dataset",
                        "distribution": {"contentUrl": "https://example.org/old.csv"},
                    },
                ]
            },
        )
    else:
        response = httpx.Response(
            200,
            text="""@prefix dcat: <http://www.w3.org/ns/dcat#> .
<https://example.org/current> dcat:distribution [dcat:downloadURL <https://example.org/current.csv>];
<https://schema.org/citation> <https://example.org/old> .
<https://example.org/old> dcat:distribution [dcat:downloadURL <https://example.org/old.csv>] .""",
            headers={"content-type": "text/turtle"},
        )
    external, files = await enrich_records(
        FakeTransport(lambda request: response), research(current), {}, [], []
    )
    assert [entry["url"] for entry in files] == ["https://example.org/current.csv"]
    assert "old.csv" in str(external[0]["metadata"])


async def test_restricted_repository_api_uses_landing_metadata_fallback():
    def handler(request):
        if "/api/" in request.url.path:
            return httpx.Response(403)
        return httpx.Response(
            200,
            text='<script type="application/ld+json">{"@type":"Dataset","distribution":{"contentUrl":"https://example.org/data.csv"}}</script>',
            headers={"content-type": "text/html"},
        )

    transport = FakeTransport(handler)
    external, files = await enrich_records(
        transport, research("10.5281/zenodo.1"), {}, [], []
    )
    assert len(transport.calls) == 2
    assert {entry["status"] for entry in external} == {"restricted", "available"}
    assert [entry["url"] for entry in files] == ["https://example.org/data.csv"]


async def test_ambiguous_links_do_not_attribute_dataportalen_files():
    dataportalen = {
        "urn:d": {
            "uri": "urn:d",
            "metadata": {
                "root": "urn:d",
                "nodes": {
                    "urn:d": {
                        "http://www.w3.org/ns/dcat#downloadURL": [
                            {"type": "uri", "value": "https://example.org/data.csv"}
                        ]
                    }
                },
            },
        }
    }
    _, files = await enrich_records(
        FakeTransport(lambda request: pytest.fail("Unexpected request")),
        {"r": {"representations": {}}},
        dataportalen,
        [{"identifier": "r", "uri": "urn:d", "status": "ambiguous"}],
        [],
    )
    assert files == []


async def test_doi_resolution_requests_landing_page_without_crosscite_negotiation():
    def handler(request):
        assert request.headers["accept"] == "text/html"
        return httpx.Response(
            200,
            text='<meta name="citation_title" content="Dataset">',
            headers={"content-type": "text/html"},
        )

    external, _ = await enrich_records(
        FakeTransport(handler), research("10.1234/current"), {}, [], []
    )
    assert external[0]["status"] == "available"


@pytest.mark.parametrize("wrapped", [True, False])
async def test_single_different_external_dataset_never_owns_current_files(wrapped):
    dataset = {
        "@type": "Dataset",
        "@id": "https://example.org/older",
        "distribution": {"contentUrl": "https://example.org/older.csv"},
    }
    payload = {"@graph": [dataset]} if wrapped else dataset
    external, files = await enrich_records(
        FakeTransport(lambda request: httpx.Response(200, json=payload)),
        research("https://example.org/current"),
        {},
        [],
        [],
    )
    assert files == []
    assert external[0]["metadata"] == payload


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
                        "http://www.w3.org/ns/dcat#distribution": [
                            {"type": "bnode", "value": "dist"}
                        ]
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
    assert first[1]["metadata"]["nodes"]["https://example.org/dataset/1"][
        "https://schema.org/name"
    ] == [{"xml:lang": "en", "type": "literal", "value": "Name"}]


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


async def test_rdf_literals_and_multiple_download_links_survive():
    content = """@prefix dcat: <http://www.w3.org/ns/dcat#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
<https://example.org/dataset/1> dcat:downloadURL <https://example.org/a.csv>, <https://example.org/b.csv>;
<https://example.org/code> "001"^^xsd:integer ."""
    transport = FakeTransport(
        lambda request: httpx.Response(
            200, text=content, headers={"content-type": "text/turtle"}
        )
    )
    external, files = await enrich_records(
        transport, research("https://example.org/dataset/1"), {}, [], []
    )
    terms = external[0]["metadata"]["nodes"]["https://example.org/dataset/1"]
    assert terms["https://example.org/code"] == [
        {
            "type": "literal",
            "value": "001",
            "datatype": "http://www.w3.org/2001/XMLSchema#integer",
        }
    ]
    assert {entry["url"] for entry in files} == {
        "https://example.org/a.csv",
        "https://example.org/b.csv",
    }


async def test_xml_dcat_distributions_are_processed():
    from defusedxml.ElementTree import fromstring

    from harvester.oai import xml_tree

    tree = xml_tree(
        fromstring("""<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#" xmlns:dcat="http://www.w3.org/ns/dcat#">
<dcat:Dataset><dcat:distribution><dcat:Distribution><dcat:downloadURL rdf:resource="https://example.org/data.csv"/><dcat:accessURL rdf:resource="https://example.org/metadata"/></dcat:Distribution></dcat:distribution></dcat:Dataset></rdf:RDF>""")
    )
    transport = FakeTransport(
        lambda request: httpx.Response(200, json={"name": "More metadata"})
    )
    external, files = await enrich_records(
        transport, {"r": {"representations": {"dcat": tree}}}, {}, [], []
    )
    assert [record["url"] for record in external] == ["https://example.org/metadata"]
    assert [record["url"] for record in files] == ["https://example.org/data.csv"]


async def test_jsonld_graph_uses_datacite_owner_for_files_and_metadata_resources():
    distribution = {
        "@type": "DataDownload",
        "contentUrl": "https://files.test/owner.csv",
        "encodingFormat": ["text/csv", "application/csv"],
        "description": [{"@value": "Data", "@language": "en"}],
    }
    graph = {
        "@graph": [
            {
                "@type": "Dataset",
                "identifier": "10.5878/cited",
                "url": "https://metadata.test/cited",
                "distribution": {"contentUrl": "https://files.test/cited.csv"},
            },
            {
                "@type": "Dataset",
                "identifier": {"value": "https://doi.org/10.5878/OWNER"},
                "url": "https://metadata.test/owner",
                "distribution": distribution,
                "citation": {"@id": "https://metadata.test/cited"},
            },
        ]
    }
    before = copy.deepcopy(graph)
    records = research("10.5878/owner")
    records["dataset"]["identities"][0]["format"] = "datacite"
    records["dataset"]["representations"] = {"json_ld": graph}

    def handler(request):
        assert str(request.url) == "https://metadata.test/owner"
        return httpx.Response(200, json={"name": ["Metadata", "Metadata"]})

    transport = FakeTransport(handler)
    external, files = await enrich_records(transport, records, {}, [], [])
    assert [item["url"] for item in external] == ["https://metadata.test/owner"]
    assert external[0]["metadata"] == {"name": ["Metadata", "Metadata"]}
    assert [item["url"] for item in files] == [distribution["contentUrl"]]
    assert files[0]["metadata"]["raw"] == distribution
    assert len(transport.calls) == 1
    assert graph == before


@pytest.mark.parametrize("graph_identifier", ["10.5878/related", "10.5878/owner"])
async def test_ambiguous_jsonld_graph_does_not_assign_files_or_resources(
    graph_identifier: str,
):
    records = research("10.5878/owner")
    records["dataset"]["identities"][0]["format"] = "datacite"
    records["dataset"]["representations"] = {
        "json_ld": {
            "@graph": [
                {
                    "@type": "Dataset",
                    "identifier": identifier,
                    "url": f"https://metadata.test/{index}",
                    "distribution": {"contentUrl": f"https://files.test/{index}.csv"},
                }
                for index, identifier in enumerate([graph_identifier] * 2)
            ]
        }
    }
    transport = FakeTransport(
        lambda request: pytest.fail("Unexpected metadata request")
    )
    external, files = await enrich_records(transport, records, {}, [], [])
    assert external == []
    assert files == []
