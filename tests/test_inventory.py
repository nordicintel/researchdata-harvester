# SPDX-License-Identifier: NOASSERTION
# Copyright (c) 2026 NordicIntel
"""Exercise inventory completeness and incremental selection against registry fixtures."""

from copy import deepcopy
from typing import Any

import httpx
import pytest

from harvester.dataportalen import inventory
from harvester.runtime import Config, Http
from harvester.vocabulary import NAMESPACES


def registry_entry(config: Config, identifier: str, modified: str = "2026-10-22T00:00:00Z") -> dict[str, Any]:
    """Construct the registry's RDF/JSON search envelope, including its change signal."""
    uri = f"https://data.example/dataset/{identifier}"
    entry_uri = f"{config.context_url}/entry/{identifier}"
    return {
        "contextId": "818",
        "entryId": identifier,
        "metadata": {
            uri: {
                NAMESPACES["rdf"] + "type": [{"type": "uri", "value": NAMESPACES["dcat"] + "Dataset"}],
                NAMESPACES["dct"] + "title": [{"type": "literal", "value": identifier}],
                NAMESPACES["dct"] + "modified": [{"type": "literal", "value": "1900"}],
            }
        },
        "info": {
            entry_uri: {
                NAMESPACES["es"] + "resource": [{"type": "uri", "value": uri}],
                NAMESPACES["es"] + "metadata": [
                    {"type": "uri", "value": f"{config.context_url}/metadata/{identifier}"}
                ],
                NAMESPACES["dct"] + "modified": [{"type": "literal", "value": modified}],
            }
        },
    }


def registry_transport(
    config: Config, entries: list[dict[str, Any]], members: list[str], requests: list[httpx.Request]
) -> httpx.MockTransport:
    """Serve bounded registry pages and a separate explicit membership document."""

    def respond(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if str(request.url).split("?")[0] == config.catalog_metadata_url:
            return httpx.Response(
                200,
                json={
                    config.catalog_uri: {
                        NAMESPACES["rdf"] + "type": [{"type": "uri", "value": NAMESPACES["dcat"] + "Catalog"}],
                        NAMESPACES["dcat"] + "dataset": [{"type": "uri", "value": uri} for uri in members],
                    }
                },
            )
        if request.url.path.endswith("/search"):
            offset = int(request.url.params.get("offset", "0"))
            limit = int(request.url.params["limit"])
            selected = entries
            query = request.url.params.get("query", "").replace("\\", "")
            if "resource:" in query:
                selected = [entry for entry in entries if next(iter(entry["metadata"])) in query]
            return httpx.Response(
                200,
                json={
                    "offset": offset,
                    "limit": limit,
                    "results": len(selected),
                    "resource": {"children": selected[offset : offset + limit]},
                },
            )
        for entry in entries:
            if request.url.path.endswith(f"/metadata/{entry['entryId']}"):
                return httpx.Response(200, json=entry["metadata"])
        return httpx.Response(404)

    return httpx.MockTransport(respond)


async def test_inventory_pages_and_registry_change_selection() -> None:
    config = Config(page_size=2, attempts=1)
    entries = [registry_entry(config, identifier) for identifier in ("a", "b", "c")]
    members = [next(iter(entry["metadata"])) for entry in entries]
    requests: list[httpx.Request] = []
    async with httpx.AsyncClient(transport=registry_transport(config, entries, members, requests)) as client:
        http = Http(config, client=client)
        first = await inventory(http, config, {})
        assert first["complete"] is True
        assert set(first["selected"]) == set(members)
        searches = [request for request in requests if request.url.path.endswith("/search")]
        assert [int(request.url.params["offset"]) for request in searches] == [0, 2]
        query = searches[0].url.params["query"]
        assert "public:true" in query and "NON_PUBLIC" in query
        assert "23fc4665" in query and "818" in query
        assert "distribution" not in query.lower()
        assert all(request.url.params["sort"] == "uri asc" for request in searches)

        previous = deepcopy(first["entries"])
        for entry in previous.values():
            entry.update(outcome="eligible", retry=False)
        unchanged = await inventory(http, config, previous)
        assert unchanged["selected"] == []
        # Dataset dct:modified stays unchanged: only the separate registry signal advances.
        entries[1] = registry_entry(config, "b", "2026-10-23T00:00:00Z")
        changed = await inventory(http, config, previous)
        assert changed["selected"] == [members[1]]
        full = await inventory(http, config, previous, full=True)
        assert set(full["selected"]) == set(members)


async def test_inventory_requires_membership_evidence_before_removal() -> None:
    config = Config(attempts=1)
    entries = [registry_entry(config, "a")]
    uri = next(iter(entries[0]["metadata"]))
    requests: list[httpx.Request] = []
    async with httpx.AsyncClient(transport=registry_transport(config, entries, [uri], requests)) as client:
        previous = (await inventory(Http(config, client=client), config, {}))["entries"]
    async with httpx.AsyncClient(transport=registry_transport(config, [], [], requests)) as client:
        result = await inventory(Http(config, client=client), config, previous)
        assert result["removed"] == [uri]
    # A retained catalog member cannot disappear merely because filtered search omits it.
    transport = registry_transport(config, entries, [uri], requests)

    def missing_search(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/search"):
            return httpx.Response(200, json={"offset": 0, "limit": 100, "results": 0, "resource": {"children": []}})
        return transport.handler(request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(missing_search)) as inconsistent:
        with pytest.raises(ValueError, match="(?i)inconsisten|missing|absent"):
            await inventory(Http(config, client=inconsistent), config, previous)


async def test_scoped_inventory_cannot_remove_unselected_records() -> None:
    config = Config(attempts=1)
    entries = [registry_entry(config, "a"), registry_entry(config, "b")]
    members = [next(iter(entry["metadata"])) for entry in entries]
    requests: list[httpx.Request] = []
    async with httpx.AsyncClient(transport=registry_transport(config, entries, members, requests)) as client:
        result = await inventory(Http(config, client=client), config, {}, dataset_uris=[members[0]])
    assert result["complete"] is False
    assert result["selected"] == [members[0]]
    assert result["removed"] == []
