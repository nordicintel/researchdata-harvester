# SPDX-License-Identifier: NOASSERTION
# Copyright (c) 2026 NordicIntel
"""Ensure external metadata requests cannot consume research-file bodies."""

import gzip

import httpx
import pytest

from harvester.config import Config
from harvester.transport import Transport


class UnreadableFile(httpx.AsyncByteStream):
    async def __aiter__(self):
        pytest.fail("A research-file body was consumed by metadata retrieval.")
        yield b""


async def test_metadata_request_skips_binary_body():
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(
                200,
                headers={"Content-Type": "application/zip"},
                stream=UnreadableFile(),
            )
        )
    ) as client:
        transport = Transport(Config(), client)
        response = await transport.request(
            "GET", "https://example.org/resource", metadata_only=True
        )
        assert response.extensions["metadata_body_skipped"]
        assert transport.bytes_received == 0


async def test_compressed_metadata_is_decoded_once():
    payload = b'{"name":"dataset"}'
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(
                200,
                content=gzip.compress(payload),
                headers={
                    "Content-Type": "application/json",
                    "Content-Encoding": "gzip",
                },
            )
        )
    ) as client:
        transport = Transport(Config(), client)
        response = await transport.request(
            "GET", "https://example.org/metadata", metadata_only=True
        )
        assert response.json() == {"name": "dataset"}


async def test_retry_after_is_honored(monkeypatch):
    sleeps = []

    async def sleep(delay):
        sleeps.append(delay)

    monkeypatch.setattr("harvester.transport.asyncio.sleep", sleep)
    attempts = 0

    def respond(request):
        nonlocal attempts
        attempts += 1
        return (
            httpx.Response(429, headers={"Retry-After": "2"})
            if attempts == 1
            else httpx.Response(200, json={})
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        await Transport(Config(), client).request("GET", "https://example.org")
    assert sleeps == [2]


async def test_request_metrics_include_redirects_and_detach_from_shared_client():
    def respond(request):
        if request.url.path == "/doi":
            return httpx.Response(302, headers={"Location": "/dataset"})
        return httpx.Response(200, json={})

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(respond), follow_redirects=True
    ) as client:
        transport = Transport(Config(), client)
        await transport.request("GET", "https://example.org/doi", metadata_only=True)
        assert transport.requests == 2
        assert transport.attempts == 1
        await transport.close()
        await client.get("https://example.org/dataset")
        assert transport.requests == 2
