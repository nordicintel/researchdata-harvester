# SPDX-License-Identifier: NOASSERTION
# Copyright (c) 2026 NordicIntel
"""Shared bounded asynchronous HTTP transport with transient failure retries."""

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import Any

import httpx

from researchdata_harvester.config import Config

TRANSIENT = {408, 425, 429, 500, 502, 503, 504}


class Transport:
    def __init__(self, config: Config, client: httpx.AsyncClient | None = None):
        self.config = config
        self.client = client or httpx.AsyncClient(
            timeout=httpx.Timeout(config.timeout, connect=min(15, config.timeout)),
            follow_redirects=True,
            max_redirects=8,
            headers={"User-Agent": "researchdata-harvester/0.1.0"},
        )
        self.owned = client is None
        self.semaphore = asyncio.Semaphore(config.concurrency)

    async def close(self) -> None:
        if self.owned:
            await self.client.aclose()

    @staticmethod
    def delay(response: httpx.Response | None, attempt: int) -> float:
        value = response.headers.get("Retry-After", "") if response else ""
        try:
            return max(0, float(value))
        except ValueError:
            try:
                return max(
                    0,
                    (parsedate_to_datetime(value) - datetime.now(UTC)).total_seconds(),
                )
            except (ValueError, TypeError, OverflowError):
                return min(30, 2**attempt)

    async def request(self, method: str, url: str, **kwargs: Any) -> httpx.Response:
        for attempt in range(self.config.retries + 1):
            response = None
            try:
                async with self.semaphore:
                    response = await self.client.request(method, url, **kwargs)
                if (
                    response.status_code not in TRANSIENT
                    or attempt == self.config.retries
                ):
                    response.raise_for_status()
                    return response
            except httpx.TransportError:
                if attempt == self.config.retries:
                    raise
            await asyncio.sleep(self.delay(response, attempt))
        raise RuntimeError("HTTP retries exhausted.")

    @asynccontextmanager
    async def stream(self, url: str) -> AsyncIterator[httpx.Response]:
        """Retry connection/status failures before yielding a streaming body.

        Interrupted bodies are handled by the downloader, which discards partial files.
        """
        for attempt in range(self.config.retries + 1):
            response = None
            try:
                async with self.semaphore:
                    async with self.client.stream("GET", url) as response:
                        if (
                            response.status_code not in TRANSIENT
                            or attempt == self.config.retries
                        ):
                            response.raise_for_status()
                            yield response
                            return
            except httpx.TransportError:
                if response is not None or attempt == self.config.retries:
                    raise
            await asyncio.sleep(self.delay(response, attempt))
