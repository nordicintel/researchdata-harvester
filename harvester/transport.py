# SPDX-License-Identifier: NOASSERTION
# Copyright (c) 2026 NordicIntel
"""Share bounded HTTP requests, retry policy and observable request counts."""

import asyncio
import logging
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import Any

import httpx

from harvester.config import Config

TRANSIENT = {408, 425, 429, 500, 502, 503, 504}


class Transport:
    def __init__(self, config: Config, client: httpx.AsyncClient | None = None):
        self.config = config
        self.client = client or httpx.AsyncClient(
            timeout=httpx.Timeout(config.timeout, connect=min(15, config.timeout)),
            follow_redirects=True,
            max_redirects=8,
            headers={"User-Agent": "NordicIntel-Researchdata-Harvester/1.0"},
        )
        self.owned = client is None
        self.semaphore = asyncio.Semaphore(config.concurrency)
        self.requests = 0
        self.attempts = 0
        self.not_modified = 0
        self.bytes_received = 0
        self.client.event_hooks["request"].append(self.count_request)

    async def count_request(self, request: httpx.Request) -> None:
        self.requests += 1

    async def close(self) -> None:
        self.client.event_hooks["request"].remove(self.count_request)
        if self.owned:
            await self.client.aclose()

    @staticmethod
    def delay(response: httpx.Response | None, attempt: int) -> float:
        value = response.headers.get("Retry-After", "") if response is not None else ""
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

    async def request(
        self,
        method: str,
        url: str,
        *,
        allowed_statuses: set[int] | None = None,
        metadata_only: bool = False,
        **kwargs: Any,
    ) -> httpx.Response:
        for attempt in range(self.config.retries + 1):
            response = None
            try:
                async with self.semaphore:
                    self.attempts += 1
                    if metadata_only:
                        async with self.client.stream(
                            method, url, **kwargs
                        ) as streamed:
                            media_type = (
                                streamed.headers.get("content-type", "")
                                .split(";", 1)[0]
                                .lower()
                            )
                            is_metadata = any(
                                token in media_type
                                for token in (
                                    "json",
                                    "html",
                                    "xml",
                                    "turtle",
                                    "n-triples",
                                )
                            )
                            body = await streamed.aread() if is_metadata else b""
                            response_headers = dict(streamed.headers)
                            response_headers.pop("content-encoding", None)
                            response_headers.pop("transfer-encoding", None)
                            response = httpx.Response(
                                streamed.status_code,
                                headers=response_headers,
                                content=body,
                                request=streamed.request,
                            )
                            if not is_metadata:
                                response.extensions["metadata_body_skipped"] = True
                    else:
                        response = await self.client.request(method, url, **kwargs)
                    self.bytes_received += len(response.content)
                if (
                    response.status_code not in TRANSIENT
                    or attempt == self.config.retries
                ):
                    if response.status_code not in (allowed_statuses or set()):
                        response.raise_for_status()
                    self.not_modified += response.status_code == 304
                    return response
                reason = f"HTTP {response.status_code} from {response.url}"
            except httpx.TransportError as error:
                if attempt == self.config.retries:
                    raise
                reason = str(error) or type(error).__name__
            delay = self.delay(response, attempt)
            logging.getLogger(__name__).warning(
                "Retry %d/%d for %s in %.1fs: %s",
                attempt + 1,
                self.config.retries,
                url,
                delay,
                reason,
            )
            await asyncio.sleep(delay)
        raise RuntimeError("HTTP retries exhausted.")
