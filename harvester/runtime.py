# SPDX-License-Identifier: NOASSERTION
# Copyright (c) 2026 NordicIntel
"""Validated settings, bounded HTTP operations and atomic temporary artifacts."""

import asyncio
import hashlib
import json
import logging
import os
import tempfile
import time
import tomllib
import zlib
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit, urlunsplit

import httpx
from pydantic import BaseModel, ConfigDict, Field
from tenacity import AsyncRetrying, retry_if_exception, stop_after_attempt, wait_exponential

logger = logging.getLogger(__name__)


class Config(BaseModel):
    """Operational settings; sizes are bytes and durations are seconds."""

    model_config = ConfigDict(extra="forbid")
    current_directory: Path = Path("data/current")
    search_url: str = "https://admin.dataportal.se/store/search"
    context_url: str = "https://admin.dataportal.se/store/818"
    catalog_metadata_url: str = "https://admin.dataportal.se/store/818/metadata/9198"
    catalog_uri: str = "https://researchdata.se/catalogue/"
    oai_url: str = "https://api.researchdata.se/oai-pmh"
    concurrency: int = Field(default=4, ge=1, le=32)
    connect_timeout: float = Field(default=10, gt=0)
    read_timeout: float = Field(default=30, gt=0)
    attempts: int = Field(default=3, ge=1, le=10)
    operation_deadline: float = Field(default=90, gt=0)
    verification_rows: int = Field(default=100, ge=1, le=100)
    verification_bytes: int = Field(default=32 * 1024 * 1024, ge=1)
    page_size: int = Field(default=100, ge=1, le=100)

    @classmethod
    def load(cls, path: Path) -> "Config":
        with path.open("rb") as stream:
            config = cls.model_validate(tomllib.load(stream))
        if not config.current_directory.is_absolute():
            config.current_directory = (path.parent / config.current_directory).resolve()
        return config


def fingerprint(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False, default=str).encode()).hexdigest()


def write_json(path: Path, value: Any) -> None:
    """Publish a complete JSON artifact without exposing partial writes."""
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = (
        json.dumps(value, sort_keys=True, ensure_ascii=False, allow_nan=False, default=str, separators=(",", ":"))
        + "\n"
    )
    if path.exists() and path.read_text(encoding="utf-8") == payload:
        return
    fd, name = tempfile.mkstemp(dir=path.parent, suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(name, path)
    finally:
        Path(name).unlink(missing_ok=True)


def read_json(path: Path, default: Any = None) -> Any:
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else default


def safe_url(url: str) -> str:
    parsed = urlsplit(url)
    host = parsed.hostname or ""
    return urlunsplit((parsed.scheme, host, parsed.path, "", ""))


def transient(error: BaseException) -> bool:
    return isinstance(error, httpx.TransportError) or (
        isinstance(error, httpx.HTTPStatusError) and error.response.status_code in {429, 500, 502, 503, 504}
    )


class Http(httpx.AsyncClient):
    """One client whose normal requests and streamed samples share bounded retries."""

    def __init__(self, config: Config, client: httpx.AsyncClient | None = None) -> None:
        super().__init__(
            timeout=httpx.Timeout(config.read_timeout, connect=config.connect_timeout),
            follow_redirects=True,
            headers={"User-Agent": "NordicIntel-Researchdata-Harvester/1.0"},
        )
        self.config = config
        self.source = client
        self.limit = asyncio.Semaphore(config.concurrency)
        self.requests = 0
        self.failures = 0
        self.started = time.monotonic()

    def retry(self) -> AsyncRetrying:
        def delay(state: Any) -> float:
            error = state.outcome.exception() if state.outcome else None
            if isinstance(error, httpx.HTTPStatusError):
                header = error.response.headers.get("retry-after")
                if header:
                    try:
                        return max(0, float(header))
                    except ValueError:
                        try:
                            return max(0, (parsedate_to_datetime(header) - datetime.now(UTC)).total_seconds())
                        except (ValueError, TypeError):
                            pass
            return float(wait_exponential(multiplier=1, max=10)(state))

        return AsyncRetrying(
            stop=stop_after_attempt(self.config.attempts), wait=delay, retry=retry_if_exception(transient), reraise=True
        )

    async def request(self, method: str, url: httpx.URL | str, **kwargs: Any) -> httpx.Response:
        try:
            async with asyncio.timeout(self.config.operation_deadline), self.limit:
                async for attempt in self.retry():
                    with attempt:
                        self.requests += 1
                        started = time.monotonic()
                        try:
                            source = self.source or self
                            async with source.stream(method, url, **kwargs) as incoming:
                                incoming.raise_for_status() if incoming.status_code in {
                                    429,
                                    500,
                                    502,
                                    503,
                                    504,
                                } else None
                                content = bytearray()
                                async for chunk in incoming.aiter_bytes():
                                    content.extend(chunk)
                                    if len(content) > self.config.verification_bytes:
                                        raise ValueError("Metadata response exceeds configured byte limit")
                                headers = dict(incoming.headers)
                                headers.pop("content-encoding", None)
                                headers.pop("content-length", None)
                                response = httpx.Response(
                                    incoming.status_code,
                                    headers=headers,
                                    content=bytes(content),
                                    request=incoming.request,
                                )
                            logger.debug(
                                "request url=%s attempt=%s status=%s seconds=%.3f",
                                safe_url(str(url)),
                                attempt.retry_state.attempt_number,
                                response.status_code,
                                time.monotonic() - started,
                            )
                            if response.status_code in {429, 500, 502, 503, 504}:
                                response.raise_for_status()
                            return response
                        except httpx.HTTPError as error:
                            self.failures += 1
                            logger.debug("request failed url=%s exception=%s", safe_url(str(url)), type(error).__name__)
                            raise
        except TimeoutError as error:
            raise httpx.ReadTimeout("HTTP operation deadline exhausted", request=httpx.Request(method, url)) from error
        raise RuntimeError("Retry loop produced no response")

    async def sample(self, url: str) -> tuple[bytes, str, str, int]:
        """Read bounded content, stopping text resources after enough complete lines."""
        try:
            async with asyncio.timeout(self.config.operation_deadline), self.limit:
                async for attempt in self.retry():
                    with attempt:
                        self.requests += 1
                        source = self.source or self
                        # stream() uses send(), so it does not reenter request()'s semaphore.
                        async with source.stream(
                            "GET", url, follow_redirects=True, headers={"Accept-Encoding": "identity"}
                        ) as response:
                            response.raise_for_status()
                            content_type = response.headers.get("content-type", "").split(";")[0]
                            is_text = (
                                any(
                                    ext in response.headers.get("content-disposition", "").lower()
                                    for ext in (".csv", ".tsv")
                                )
                                or content_type in {"text/csv", "text/tab-separated-values"}
                                or urlsplit(url).path.lower().endswith((".csv", ".tsv"))
                            )
                            decoder = (
                                zlib.decompressobj(16 + zlib.MAX_WBITS)
                                if response.headers.get("content-encoding") == "gzip"
                                else None
                            )
                            data = bytearray()
                            transferred = 0

                            async def chunks() -> AsyncIterator[bytes]:
                                if response.is_stream_consumed:
                                    yield response.content
                                else:
                                    async for block in response.aiter_raw():
                                        yield block

                            async for chunk in chunks():
                                transferred += len(chunk)
                                if transferred > self.config.verification_bytes:
                                    raise ValueError("transferred_byte_limit")
                                if decoder:
                                    chunk = decoder.decompress(chunk, self.config.verification_bytes - len(data) + 1)
                                data.extend(chunk)
                                if len(data) > self.config.verification_bytes:
                                    raise ValueError("decompressed_byte_limit")
                                if is_text and data.count(b"\n") > self.config.verification_rows + 1:
                                    break
                            if urlsplit(url).path.lower().endswith(".gz") and data[:2] == b"\x1f\x8b":
                                unzip = zlib.decompressobj(16 + zlib.MAX_WBITS)
                                data = bytearray(unzip.decompress(bytes(data), self.config.verification_bytes + 1))
                                if len(data) > self.config.verification_bytes or not unzip.eof:
                                    raise ValueError("decompressed_byte_limit")
                            return bytes(data), str(response.url), content_type, transferred
        except TimeoutError as error:
            raise httpx.ReadTimeout("HTTP sample deadline exhausted", request=httpx.Request("GET", url)) from error
        raise RuntimeError("Retry loop produced no sample")
