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
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager, nullcontext
from contextvars import ContextVar
from copy import copy
from datetime import UTC, datetime
from email.message import Message
from email.utils import parsedate_to_datetime
from pathlib import Path
from types import TracebackType
from typing import Any
from urllib.parse import urlsplit, urlunsplit

import httpx
from pydantic import BaseModel, ConfigDict, Field
from tenacity import AsyncRetrying, retry_if_exception, stop_after_attempt, wait_exponential

logger = logging.getLogger(__name__)
dataset_context: ContextVar[str] = ContextVar("dataset_uri", default="-")


class RecordError(RuntimeError):
    """Expected source failure confined to one record or document."""

    def __init__(self, reason: str, *, url: str = "", retry: bool = False) -> None:
        super().__init__(reason)
        self.url = safe_url(url) if url else ""
        self.retry = retry


def record_key(plan: dict[str, Any], entry: dict[str, Any]) -> str:
    """Identify source inputs independently of previous processing outcomes."""
    return fingerprint(
        [
            plan["implementation"],
            plan["configuration"],
            {
                key: entry.get(key)
                for key in (
                    "dataset_uri",
                    "registry_entry_uri",
                    "metadata_url",
                    "registry_modified_value",
                    "fingerprint",
                )
            },
        ]
    )


def validate_result(result: Any, stage: str, uri: str) -> None:
    """Reject corrupt terminal artifacts rather than treating them as source failures."""
    if not isinstance(result, dict) or result.get("status") not in {"succeeded", "failed"}:
        raise ValueError(f"Invalid {stage} result for {uri}")
    if result["status"] == "failed":
        if (
            result.get("dataset_uri") != uri
            or result.get("stage") != stage
            or not all(isinstance(result.get(key), str) for key in ("reason", "operation", "url", "category"))
            or not isinstance(result.get("retry"), bool)
        ):
            raise ValueError(f"Incomplete {stage} failure result for {uri}")
    elif stage == "dataportalen":
        if not isinstance(result.get("documents", result.get("row")), (list, dict)):
            raise ValueError(f"Incomplete Dataportalen result for {uri}")
    elif result.get("outcome") not in {"eligible", "unavailable", "unresolved", "excluded"}:
        raise ValueError(f"Incomplete Researchdata result for {uri}")


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
    retry_initial_delay: float = Field(default=5, ge=0, le=60)
    retry_max_delay: float = Field(default=60, ge=1, le=600)
    source_failure_limit: int = Field(default=6, ge=3, le=100)
    session_reset_delay: float = Field(default=60, ge=0, le=600)
    verification_rows: int = Field(default=100, ge=1, le=100)
    verification_bytes: int = Field(default=8 * 1024 * 1024, ge=1)
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


def failing_host(error: BaseException | None, url: str) -> str:
    """The host that produced an error, which after a redirect is not the requested one."""
    if isinstance(error, httpx.HTTPError):
        try:
            return error.request.url.netloc.decode()
        except RuntimeError:
            pass
    return urlsplit(url).netloc


class Http:
    """One client whose normal requests and streamed samples share bounded retries."""

    def __init__(self, config: Config, client: httpx.AsyncClient | None = None) -> None:
        self.config = config
        self._owns_client = client is None
        self.source = client or self._new_client()
        self.source.event_hooks["request"].append(self._refuse_unavailable)
        self._session_condition = asyncio.Condition()
        self._active = 0
        self._resetting = False
        self._reset_pending = False
        self.limit = asyncio.Semaphore(config.concurrency)
        self.requests = 0
        self.failures = 0
        self.started = time.monotonic()
        self.source_failures: dict[str, int] = {}
        self._resume_at: dict[str, float] = {}
        self._source_gates: dict[str, asyncio.Lock] = {}

    def _new_client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(
            timeout=httpx.Timeout(self.config.read_timeout, connect=self.config.connect_timeout),
            follow_redirects=True,
            headers={"User-Agent": "NordicIntel-Researchdata-Harvester/1.0"},
        )

    async def __aenter__(self) -> "Http":
        return self

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc: BaseException | None, tb: TracebackType | None
    ) -> None:
        if self._owns_client:
            await self.source.aclose()

    def build_request(self, method: str, url: str, **kwargs: Any) -> httpx.Request:
        return self.source.build_request(method, url, **kwargs)

    async def get(self, url: str, **kwargs: Any) -> httpx.Response:
        return await self.request("GET", url, **kwargs)

    async def _refuse_unavailable(self, request: httpx.Request) -> None:
        if self.source_failures.get(request.url.netloc.decode(), 0) >= self.config.source_failure_limit:
            raise RecordError("source_unavailable_after_recovery_attempts", url=str(request.url), retry=True)

    async def _reset_session(self) -> None:
        async with self._session_condition:
            await self._session_condition.wait_for(lambda: not self._resetting)
            if not self._reset_pending:
                return
            self._resetting = True
            try:
                await self._session_condition.wait_for(lambda: self._active == 0)
                if self._owns_client:
                    await self.source.aclose()
                    self.source = self._new_client()
                    self.source.event_hooks["request"].append(self._refuse_unavailable)
                self._reset_pending = False
                logger.warning("HTTP session restarted after consecutive timeouts; source recovery pause follows")
            finally:
                self._resetting = False
                self._session_condition.notify_all()

    @asynccontextmanager
    async def attempt(self, url: str, number: int, deadline: float) -> AsyncIterator[None]:
        """Coordinate recovery per origin while other sources remain available."""
        origin = urlsplit(url).netloc
        recovering = bool(self.source_failures.get(origin))
        gate = self._source_gates.setdefault(origin, asyncio.Lock())
        async with gate if recovering else nullcontext():
            if self.source_failures.get(origin, 0) >= self.config.source_failure_limit:
                raise RecordError("source_unavailable_after_recovery_attempts", url=url, retry=True)
            delay = max(0, self._resume_at.get(origin, 0) - time.monotonic())
            if delay:
                if delay >= deadline - time.monotonic():
                    raise RecordError("source_backoff_exceeds_operation_deadline", url=url, retry=True)
                logger.warning(
                    "source backoff host=%s wait_seconds=%.1f dataset=%s", origin, delay, dataset_context.get()
                )
                await asyncio.sleep(delay)
            async with self.limit:
                async with self._session_condition:
                    await self._session_condition.wait_for(lambda: not self._resetting)
                    self._active += 1
                self.requests += 1
                started = time.monotonic()
                logger.debug("request start dataset=%s url=%s attempt=%s", dataset_context.get(), safe_url(url), number)
                try:
                    yield
                except httpx.HTTPError as error:
                    self.failures += 1
                    host = failing_host(error, url)
                    if transient(error):
                        self.source_failures[host] = self.source_failures.get(host, 0) + 1
                        if isinstance(error, httpx.TimeoutException) and self.source_failures[host] == 2:
                            self._reset_pending = True
                        if self.source_failures[host] == self.config.source_failure_limit:
                            logger.error(
                                "source unavailable host=%s consecutive_failures=%s; deferring remaining requests for retry",
                                host,
                                self.source_failures[host],
                            )
                    else:
                        self.source_failures.pop(host, None)
                        self._resume_at.pop(host, None)
                    logger.warning(
                        "request failed dataset=%s url=%s host=%s attempt=%s exception=%s host_failures=%s seconds=%.3f",
                        dataset_context.get(),
                        safe_url(url),
                        host,
                        number,
                        type(error).__name__,
                        self.source_failures.get(host, 0),
                        time.monotonic() - started,
                    )
                    raise
                else:
                    self.source_failures.pop(origin, None)
                    self._resume_at.pop(origin, None)
                finally:
                    async with self._session_condition:
                        self._active -= 1
                        self._session_condition.notify_all()
                    logger.debug(
                        "request end dataset=%s url=%s seconds=%.3f",
                        dataset_context.get(),
                        safe_url(url),
                        time.monotonic() - started,
                    )

    def retry(self, url: str = "", deadline: float | None = None) -> AsyncRetrying:
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
            shared = copy(state)
            shared.attempt_number = max(state.attempt_number, self.source_failures.get(failing_host(error, url), 0))
            pause = float(
                wait_exponential(multiplier=self.config.retry_initial_delay, max=self.config.retry_max_delay)(shared)
            )
            return max(pause, self.config.session_reset_delay if self._reset_pending else 0)

        async def before_sleep(state: Any) -> None:
            pause = float(state.next_action.sleep)
            await self._reset_session()
            origin = failing_host(state.outcome.exception(), url)
            self._resume_at[origin] = max(self._resume_at.get(origin, 0), time.monotonic() + pause)
            if deadline is not None and pause >= deadline - time.monotonic():
                logger.warning(
                    "source retry deferred dataset=%s host=%s wait_seconds=%.1f exceeds remaining deadline",
                    dataset_context.get(),
                    origin,
                    pause,
                )
                raise RecordError("source_backoff_exceeds_operation_deadline", url=url, retry=True)
            error = state.outcome.exception()
            logger.warning(
                "retry dataset=%s url=%s attempt=%s/%s exception=%s wait_seconds=%.1f",
                dataset_context.get(),
                safe_url(url),
                state.attempt_number,
                self.config.attempts,
                type(error).__name__,
                pause,
            )

        return AsyncRetrying(
            stop=stop_after_attempt(self.config.attempts),
            wait=delay,
            retry=retry_if_exception(
                lambda error: (
                    transient(error)
                    and self.source_failures.get(failing_host(error, url), 0) < self.config.source_failure_limit
                )
            ),
            before_sleep=before_sleep,
            reraise=True,
        )

    async def request(self, method: str, url: httpx.URL | str, **kwargs: Any) -> httpx.Response:
        deadline = time.monotonic() + self.config.operation_deadline
        try:
            async with asyncio.timeout(self.config.operation_deadline):
                async for attempt in self.retry(str(url), deadline):
                    with attempt:
                        async with self.attempt(str(url), attempt.retry_state.attempt_number, deadline):
                            source = self.source
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
                                        raise RecordError(
                                            "Metadata response exceeds configured byte limit", url=str(url)
                                        )
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
                                "request url=%s attempt=%s status=%s requests=%s",
                                safe_url(str(url)),
                                attempt.retry_state.attempt_number,
                                response.status_code,
                                self.requests,
                            )
                            if response.status_code in {429, 500, 502, 503, 504}:
                                response.raise_for_status()
                            return response

        except TimeoutError as error:
            logger.warning(
                "operation deadline exhausted dataset=%s url=%s seconds=%s",
                dataset_context.get(),
                safe_url(str(url)),
                self.config.operation_deadline,
            )
            raise httpx.ReadTimeout("HTTP operation deadline exhausted", request=httpx.Request(method, url)) from error
        raise RuntimeError("Retry loop produced no response")

    async def sample(
        self, url: str, readable: Callable[[str | None, str, str], bool] | None = None
    ) -> tuple[bytes, str, str, int, str | None]:
        """Read bounded content, stopping text resources after enough complete lines.

        ``readable(filename, resolved_url, media_type)`` rejects a response from its headers,
        before any of the body is transferred.
        """
        deadline = time.monotonic() + self.config.operation_deadline
        try:
            async with asyncio.timeout(self.config.operation_deadline):
                async for attempt in self.retry(str(url), deadline):
                    with attempt:
                        async with self.attempt(url, attempt.retry_state.attempt_number, deadline):
                            source = self.source
                            # stream() uses send(), so it does not reenter request()'s semaphore.
                            async with source.stream(
                                "GET", url, follow_redirects=True, headers={"Accept-Encoding": "identity"}
                            ) as response:
                                response.raise_for_status()
                                content_type = response.headers.get("content-type", "").split(";")[0]
                                disposition = Message()
                                disposition["Content-Disposition"] = response.headers.get("content-disposition", "")
                                filename = disposition.get_filename()
                                if readable and not readable(filename, str(response.url), content_type):
                                    raise ValueError("Unsupported or inconclusive tabular format")
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
                                        chunk = decoder.decompress(
                                            chunk, self.config.verification_bytes - len(data) + 1
                                        )
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
                                return bytes(data), str(response.url), content_type, transferred, filename
        except zlib.error as error:
            raise ValueError("Malformed gzip resource") from error

        except TimeoutError as error:
            logger.warning(
                "operation deadline exhausted dataset=%s url=%s seconds=%s",
                dataset_context.get(),
                safe_url(str(url)),
                self.config.operation_deadline,
            )
            raise httpx.ReadTimeout("HTTP sample deadline exhausted", request=httpx.Request("GET", url)) from error
        raise RuntimeError("Retry loop produced no sample")
