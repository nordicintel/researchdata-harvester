# SPDX-License-Identifier: NOASSERTION
# Copyright (c) 2026 NordicIntel
"""Explicit file downloads with verified reuse and atomic integrity-checked publication."""

import asyncio
import hashlib
import os
import re
import tempfile
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlsplit

import httpx

from researchdata_harvester.transport import Transport


def safe_relative_path(url: str, name: str | None = None) -> Path:
    """Preserve metadata-provided subpaths while rejecting filesystem escapes."""
    path = name if name else unquote(urlsplit(url).path)
    if not name:
        path = (
            path.split("/file/", 1)[-1] if "/file/" in path else path.rsplit("/", 1)[-1]
        )
    parts = path.replace("\\", "/").split("/")
    if not parts or any(
        part in {"", ".", ".."}
        or re.search(r'[<>:"|?*\x00-\x1f]', part)
        or part.endswith((".", " "))
        for part in parts
    ):
        raise ValueError("Download URL contains an unsafe filename or subpath.")
    if any(
        part.split(".")[0].upper()
        in {
            "CON",
            "PRN",
            "AUX",
            "NUL",
            *{f"COM{i}" for i in range(1, 10)},
            *{f"LPT{i}" for i in range(1, 10)},
        }
        for part in parts
    ):
        raise ValueError("Download filename is reserved by the operating system.")
    return Path(*parts)


def verify(path: Path, metadata: dict[str, Any]) -> bool:
    if metadata.get("size") is not None and path.stat().st_size != metadata["size"]:
        return False
    if metadata.get("sha256"):
        with path.open("rb") as source:
            return (
                hashlib.file_digest(source, "sha256").hexdigest().lower()
                == str(metadata["sha256"]).lower()
            )
    return metadata.get("size") is not None


async def download_file(
    transport: Transport, metadata: dict[str, Any], destination: Path
) -> dict[str, Any]:
    """Return an independent file outcome; metadata harvesting is unaffected."""
    temporary: Path | None = None
    try:
        destination = destination.resolve()
        target = destination / safe_relative_path(metadata["url"], metadata.get("name"))
        if not target.resolve().is_relative_to(destination):
            raise ValueError("Download path leaves the requested destination.")
        if target.exists():
            return {
                "status": "verified" if verify(target, metadata) else "conflict",
                "path": str(target),
            }
        target.parent.mkdir(parents=True, exist_ok=True)
        for attempt in range(transport.config.retries + 1):
            descriptor, name = tempfile.mkstemp(prefix=".download-", dir=target.parent)
            temporary = Path(name)
            digest, length = hashlib.sha256(), 0
            try:
                async with transport.stream(metadata["url"]) as response:
                    expected_type = metadata.get("encoding_format") or metadata.get(
                        "raw", {}
                    ).get("encodingFormat")
                    if (
                        expected_type
                        and "html" not in str(expected_type)
                        and "text/html" in response.headers.get("content-type", "")
                    ):
                        raise ValueError(
                            "Received an HTML page instead of the advertised data file."
                        )
                    with os.fdopen(descriptor, "wb") as output:
                        descriptor = -1
                        async for chunk in response.aiter_bytes():
                            output.write(chunk)
                            digest.update(chunk)
                            length += len(chunk)
                        output.flush()
                        os.fsync(output.fileno())
                if metadata.get("size") is not None and length != metadata["size"]:
                    raise ValueError(
                        f"Size mismatch: expected {metadata['size']}, received {length}."
                    )
                if (
                    metadata.get("sha256")
                    and digest.hexdigest().lower() != str(metadata["sha256"]).lower()
                ):
                    raise ValueError(
                        "SHA-256 mismatch; the temporary download was discarded."
                    )
                try:
                    # A hard link gives atomic publication without replacing an existing file.
                    os.link(temporary, target)
                except FileExistsError:
                    return {
                        "status": "verified"
                        if verify(target, metadata)
                        else "conflict",
                        "path": str(target),
                    }
                temporary.unlink()
                temporary = None
                return {
                    "status": "verified"
                    if metadata.get("sha256") or metadata.get("size") is not None
                    else "downloaded_unverified",
                    "path": str(target),
                    "size": length,
                    "sha256": digest.hexdigest(),
                }
            except httpx.TransportError:
                if attempt == transport.config.retries:
                    raise
                await asyncio.sleep(min(30, 2**attempt))
            finally:
                if descriptor >= 0:
                    os.close(descriptor)
                if temporary is not None:
                    temporary.unlink(missing_ok=True)
                    temporary = None
    except httpx.HTTPStatusError as error:
        status = (
            "restricted"
            if error.response.status_code in {401, 403}
            else "unavailable"
            if error.response.status_code in {404, 410}
            else "failed"
        )
        return {"status": status, "error": str(error)}
    except (OSError, ValueError, httpx.HTTPError) as error:
        return {"status": "failed", "error": str(error)}
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
    raise RuntimeError("Download retry loop did not produce an outcome.")
