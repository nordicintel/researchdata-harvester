# SPDX-License-Identifier: NOASSERTION
# Copyright (c) 2026 NordicIntel
"""Build one complete current catalog and replace it only after validation."""

import tempfile
import time
from collections import Counter
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx

from harvester.config import Config
from harvester.dataportalen import harvest_dataportalen
from harvester.external import enrich_records
from harvester.matching import match_records
from harvester.researchdata import harvest_researchdata
from harvester.storage import (
    CatalogLock,
    activate,
    iter_records,
    load_records,
    read_json,
    recover_current,
    validate_catalog,
    write_catalog,
)
from harvester.transport import Transport


async def sync(
    config: Config,
    *,
    full: bool = False,
    client: httpx.AsyncClient | None = None,
    progress: Callable[[str], None] = print,
) -> dict[str, Any]:
    started = time.monotonic()
    boundary = datetime.now(UTC).date().isoformat()
    current = config.current_directory
    with CatalogLock(current.parent):
        recover_current(current)
        if current.exists():
            validate_catalog(current)
        state = read_json(current / "state.json") if current.exists() else {}
        matches = read_json(current / "matches.json") if current.exists() else {}
        research = (
            {
                record["identifier"]: {
                    "identifier": record["identifier"],
                    "datestamp": record["datestamp"],
                }
                for record in iter_records(current, "researchdata")
            }
            if full
            else load_records(current, "researchdata")
        )
        dataportalen = load_records(current, "dataportalen")
        previous_links = list(load_records(current, "links").values())
        previous_external = list(load_records(current, "external").values())
        previous_files = list(load_records(current, "files").values())
        transport = Transport(config, client)
        try:
            progress(
                f"Researchdata: {'full' if full or not research else 'incremental'} retrieval"
            )
            research, research_report = await harvest_researchdata(
                transport, config, research, state.get("boundary"), full=full
            )
            progress(
                f"Researchdata: {len(research)} current records; retrieving scoped Dataportalen metadata"
            )
            dataportalen, catalog = await harvest_dataportalen(
                transport, config, dataportalen
            )
            progress(f"Dataportalen: {len(dataportalen)} current records; matching")
            links, matches, fingerprints = match_records(
                research,
                dataportalen,
                previous_links,
                matches,
                state.get("fingerprints", {}),
                catalog,
            )
            progress("Resolving and revalidating dataset-owned external metadata")
            external, files = await enrich_records(
                transport,
                research,
                dataportalen,
                links,
                previous_external,
                previous_files,
            )
            state = {"boundary": boundary, "fingerprints": fingerprints}
            collections = {
                "researchdata": research.values(),
                "dataportalen": dataportalen.values(),
                "external": external,
                "links": links,
                "files": files,
            }
            progress("Validating and publishing current JSON catalog")
            with tempfile.TemporaryDirectory(
                prefix=".sync-", dir=current.parent
            ) as temporary:
                candidate = Path(temporary) / "current"
                manifest = write_catalog(candidate, collections, matches, state)
                activate(candidate, current)
            linked_research = {link["identifier"] for link in links}
            linked_dataportalen = {link["uri"] for link in links}
            return {
                "status": "complete",
                "boundary": boundary,
                "counts": {
                    name: entry["count"]
                    for name, entry in manifest["collections"].items()
                },
                "researchdata": research_report,
                "matching": {
                    "researchdata_with_links": len(linked_research),
                    "researchdata_without_links": len(
                        research.keys() - linked_research
                    ),
                    "dataportalen_with_links": len(linked_dataportalen),
                    "dataportalen_without_links": len(
                        dataportalen.keys() - linked_dataportalen
                    ),
                    "link_statuses": dict(Counter(link["status"] for link in links)),
                },
                "external_outcomes": dict(
                    Counter(record["status"] for record in external)
                ),
                "requests": transport.requests,
                "request_attempts": transport.attempts,
                "not_modified": transport.not_modified,
                "bytes_received": transport.bytes_received,
                "elapsed_seconds": round(time.monotonic() - started, 2),
            }
        finally:
            await transport.close()
