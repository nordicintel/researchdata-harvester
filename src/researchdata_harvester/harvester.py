# SPDX-License-Identifier: NOASSERTION
# Copyright (c) 2026 NordicIntel
"""Coordinate resumable independent metadata streams and explicit file operations."""

import asyncio
import hashlib
import json
import re
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlsplit

import httpx
from defusedxml.ElementTree import fromstring

from researchdata_harvester.config import Config
from researchdata_harvester.dataportalen import (
    DataportalenClient,
    identity_index,
    match_identities,
)
from researchdata_harvester.downloads import download_file
from researchdata_harvester.matching import MATCHING_POLICY, MetadataIndex
from researchdata_harvester.metadata import JSONLDParser, extract_files, local_name
from researchdata_harvester.oai import OAIClient, OAIError
from researchdata_harvester.store import Store, encode, now
from researchdata_harvester.transport import Transport


class Harvester:
    """Async context manager exposing sync, source clients, search and download.

    SQLite is the intentionally supported storage backend for this standalone tool.
    A process owns one writer. Use external scheduling without overlapping writers.
    """

    def __init__(
        self,
        config: Config,
        *,
        client: httpx.AsyncClient | None = None,
        progress: Callable[[str], None] | None = None,
    ):
        self.config = config
        self.transport = Transport(config, client)
        self.oai = OAIClient(self.transport, str(config.oai_url))
        self.dataportalen = DataportalenClient(self.transport, str(config.sparql_url))
        self.store = Store(config.database)
        self.progress = progress or (lambda message: None)

    async def __aenter__(self) -> "Harvester":
        return self

    async def __aexit__(self, *args: Any) -> None:
        self.store.close()
        await self.transport.close()

    async def sync(self, *, full: bool = False) -> dict[str, Any]:
        mode = "full" if full else "incremental"
        connection = self.store.connection
        previous = connection.execute(
            "SELECT * FROM runs WHERE mode=? AND status IN ('running','incomplete') AND id > COALESCE((SELECT MAX(id) FROM runs WHERE status='complete' AND mode IN ('full','incremental')),0) ORDER BY id DESC LIMIT 1",
            (mode,),
        ).fetchone()
        if previous:
            run, boundary = previous["id"], previous["boundary"]
            stats = json.loads(previous["stats"])
            with connection:
                connection.execute(
                    "UPDATE runs SET status='running',error=NULL,finished=NULL WHERE id=?",
                    (run,),
                )
        else:
            boundary = datetime.now(UTC).date().isoformat()
            run = self.store.start_run(mode, boundary)
            stats = {}
        last_boundary = self.store.last_boundary()
        from_date = (
            (
                datetime.fromisoformat(last_boundary).date() - timedelta(days=1)
            ).isoformat()
            if not full and last_boundary
            else None
        )
        scope = {
            "formats": self.config.formats,
            "oai_url": str(self.config.oai_url),
            "sitemap_url": str(self.config.sitemap_url),
            "sparql_url": str(self.config.sparql_url),
            "dataportalen_scope": self.config.dataportalen_scope,
            "catalog_metadata_url": str(self.config.catalog_metadata_url),
            "catalog_uri": str(self.config.catalog_uri),
            "landing_fallback": self.config.landing_fallback,
            "matching_policy": MATCHING_POLICY,
        }
        saved_scope = connection.execute(
            "SELECT details FROM checkpoints WHERE source='scope' AND run_id=?", (run,)
        ).fetchone()
        if saved_scope and json.loads(saved_scope[0]) != scope:
            self.store.finish_run(
                run,
                "incomplete",
                stats,
                "Resume configuration differs from saved source scope.",
            )
            raise ValueError(
                "Resume configuration differs; use the original source/format configuration or a separate database."
            )
        with connection:
            self.store.checkpoint("scope", run, completed=True, details=scope)
        try:
            await self._discovery(run)
            for prefix in self.config.formats:
                await self._research_stream(prefix, run, from_date, boundary)
            await self._sitemap(run)
            await self._current_inventory(run)
            await self._reconcile(run)
            if self.config.landing_fallback:
                stats["file_discovery"] = await self._discover_files(run)
            stats["dataportalen"] = await self._dataportalen(run)
            stats["researchdata_matching"] = self.store.matching_summary()
            self.store.finish_run(run, "complete", stats)
        except BaseException as error:
            self.store.finish_run(
                run, "incomplete", stats, f"{type(error).__name__}: {error}"
            )
            raise
        return {"run_id": run, "status": "complete", "boundary": boundary, **stats}

    async def _research_stream(
        self, prefix: str, run: int, from_date: str | None, until: str
    ) -> None:
        source = "oai:" + prefix
        connection = self.store.connection
        checkpoint = connection.execute(
            "SELECT * FROM checkpoints WHERE source=? AND run_id=?", (source, run)
        ).fetchone()
        if checkpoint and checkpoint["completed"]:
            if checkpoint["from_date"] is None:
                with connection:
                    connection.execute(
                        "UPDATE representations SET present=0 WHERE format=? AND catalogue=1 AND seen_run != ?",
                        (prefix, run),
                    )
                    self.store.refresh_identities()
            return
        if checkpoint:
            from_date, until = checkpoint["from_date"], checkpoint["until_date"]
        token = checkpoint["token"] if checkpoint else None
        restarts = 0
        while True:
            try:
                page_number = 0
                async for page in self.oai.iter_pages(
                    "ListRecords",
                    metadata_prefix=prefix,
                    from_=from_date,
                    until=until,
                    resumption_token=token,
                ):
                    page_number += 1
                    with connection:
                        for record in page.items:
                            self.store.put_record(
                                record,
                                prefix,
                                run,
                                str(self.config.oai_url),
                                catalogue=True,
                            )
                        connection.execute(
                            "INSERT OR IGNORE INTO source_pages VALUES(?,?,?,?,?)",
                            (
                                run,
                                source,
                                hashlib.sha256(page.raw_xml.encode()).hexdigest(),
                                page.raw_xml,
                                now(),
                            ),
                        )
                        self.store.checkpoint(
                            source,
                            run,
                            from_date=from_date,
                            until_date=until,
                            token=page.token,
                            completed=page.token is None,
                            details={
                                "page_records": len(page.items),
                                "reported_total": page.total,
                            },
                        )
                    self.progress(
                        f"{source} page {page_number}: {len(page.items)} records; total {page.total}; continuation {page.token is not None}"
                    )
                if from_date is None:
                    with connection:
                        connection.execute(
                            "UPDATE representations SET present=0 WHERE format=? AND catalogue=1 AND seen_run != ?",
                            (prefix, run),
                        )
                return
            except OAIError as error:
                if error.code != "badResumptionToken" or restarts >= 2:
                    raise
                restarts += 1
                token = None
                self.progress(
                    f"{source}: expired token; restarting the same date interval safely"
                )

    async def _sitemap(self, run: int) -> None:
        connection = self.store.connection
        checkpoint = connection.execute(
            "SELECT completed FROM checkpoints WHERE source='sitemap' AND run_id=?",
            (run,),
        ).fetchone()
        if checkpoint and checkpoint[0]:
            return
        queue, seen, entries = [str(self.config.sitemap_url)], set(), {}
        while queue:
            url = queue.pop()
            if url in seen:
                continue
            seen.add(url)
            if len(seen) > 10000:
                raise ValueError(
                    "Sitemap index exceeds 10,000 documents; scan is incomplete."
                )
            response = await self.transport.request("GET", url)
            root = fromstring(response.content)
            kind = local_name(root.tag)
            if kind not in {"sitemapindex", "urlset"}:
                raise ValueError("Expected sitemapindex or urlset; scan is incomplete.")
            for item in root:
                location = next(
                    (child.text for child in item if local_name(child.tag) == "loc"),
                    None,
                )
                if not location:
                    raise ValueError("Sitemap entry is missing its location.")
                if urlsplit(location).scheme not in {"http", "https"}:
                    raise ValueError("Sitemap contains a non-HTTP location.")
                if kind == "sitemapindex":
                    if (
                        urlsplit(location).netloc
                        != urlsplit(str(self.config.sitemap_url)).netloc
                    ):
                        raise ValueError(
                            "Cross-host sitemap indexes require explicit configuration."
                        )
                    queue.append(location)
                else:
                    path = urlsplit(location).path
                    entries[location] = (
                        "dataset"
                        if "/dataset/" in path
                        else "collection"
                        if "/collection/" in path
                        else "other"
                    )
        with connection:
            for url, kind in entries.items():
                connection.execute(
                    "INSERT INTO sitemap_entries VALUES(?,?,?,1,?) ON CONFLICT(url) DO UPDATE SET kind=excluded.kind,seen_run=excluded.seen_run,present=1,provenance=excluded.provenance",
                    (url, kind, run, encode({"source": str(self.config.sitemap_url)})),
                )
            connection.execute(
                "UPDATE sitemap_entries SET present=0 WHERE seen_run != ?", (run,)
            )
            self.store.checkpoint(
                "sitemap",
                run,
                completed=True,
                details={
                    kind: sum(value == kind for value in entries.values())
                    for kind in {"dataset", "collection", "other"}
                },
            )
        self.progress(
            f"sitemap: {sum(value == 'dataset' for value in entries.values())} datasets; collections counted separately"
        )

    async def _discovery(self, run: int) -> None:
        checkpoint = self.store.connection.execute(
            "SELECT 1 FROM checkpoints WHERE source='discovery' AND run_id=? AND completed=1",
            (run,),
        ).fetchone()
        if checkpoint:
            return
        identify = await self.oai.identify()
        formats = await self.oai.list_metadata_formats()
        sets = [item async for item in self.oai.iter_sets()]
        available = {
            child["text"]
            for item in formats
            for child in item["children"]
            if local_name(child["tag"]) == "metadataPrefix"
        }
        missing = set(self.config.formats) - available
        if missing:
            raise ValueError(
                f"Repository does not advertise configured metadata formats: {sorted(missing)}."
            )
        with self.store.connection:
            self.store.checkpoint(
                "discovery",
                run,
                completed=True,
                details={"identify": identify, "formats": formats, "sets": sets},
            )
        self.progress(f"OAI discovery: {len(available)} formats; {len(sets)} sets")

    async def _current_inventory(self, run: int) -> None:
        """Scan an unfiltered independent identifier inventory on every sync.

        Incremental metadata feeds cannot identify versions that disappeared from
        the current catalog. Only this complete inventory controls membership.
        """
        connection = self.store.connection
        source = "oai:inventory"
        checkpoint = connection.execute(
            "SELECT * FROM checkpoints WHERE source=? AND run_id=?", (source, run)
        ).fetchone()
        if checkpoint and checkpoint["completed"]:
            return
        token = checkpoint["token"] if checkpoint else None
        restarts = 0
        while True:
            try:
                async for page in self.oai.iter_pages(
                    "ListIdentifiers", metadata_prefix="oai_dc", resumption_token=token
                ):
                    with connection:
                        for header in page.items:
                            identifier = next(
                                (
                                    child["text"]
                                    for child in header["children"]
                                    if local_name(child["tag"]) == "identifier"
                                ),
                                None,
                            )
                            if not identifier:
                                raise ValueError(
                                    "Current identifier inventory contains a malformed header."
                                )
                            if header["attributes"].get("status") != "deleted":
                                connection.execute(
                                    "INSERT INTO current_inventory VALUES(?,?,?) ON CONFLICT(identifier) DO UPDATE SET header=excluded.header,seen_run=excluded.seen_run",
                                    (identifier, encode(header), run),
                                )
                        connection.execute(
                            "INSERT OR IGNORE INTO source_pages VALUES(?,?,?,?,?)",
                            (
                                run,
                                source,
                                hashlib.sha256(page.raw_xml.encode()).hexdigest(),
                                page.raw_xml,
                                now(),
                            ),
                        )
                        self.store.checkpoint(
                            source,
                            run,
                            token=page.token,
                            completed=page.token is None,
                            details={"reported_total": page.total},
                        )
                    self.progress(
                        f"current identifier inventory: {len(page.items)} headers; reported total {page.total}; continuation {page.token is not None}"
                    )
                return
            except OAIError as error:
                if error.code != "badResumptionToken" or restarts >= 2:
                    raise
                restarts += 1
                with connection:
                    connection.execute(
                        "DELETE FROM current_inventory WHERE seen_run=?", (run,)
                    )
                    self.store.checkpoint(source, run)
                token = None
                self.progress(
                    "current identifier inventory: expired token; restarting full inventory"
                )

    async def _reconcile(self, run: int) -> None:
        connection = self.store.connection
        inventory_complete = connection.execute(
            "SELECT 1 FROM checkpoints WHERE source='oai:inventory' AND run_id=? AND completed=1",
            (run,),
        ).fetchone()
        if not inventory_complete:
            raise ValueError(
                "Current identifier inventory is incomplete; membership cannot be reconciled."
            )
        active = {
            row[0]
            for row in connection.execute(
                "SELECT identifier FROM current_inventory WHERE seen_run=?", (run,)
            )
        }
        sitemap = {
            urlsplit(row[0]).path.rstrip("/").rsplit("/", 1)[-1]: row[0]
            for row in connection.execute(
                "SELECT url FROM sitemap_entries WHERE present=1 AND kind='dataset'"
            )
        }
        by_base = {
            identifier.removeprefix("oai:researchdata.se:").rsplit("/", 1)[
                0
            ]: identifier
            for identifier in active
        }
        missing = sorted(set(sitemap) - set(by_base))
        extra = sorted(set(by_base) - set(sitemap))
        for dataset in missing:
            response = await self.transport.request(
                "GET", sitemap[dataset], timeout=min(25, self.config.timeout)
            )
            parser = JSONLDParser()
            parser.feed(response.text)
            text = encode(parser.documents)
            identifiers = set(
                re.findall(
                    r"oai:researchdata\.se:" + re.escape(dataset) + r"/[A-Za-z0-9._-]+",
                    text,
                )
            )
            for href in parser.links:
                target = urlsplit(urljoin(str(response.url), href))
                if target.hostname != urlsplit(str(self.config.sitemap_url)).hostname:
                    continue
                match = re.fullmatch(
                    r"/catalogue/dataset/"
                    + re.escape(dataset)
                    + r"/([A-Za-z0-9._-]+)/export/[A-Za-z0-9_]+/?",
                    target.path,
                )
                if match:
                    identifiers.add(f"oai:researchdata.se:{dataset}/{match[1]}")
            if len(identifiers) != 1:
                raise ValueError(
                    f"Sitemap dataset {dataset} is missing from the OAI inventory and its landing metadata or export links do not identify one current version; coverage is incomplete."
                )
            active.update(identifiers)
        for prefix in self.config.formats:
            present = {
                row[0]
                for row in connection.execute(
                    "SELECT identifier FROM representations WHERE format=? AND present=1 AND catalogue=1",
                    (prefix,),
                )
            }
            for identifier in sorted(active - present):
                self.progress(f"reconcile: GetRecord {prefix} {identifier}")
                record = await self.oai.get_record(identifier, prefix)
                with connection:
                    self.store.put_record(
                        record, prefix, run, str(self.config.oai_url), catalogue=True
                    )
        with connection:
            absent = {
                row[0]
                for row in connection.execute(
                    "SELECT DISTINCT identifier FROM representations WHERE catalogue=1"
                )
            } - active
            connection.executemany(
                "UPDATE representations SET present=0 WHERE catalogue=1 AND identifier=?",
                [(identifier,) for identifier in sorted(absent)],
            )
            self.store.refresh_identities()
            connection.execute(
                "UPDATE research_records SET current_catalog=EXISTS(SELECT 1 FROM representations p WHERE p.identifier=research_records.identifier AND p.present=1 AND p.catalogue=1)"
            )
            self.store.checkpoint(
                "reconciliation",
                run,
                completed=True,
                details={
                    "sitemap_datasets": len(sitemap),
                    "oai_datasets": len(by_base),
                    "oai_records": len(active),
                    "missing_from_oai": missing,
                    "missing_from_sitemap": extra,
                },
            )
        self.progress(
            f"reconciliation: {len(active)} OAI versions; {len(sitemap)} sitemap datasets; {len(missing)} recovered; {len(extra)} absent from sitemap"
        )

    async def _discover_files(self, run: int) -> dict[str, int]:
        connection = self.store.connection
        records = connection.execute(
            "SELECT identifier,identities FROM research_records WHERE deleted=0 AND current_catalog=1 ORDER BY identifier"
        ).fetchall()
        semaphore = asyncio.Semaphore(self.config.concurrency)
        counts: dict[str, int] = {}

        async def discover(row: Any) -> None:
            identifier = row["identifier"]
            source = "file-discovery:" + identifier
            previous = connection.execute(
                "SELECT details FROM checkpoints WHERE source=? AND completed=1 ORDER BY run_id DESC LIMIT 1",
                (source,),
            ).fetchone()
            # Revisit when the metadata was refreshed in this run, except on a resumed run.
            current = connection.execute(
                "SELECT details FROM checkpoints WHERE source=? AND run_id=? AND completed=1",
                (source, run),
            ).fetchone()
            refreshed = connection.execute(
                "SELECT 1 FROM representations WHERE identifier=? AND seen_run=?",
                (identifier, run),
            ).fetchone()
            if current or (previous and not refreshed):
                outcome = json.loads((current or previous)[0])["status"]
                counts[outcome] = counts.get(outcome, 0) + 1
                return
            current_files = self.store.file_entries(identifier)
            landing_refresh = bool(
                refreshed
                and any(entry["metadata"].get("landing_url") for entry in current_files)
            )
            if current_files and not landing_refresh:
                outcome, detail = "files_available", {}
            else:
                evidence = json.loads(row["identities"])
                urls = list(
                    dict.fromkeys(
                        item["value"]
                        for item in evidence
                        if item["value"].startswith(("http://", "https://"))
                        and (
                            "landingPage" in item["field"]
                            or "doi.org/" in item["value"]
                            or "researchdata.se/" in item["value"]
                        )
                    )
                )[:3]
                outcome, detail = "no_explicit_landing", {}
                async with semaphore:
                    for url in urls:
                        try:
                            response = await self.transport.request(
                                "GET", url, timeout=min(25, self.config.timeout)
                            )
                            parser = JSONLDParser()
                            parser.feed(response.text)
                            entries = extract_files(parser.documents)
                            with connection:
                                connection.execute(
                                    "UPDATE files SET present=0 WHERE identifier=? AND json_extract(metadata,'$.landing_url') IS NOT NULL",
                                    (identifier,),
                                )
                                for entry in entries:
                                    self.store.put_file(
                                        identifier,
                                        dict(entry, landing_url=str(response.url)),
                                    )
                            outcome, detail = (
                                "files_available"
                                if entries
                                else "no_files_in_landing_metadata",
                                {"url": str(response.url)},
                            )
                            if entries:
                                break
                        except (httpx.HTTPError, ValueError) as error:
                            outcome, detail = (
                                "landing_unavailable",
                                {"url": url, "error": str(error)},
                            )
            with connection:
                self.store.checkpoint(
                    source, run, completed=True, details={"status": outcome, **detail}
                )
            counts[outcome] = counts.get(outcome, 0) + 1
            if sum(counts.values()) % 100 == 0:
                self.progress(
                    f"file discovery: {sum(counts.values())}/{len(records)} attempts completed"
                )

        await asyncio.gather(*(discover(row) for row in records))
        return counts

    async def _dataportalen(self, run: int) -> dict[str, Any]:
        connection = self.store.connection
        checkpoint = connection.execute(
            "SELECT * FROM checkpoints WHERE source='dataportalen' AND run_id=?", (run,)
        ).fetchone()
        if checkpoint and checkpoint["completed"]:
            return json.loads(checkpoint["details"])
        stats = (
            json.loads(checkpoint["details"])
            if checkpoint
            else {
                "scanned": 0,
                "matched": 0,
                "unresolved": 0,
                "no_identifier": 0,
                "scope": self.config.dataportalen_scope,
            }
        )
        after = checkpoint["token"] if checkpoint else None
        inventory_checkpoint = connection.execute(
            "SELECT details FROM checkpoints WHERE source='dataportalen:inventory' AND run_id=?",
            (run,),
        ).fetchone()
        if inventory_checkpoint:
            inventory = json.loads(inventory_checkpoint[0])
        else:
            if checkpoint:
                raise ValueError(
                    "Dataportalen resume has no catalog inventory snapshot; use the original scope."
                )
            inventory = await self.dataportalen.catalog_inventory(
                str(self.config.catalog_metadata_url), str(self.config.catalog_uri)
            )
            inventory["fetched_at"] = now()
            with connection:
                self.store.checkpoint(
                    "dataportalen:inventory", run, completed=True, details=inventory
                )
        members = (
            inventory["members"]
            if self.config.dataportalen_scope == "researchdata"
            else None
        )
        trusted_members = set(inventory["members"])
        metadata_index = MetadataIndex(connection)
        research = {
            row["identifier"]: json.loads(row["identities"])
            for row in connection.execute(
                "SELECT identifier,identities FROM research_records WHERE deleted=0 AND current_catalog=1"
            )
        }
        index = identity_index(research)
        while True:
            if members is None:
                uris = await self.dataportalen.dataset_page(
                    after=after, limit=self.config.sparql_page_size
                )
            else:
                uris = [uri for uri in members if after is None or uri > after][
                    : self.config.sparql_page_size
                ]
            if not uris:
                with connection:
                    if members is None:
                        connection.execute(
                            "UPDATE dataportalen_records SET present=0 WHERE seen_run != ?",
                            (run,),
                        )
                    connection.execute(
                        "UPDATE dataportalen_records SET in_scope=CASE WHEN seen_run=? THEN 1 ELSE 0 END",
                        (run,),
                    )
                    self.store.checkpoint(
                        "dataportalen", run, token=after, completed=True, details=stats
                    )
                return stats
            evidence = {}
            for offset in range(0, len(uris), self.config.identity_batch_size):
                evidence.update(
                    await self.dataportalen.identities(
                        uris[offset : offset + self.config.identity_batch_size]
                    )
                )
            matches = {
                uri: match_identities(research, evidence[uri], index=index)
                for uri in uris
            }
            candidates = [
                uri
                for uri in uris
                if uri in trusted_members
                or matches[uri]
                or any(
                    urlsplit(item["value"]).hostname
                    in {"researchdata.se", "api.researchdata.se"}
                    for item in evidence[uri]
                )
            ]
            metadata = {}
            for offset in range(0, len(candidates), 50):
                metadata.update(
                    await self.dataportalen.metadata(candidates[offset : offset + 50])
                )
            for uri in uris:
                if not matches[uri] and uri in metadata:
                    matches[uri] = metadata_index.match(uri, metadata[uri], inventory)
            with connection:
                for uri in uris:
                    existing = connection.execute(
                        "SELECT triples,metadata_complete FROM dataportalen_records WHERE uri=?",
                        (uri,),
                    ).fetchone()
                    triples = metadata.get(
                        uri, json.loads(existing[0]) if existing else []
                    )
                    connection.execute(
                        "INSERT INTO dataportalen_records(uri,identities,triples,metadata_complete,present,seen_run,fetched_at,provenance,in_scope) VALUES(?,?,?,?,1,?,?,?,0) ON CONFLICT(uri) DO UPDATE SET identities=excluded.identities,triples=excluded.triples,metadata_complete=excluded.metadata_complete,present=1,seen_run=excluded.seen_run,fetched_at=excluded.fetched_at,provenance=excluded.provenance",
                        (
                            uri,
                            encode(evidence[uri]),
                            encode(triples),
                            uri in metadata or bool(existing and existing[1]),
                            run,
                            now(),
                            encode(
                                {
                                    "source": str(self.config.sparql_url),
                                    "closure": "endpoint concise bounded description; known named metadata relations and all supplied blank-node descendants",
                                    "candidate": uri in candidates,
                                }
                            ),
                        ),
                    )
                    connection.execute("DELETE FROM links WHERE uri=?", (uri,))
                    for match in matches[uri]:
                        connection.execute(
                            "INSERT INTO links VALUES(?,?,?,?,?,?,?)",
                            (
                                match["identifier"],
                                uri,
                                match["rule"],
                                match["relation"],
                                match["status"],
                                encode(match["evidence"]),
                                run,
                            ),
                        )
                    stats["scanned"] += 1
                    stats["matched"] += bool(matches[uri])
                    stats["unresolved"] += uri in candidates and not matches[uri]
                    stats["no_identifier"] += len(evidence[uri]) == 1
                after = uris[-1]
                self.store.checkpoint("dataportalen", run, token=after, details=stats)
            self.progress(
                f"Dataportalen: {stats['scanned']} identities scanned; {stats['matched']} matched; {stats['unresolved']} unresolved"
            )

    async def fetch_record(
        self, identifier: str, *, formats: list[str] | None = None
    ) -> dict[str, Any]:
        """Fetch only the requested version, without traversing version history."""
        run = self.store.start_run("record", datetime.now(UTC).date().isoformat())
        try:
            for prefix in formats or self.config.formats:
                record = await self.oai.get_record(identifier, prefix)
                with self.store.connection:
                    self.store.put_record(record, prefix, run, str(self.config.oai_url))
            self.store.finish_run(run, "complete", {})
        except BaseException as error:
            self.store.finish_run(run, "incomplete", {}, str(error))
            raise
        return self.store.record(identifier)

    async def download(
        self,
        identifier: str,
        destination: Path | None = None,
        *,
        urls: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        """Download current advertised files, optionally selecting exact source URLs."""
        entries = self.store.file_entries(identifier)
        if urls is not None:
            selected = set(urls)
            unknown = selected - {entry["url"] for entry in entries}
            if unknown:
                raise ValueError(
                    f"Selected URLs are not current advertised files for {identifier}: {sorted(unknown)}."
                )
            entries = [entry for entry in entries if entry["url"] in selected]
        destination = (
            destination
            or self.config.download_directory
            / identifier.removeprefix("oai:researchdata.se:").replace("/", "-")
        )
        result = []
        for entry in entries:
            outcome = await download_file(
                self.transport, entry["metadata"], destination
            )
            with self.store.connection:
                self.store.connection.execute(
                    "UPDATE files SET status=?,local_path=?,error=?,fetched_at=? WHERE identifier=? AND url=?",
                    (
                        outcome["status"],
                        outcome.get("path"),
                        outcome.get("error"),
                        now(),
                        identifier,
                        entry["url"],
                    ),
                )
            result.append({"url": entry["url"], **outcome})
        return result
