# SPDX-License-Identifier: NOASSERTION
# Copyright (c) 2026 NordicIntel
"""Validated standalone settings loaded from TOML, without environment secrets."""

import tomllib
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, HttpUrl, field_validator


class Config(BaseModel):
    """Paths and limits for a harvester; construction never opens the database."""

    model_config = ConfigDict(extra="forbid")
    database: Path = Path("data/catalog.sqlite")
    export_directory: Path = Path("data/exports")
    download_directory: Path = Path("data/downloads")
    formats: list[str] = Field(default_factory=lambda: ["datacite", "dcat", "json_ld"])
    oai_url: HttpUrl = "https://api.researchdata.se/oai-pmh"
    sitemap_url: HttpUrl = "https://researchdata.se/catalogue/sitemap.xml"
    sparql_url: HttpUrl = "https://admin.dataportal.se/sparql"
    catalog_metadata_url: HttpUrl = (
        "https://admin.dataportal.se/store/818/metadata/9198"
    )
    catalog_uri: HttpUrl = "https://researchdata.se/catalogue/"
    dataportalen_scope: Literal["researchdata", "all"] = "researchdata"
    timeout: float = Field(default=90, gt=0)
    concurrency: int = Field(default=4, ge=1, le=32)
    retries: int = Field(default=5, ge=0, le=12)
    sparql_page_size: int = Field(default=500, ge=1, le=10000)
    identity_batch_size: int = Field(default=100, ge=1, le=500)
    landing_fallback: bool = True

    @field_validator("formats")
    @classmethod
    def validate_formats(cls, formats: list[str]) -> list[str]:
        if not formats or any(
            not item or not item.replace("_", "").isalnum() for item in formats
        ):
            raise ValueError("Specify at least one OAI metadata prefix.")
        return list(dict.fromkeys(formats))

    @classmethod
    def load(cls, path: Path) -> "Config":
        """Resolve stored paths against the configuration directory."""
        config = cls.model_validate(tomllib.loads(path.read_text(encoding="utf-8")))
        for name in ("database", "export_directory", "download_directory"):
            value = getattr(config, name)
            if not value.is_absolute():
                setattr(config, name, (path.resolve().parent / value).resolve())
        return config
