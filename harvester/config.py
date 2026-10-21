# SPDX-License-Identifier: NOASSERTION
# Copyright (c) 2026 NordicIntel
"""Load the small set of runtime paths and source connection settings."""

import tomllib
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field


class Config(BaseModel):
    model_config = ConfigDict(extra="forbid")

    current_directory: Path = Path("data/current")
    oai_url: str = "https://api.researchdata.se/oai-pmh"
    sitemap_url: str = "https://researchdata.se/catalogue/sitemap.xml"
    sparql_url: str = "https://admin.dataportal.se/sparql"
    catalog_metadata_url: str = "https://admin.dataportal.se/store/818/metadata/9198"
    catalog_uri: str = "https://researchdata.se/catalogue/"
    timeout: float = Field(default=90, gt=0)
    concurrency: int = Field(default=4, ge=1, le=32)
    retries: int = Field(default=5, ge=0, le=12)
    identity_batch_size: int = Field(default=100, ge=1, le=500)

    @classmethod
    def load(cls, path: Path) -> "Config":
        config = cls.model_validate(tomllib.loads(path.read_text(encoding="utf-8")))
        if not config.current_directory.is_absolute():
            config.current_directory = (
                path.resolve().parent / config.current_directory
            ).resolve()
        return config
