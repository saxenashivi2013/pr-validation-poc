"""Runtime settings shared by every pipeline stage."""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional

_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


@dataclass(frozen=True)
class Settings:
    catalog: str
    schema: str
    volume: str = "raw"

    # Synthetic data volume / reproducibility
    seed: int = 42
    n_customers: int = 500
    n_products: int = 60
    n_orders: int = 5000
    days: int = 90
    dirty_ratio: float = 0.06

    # Quality thresholds enforced by the dq stage
    max_quarantine_ratio: float = 0.15
    max_orphan_ratio: float = 0.05

    # Overrides used by local / CI runs (no Unity Catalog volume, no Delta)
    raw_root_override: Optional[str] = None
    table_format: str = "delta"

    def __post_init__(self) -> None:
        for name in ("catalog", "schema", "volume"):
            value = getattr(self, name)
            if not _IDENT_RE.match(value):
                raise ValueError(f"Invalid {name} identifier: {value!r}")

    @property
    def uses_volume(self) -> bool:
        return self.raw_root_override is None

    @property
    def raw_root(self) -> str:
        return self.raw_root_override or f"/Volumes/{self.catalog}/{self.schema}/{self.volume}"

    @property
    def schema_fqn(self) -> str:
        return f"{self.catalog}.{self.schema}"

    def table(self, name: str) -> str:
        if not _IDENT_RE.match(name):
            raise ValueError(f"Invalid table name: {name!r}")
        return f"{self.catalog}.{self.schema}.{name}"
