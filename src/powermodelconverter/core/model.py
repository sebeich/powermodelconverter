from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pandas as pd
import pandapower as pp
from pandapower.file_io import to_json
import numpy as np


@dataclass(slots=True)
class CanonicalCase:
    """Neutral in-memory representation built from normalized element tables."""

    case_id: str
    source_format: str
    base_mva: float
    frequency_hz: float
    tables: dict[str, pd.DataFrame]
    metadata: dict[str, Any] = field(default_factory=dict)
    source_path: Path | None = None

    def table(self, name: str) -> pd.DataFrame:
        return self.tables.get(name, pd.DataFrame()).copy()

    @property
    def is_unbalanced(self) -> bool:
        return bool(self.metadata.get("is_unbalanced", False))

    @property
    def phase_count(self) -> int:
        return int(self.metadata.get("phase_count", 3 if self.is_unbalanced else 1))

    @classmethod
    def from_pandapower(
        cls,
        *,
        case_id: str,
        source_format: str,
        net: Any,
        metadata: dict[str, Any] | None = None,
        source_path: Path | None = None,
    ) -> "CanonicalCase":
        metadata = dict(metadata or {})
        metadata.setdefault("pandapower_json", to_json(net, filename=None))
        is_unbalanced = any(
            hasattr(net, table_name) and len(getattr(net, table_name))
            for table_name in ("asymmetric_load", "asymmetric_sgen")
        )
        metadata.setdefault("is_unbalanced", is_unbalanced)
        metadata.setdefault("phase_count", 3 if is_unbalanced else 1)
        tables = {}
        for element in (
            "bus",
            "line",
            "trafo",
            "load",
            "sgen",
            "gen",
            "ext_grid",
            "asymmetric_load",
            "asymmetric_sgen",
            "shunt",
            "switch",
            "poly_cost",
            "res_bus",
            "res_bus_3ph",
            "res_line",
            "res_trafo",
            "res_load",
            "res_sgen",
            "res_asymmetric_load_3ph",
            "res_asymmetric_sgen_3ph",
            "res_gen",
            "res_ext_grid",
            "res_ext_grid_3ph",
            "res_shunt",
        ):
            if hasattr(net, element):
                tables[element] = getattr(net, element).copy()

        return cls(
            case_id=case_id,
            source_format=source_format,
            base_mva=float(getattr(net, "sn_mva", 1.0)),
            frequency_hz=float(getattr(net, "f_hz", 50.0)),
            tables=tables,
            metadata=metadata,
            source_path=source_path,
        )


@dataclass(slots=True)
class ComplexVoltageProfile:
    bus_name: str
    vm_pu: float
    va_degree: float

    @property
    def complex_pu(self) -> complex:
        return self.vm_pu * complex(
            np.cos(np.deg2rad(self.va_degree)),
            np.sin(np.deg2rad(self.va_degree)),
        )
