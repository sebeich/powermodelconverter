from __future__ import annotations

"""
The canonical representation is a pandapower network object.
This is not an accident — it is the core architectural decision.
pandapower's element tables serve as the neutral exchange schema.
Every importer MUST produce a valid pandapower net.
Every exporter MUST read only from the pandapower net.
No adapter may store tool-specific data that bypasses the net.
"""

from copy import deepcopy
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pandapower as pp
from pandapower.auxiliary import pandapowerNet
from pandapower.file_io import to_json


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


@dataclass(slots=True)
class CanonicalCase:
    case_id: str
    source_tool: str
    net: pandapowerNet
    is_balanced: bool
    phase_count: int
    base_mva: float
    frequency_hz: float
    source_path: Path | None = None
    source_format_version: str | None = None
    import_timestamp: str = field(default_factory=_utc_now_iso)
    import_warnings: list[str] = field(default_factory=list)
    reference_powerflow: dict[str, Any] | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    _element_counts: dict[str, int] = field(init=False, repr=False, default_factory=dict)

    def __post_init__(self) -> None:
        self.net = deepcopy(self.net)
        self.base_mva = float(self.base_mva)
        self.frequency_hz = float(self.frequency_hz)
        self.import_warnings = list(self.import_warnings)
        self.metadata = dict(self.metadata)
        if self.source_path is not None and not isinstance(self.source_path, Path):
            self.source_path = Path(self.source_path)
        self.validate()

    @property
    def source_format(self) -> str:
        return self.source_tool

    @property
    def is_unbalanced(self) -> bool:
        return not self.is_balanced

    @property
    def element_counts(self) -> dict[str, int]:
        return dict(self._element_counts)

    @property
    def tables(self) -> dict[str, pd.DataFrame]:
        tables: dict[str, pd.DataFrame] = {}
        for name, value in self.net.items():
            if isinstance(value, pd.DataFrame):
                tables[name] = value.copy()
        return tables

    def table(self, name: str) -> pd.DataFrame:
        value = getattr(self.net, name, None)
        if isinstance(value, pd.DataFrame):
            return value.copy()
        return pd.DataFrame()

    def validate(self) -> None:
        if not isinstance(self.net, pandapowerNet):
            raise TypeError("CanonicalCase.net must be a pandapower network.")
        for table_name in ("bus", "ext_grid"):
            if not hasattr(self.net, table_name):
                raise ValueError(f"CanonicalCase.net is missing required pandapower table '{table_name}'.")
        if self.base_mva <= 0:
            raise ValueError("CanonicalCase.base_mva must be positive.")
        if self.frequency_hz <= 0:
            raise ValueError("CanonicalCase.frequency_hz must be positive.")
        if self.is_balanced and (
            len(getattr(self.net, "asymmetric_load", pd.DataFrame()))
            or len(getattr(self.net, "asymmetric_sgen", pd.DataFrame()))
        ):
            raise ValueError("Balanced CanonicalCase instances cannot contain asymmetric pandapower elements.")
        if self.is_balanced and self.phase_count != 1:
            raise ValueError("Balanced CanonicalCase instances must have phase_count == 1.")
        if not self.is_balanced and self.phase_count != 3:
            raise ValueError("Unbalanced CanonicalCase instances must have phase_count == 3.")
        self._element_counts = self._compute_element_counts()

    def copy_with(self, **changes: Any) -> "CanonicalCase":
        payload = {
            "case_id": self.case_id,
            "source_tool": self.source_tool,
            "net": deepcopy(self.net),
            "is_balanced": self.is_balanced,
            "phase_count": self.phase_count,
            "base_mva": self.base_mva,
            "frequency_hz": self.frequency_hz,
            "source_path": self.source_path,
            "source_format_version": self.source_format_version,
            "import_timestamp": self.import_timestamp,
            "import_warnings": list(self.import_warnings),
            "reference_powerflow": deepcopy(self.reference_powerflow),
            "metadata": deepcopy(self.metadata),
        }
        payload.update(changes)
        return CanonicalCase(**payload)

    def _compute_element_counts(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for name, value in self.net.items():
            if isinstance(value, pd.DataFrame):
                counts[name] = len(value)
        return counts

    @classmethod
    def from_pandapower(
        cls,
        *,
        case_id: str,
        source_format: str,
        net: Any,
        metadata: dict[str, Any] | None = None,
        source_path: Path | None = None,
        source_format_version: str | None = None,
        import_warnings: list[str] | None = None,
        reference_powerflow: dict[str, Any] | None = None,
    ) -> "CanonicalCase":
        net_copy = deepcopy(net)
        metadata = dict(metadata or {})
        metadata.setdefault("pandapower_json", to_json(net_copy, filename=None))
        inferred_unbalanced = any(
            hasattr(net_copy, table_name) and len(getattr(net_copy, table_name))
            for table_name in ("asymmetric_load", "asymmetric_sgen")
        )
        is_unbalanced = bool(metadata.get("is_unbalanced", inferred_unbalanced))
        phase_count = int(metadata.get("phase_count", 3 if is_unbalanced else 1))
        if is_unbalanced and phase_count != 3:
            phase_count = 3
        if not is_unbalanced and phase_count != 1:
            phase_count = 1
        metadata["is_unbalanced"] = is_unbalanced
        metadata["phase_count"] = phase_count
        return cls(
            case_id=case_id,
            source_tool=source_format,
            net=net_copy,
            is_balanced=not is_unbalanced,
            phase_count=phase_count,
            base_mva=float(getattr(net_copy, "sn_mva", 1.0)),
            frequency_hz=float(getattr(net_copy, "f_hz", 50.0)),
            source_path=source_path,
            source_format_version=source_format_version,
            import_warnings=list(import_warnings or []),
            reference_powerflow=reference_powerflow,
            metadata=metadata,
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
