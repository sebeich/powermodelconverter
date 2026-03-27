from __future__ import annotations

from pathlib import Path
from typing import Any

from powermodelconverter.adapters.base import ImportAdapter
from powermodelconverter.adapters.opendss_adapter import OpenDSSImportAdapter, OpenDSSResultSnapshot
from powermodelconverter.core.model import CanonicalCase


class PowerModelsDistributionImportAdapter(ImportAdapter):
    source_format = "powermodelsdistribution"

    def __init__(self) -> None:
        self._opendss = OpenDSSImportAdapter()

    def import_case(self, source: str | Path, **kwargs: Any) -> CanonicalCase:
        case = self._opendss.import_case(source, **kwargs)
        metadata = dict(case.metadata)
        metadata["import_backend"] = "powermodelsdistribution:dss_subset_via_opendssdirect"
        return CanonicalCase(
            case_id=case.case_id,
            source_format=self.source_format,
            base_mva=case.base_mva,
            frequency_hz=case.frequency_hz,
            tables=case.tables,
            metadata=metadata,
            source_path=Path(source),
        )

    def solve_source_case(self, source: str | Path) -> OpenDSSResultSnapshot:
        return self._opendss.solve_source_case(source)
