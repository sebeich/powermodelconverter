from __future__ import annotations

from pathlib import Path
from typing import Any

from powermodelconverter.core.contracts import ImportAdapter
from powermodelconverter.core.model import CanonicalCase
from powermodelconverter.importers.opendss import OpenDSSImportAdapter, OpenDSSResultSnapshot


class PowerModelsDistributionImportAdapter(ImportAdapter):
    source_format = "powermodelsdistribution"

    def __init__(self) -> None:
        self._opendss = OpenDSSImportAdapter()

    def import_case(self, source: str | Path, **kwargs: Any) -> CanonicalCase:
        case = self._opendss.import_case(source, **kwargs)
        metadata = dict(case.metadata)
        metadata["import_backend"] = "powermodelsdistribution:dss_subset_via_opendssdirect"
        return case.copy_with(
            source_tool=self.source_format,
            metadata=metadata,
            source_path=Path(source),
        )

    def solve_source_case(self, source: str | Path) -> OpenDSSResultSnapshot:
        return self._opendss.solve_source_case(source)


def import_powermodels_distribution(path: str | Path, **kwargs: Any) -> CanonicalCase:
    return PowerModelsDistributionImportAdapter().import_case(path, **kwargs)


def solve_powermodels_distribution_reference(path: str | Path) -> OpenDSSResultSnapshot:
    return PowerModelsDistributionImportAdapter().solve_source_case(path)


__all__ = [
    "OpenDSSResultSnapshot",
    "PowerModelsDistributionImportAdapter",
    "import_powermodels_distribution",
    "solve_powermodels_distribution_reference",
]
