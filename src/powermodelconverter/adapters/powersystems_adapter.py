from __future__ import annotations

from pathlib import Path
from typing import Any

from powermodelconverter.adapters.base import ExportAdapter, ImportAdapter
from powermodelconverter.adapters.matpower_adapter import MatpowerExportAdapter, MatpowerImportAdapter
from powermodelconverter.core.model import CanonicalCase


class PowerSystemsImportAdapter(ImportAdapter):
    source_format = "powersystems"

    def __init__(self) -> None:
        self._matpower = MatpowerImportAdapter()

    def import_case(self, source: str | Path, **kwargs: Any) -> CanonicalCase:
        imported = self._matpower.import_case(source, **kwargs)
        metadata = dict(imported.metadata)
        metadata["import_backend"] = "powersystems:matpower_parser_compat"
        return CanonicalCase(
            case_id=imported.case_id,
            source_format=self.source_format,
            base_mva=imported.base_mva,
            frequency_hz=imported.frequency_hz,
            tables=imported.tables,
            metadata=metadata,
            source_path=imported.source_path,
        )


class PowerSystemsExportAdapter(ExportAdapter):
    target_format = "powersystems"

    def __init__(self) -> None:
        self._matpower = MatpowerExportAdapter()

    def export_case(self, case: CanonicalCase, destination: str | Path, **kwargs: Any) -> Path:
        return self._matpower.export_case(case, destination, **kwargs)
