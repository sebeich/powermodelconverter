from __future__ import annotations

from pathlib import Path
from typing import Any

from powermodelconverter.core.contracts import ExportAdapter
from powermodelconverter.core.model import CanonicalCase
from powermodelconverter.core.registry import register_target_routes
from powermodelconverter.validation.tolerances import BALANCED_TOLERANCES


class PowerSystemsExportAdapter(ExportAdapter):
    target_format = "powersystems"

    def export_case(self, case: CanonicalCase, destination: str | Path, **kwargs: Any) -> Path:
        return export_powersystems(case, destination, **kwargs)


def export_powersystems(case: CanonicalCase, output_path: str | Path, **kwargs: Any) -> Path:
    from powermodelconverter.exporters.matpower import export_matpower

    return export_matpower(case, output_path, **kwargs)


register_target_routes(target_tool="powersystems", exporter=export_powersystems, tolerances=BALANCED_TOLERANCES)


__all__ = ["PowerSystemsExportAdapter", "export_powersystems"]
