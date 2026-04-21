from __future__ import annotations

from pathlib import Path

from powermodelconverter.core.model import CanonicalCase
from powermodelconverter.core.registry import register_target_routes
from powermodelconverter.importers.pypsa import PypsaAdapter
from powermodelconverter.validation.tolerances import BALANCED_TOLERANCES


def export_pypsa(case: CanonicalCase, output_path: str | Path, **_: object) -> Path:
    return PypsaAdapter().export_netcdf(case, output_path)


register_target_routes(target_tool="pypsa", exporter=export_pypsa, tolerances=BALANCED_TOLERANCES)


__all__ = ["export_pypsa"]
