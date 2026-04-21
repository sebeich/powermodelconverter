from __future__ import annotations

from pathlib import Path

from pandapower.converter.pandamodels.to_pm import convert_pp_to_pm

from powermodelconverter.core.pandapower_backend import PandapowerAdapter
from powermodelconverter.core.model import CanonicalCase
from powermodelconverter.core.registry import register_target_routes
from powermodelconverter.validation.tolerances import BALANCED_TOLERANCES


def export_powermodels(case: CanonicalCase, output_path: str | Path, **_: object) -> Path:
    if case.is_unbalanced:
        raise ValueError(
            "Balanced PowerModels export is only supported for balanced cases. "
            "Use a dedicated PowerModelsDistribution route for unbalanced models."
        )
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    net = PandapowerAdapter().to_net(case)
    convert_pp_to_pm(net, pm_file_path=str(path))
    return path


register_target_routes(target_tool="powermodels", exporter=export_powermodels, tolerances=BALANCED_TOLERANCES)


__all__ = ["export_powermodels"]
