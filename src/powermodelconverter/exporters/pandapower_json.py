from __future__ import annotations

from pathlib import Path

import pandapower as pp

from powermodelconverter.core.pandapower_backend import PandapowerAdapter
from powermodelconverter.core.model import CanonicalCase
from powermodelconverter.core.registry import register_target_routes
from powermodelconverter.validation.tolerances import BALANCED_TOLERANCES


class PandapowerJsonExportAdapter:
    target_format = "pandapower"

    def __init__(self) -> None:
        self._pandapower = PandapowerAdapter()

    def export_case(self, case: CanonicalCase, output_path: str | Path) -> Path:
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        net = self._pandapower.to_net(case)
        pp.to_json(net, str(path))
        return path


def export_pandapower(case: CanonicalCase, output_path: str | Path, **_: object) -> Path:
    return PandapowerJsonExportAdapter().export_case(case, output_path)


register_target_routes(target_tool="pandapower", exporter=export_pandapower, tolerances=BALANCED_TOLERANCES)


__all__ = ["PandapowerJsonExportAdapter", "export_pandapower"]
