from __future__ import annotations

from pathlib import Path
from typing import Any

import numpy as np
import pandapower as pp
from pandapower.auxiliary import LoadflowNotConverged
from pandapower.converter.pandamodels.to_pm import convert_pp_to_pm

from powermodelconverter.core.model import CanonicalCase


class PandapowerAdapter:
    format_name = "pandapower"

    def to_canonical(
        self,
        net: Any,
        *,
        case_id: str,
        source_format: str,
        metadata: dict[str, Any] | None = None,
        source_path: Path | None = None,
    ) -> CanonicalCase:
        return CanonicalCase.from_pandapower(
            case_id=case_id,
            source_format=source_format,
            net=net,
            metadata=metadata,
            source_path=source_path,
        )

    def to_net(self, case: CanonicalCase) -> Any:
        if "pandapower_json" in case.metadata:
            return pp.from_json_string(case.metadata["pandapower_json"])
        net = pp.create_empty_network(sn_mva=case.base_mva, f_hz=case.frequency_hz)
        for name, table in case.tables.items():
            setattr(net, name, table.copy())
        return net

    def run_power_flow(self, case: CanonicalCase, **kwargs: Any) -> Any:
        net = self.to_net(case)
        if kwargs:
            pp.runpp(net, **kwargs)
            return net

        strategies = [
            {"algorithm": "nr", "init": "auto", "max_iteration": 20, "numba": False},
            {"algorithm": "bfsw", "init": "flat", "max_iteration": 100, "numba": False},
            {"algorithm": "gs", "init": "flat", "max_iteration": 200, "numba": False},
        ]
        last_error: Exception | None = None
        for strategy in strategies:
            try:
                pp.runpp(net, **strategy)
                return net
            except LoadflowNotConverged as exc:
                last_error = exc
        if last_error is not None:
            raise last_error
        return net

    def run_power_flow_3ph(self, case: CanonicalCase, **kwargs: Any) -> Any:
        net = self.to_net(case)
        self._prepare_3ph_net(net)
        pp.runpp_3ph(net, numba=False, **kwargs)
        return net

    def export_json(self, case: CanonicalCase, destination: str | Path) -> Path:
        path = Path(destination)
        path.parent.mkdir(parents=True, exist_ok=True)
        net = self.to_net(case)
        pp.to_json(net, str(path))
        return path

    def export_powermodels_json(self, case: CanonicalCase, destination: str | Path) -> Path:
        if case.is_unbalanced:
            raise ValueError(
                "Balanced PowerModels export is only supported for balanced cases. "
                "Use a dedicated PowerModelsDistribution route for unbalanced models."
            )
        path = Path(destination)
        path.parent.mkdir(parents=True, exist_ok=True)
        net = self.to_net(case)
        convert_pp_to_pm(net, pm_file_path=str(path))
        return path

    def get_pm_bus_lookup(self, case: CanonicalCase) -> dict[int, int]:
        net = self.to_net(case)
        convert_pp_to_pm(net)
        bus_lookup = net.get("_pd2pm_lookups", {}).get("bus", np.array([], dtype=int))
        return {int(bus_idx): int(pm_bus) for bus_idx, pm_bus in enumerate(bus_lookup)}

    def _prepare_3ph_net(self, net: Any) -> None:
        if hasattr(net, "ext_grid") and len(net.ext_grid):
            defaults = {
                "s_sc_max_mva": 1000.0,
                "rx_max": 0.1,
                "x0x_max": 1.0,
                "r0x0_max": 0.1,
            }
            for column, default in defaults.items():
                if column not in net.ext_grid.columns:
                    net.ext_grid[column] = default
                else:
                    net.ext_grid[column] = net.ext_grid[column].fillna(default)
