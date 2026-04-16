from __future__ import annotations

from pathlib import Path
import math
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

        strategies: list[dict[str, Any]] = []
        if "pmc_init_vm_pu" in net.bus.columns and "pmc_init_va_degree" in net.bus.columns:
            strategies.append(
                {
                    "algorithm": "nr",
                    "init": "auto",
                    "init_vm_pu": net.bus["pmc_init_vm_pu"].astype(float).tolist(),
                    "init_va_degree": net.bus["pmc_init_va_degree"].astype(float).tolist(),
                    "calculate_voltage_angles": True,
                    "max_iteration": 100,
                    "numba": False,
                }
            )

        strategies.extend([
            {"algorithm": "nr", "init": "auto", "max_iteration": 20, "numba": False},
            {"algorithm": "bfsw", "init": "flat", "max_iteration": 100, "numba": False},
            {"algorithm": "gs", "init": "flat", "max_iteration": 200, "numba": False},
        ])
        last_error: Exception | None = None
        for strategy in strategies:
            try:
                pp.runpp(net, **strategy)
                return net
            except (LoadflowNotConverged, Exception) as exc:
                last_error = exc
        if last_error is not None:
            raise last_error
        return net

    def run_power_flow_3ph(self, case: CanonicalCase, **kwargs: Any) -> Any:
        net = self.to_net(case)
        self._prepare_3ph_net(net)
        if self._has_voltage_dependent_asymmetric_loads(net):
            self._runpp_3ph_with_voltage_dependent_asymmetric_loads(net, **kwargs)
        else:
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

    def _has_voltage_dependent_asymmetric_loads(self, net: Any) -> bool:
        if not hasattr(net, "asymmetric_load") or len(net.asymmetric_load) == 0:
            return False
        if "pmc_model" not in net.asymmetric_load.columns:
            return False
        models = net.asymmetric_load["pmc_model"].fillna(1)
        return bool(models.isin([2, 5]).any())

    def _runpp_3ph_with_voltage_dependent_asymmetric_loads(self, net: Any, **kwargs: Any) -> None:
        max_outer_iterations = int(kwargs.pop("pmc_max_outer_iterations", 20))
        tolerance = float(kwargs.pop("pmc_outer_tolerance", 1e-6))
        for _ in range(max_outer_iterations):
            pp.runpp_3ph(net, numba=False, **kwargs)
            max_delta = self._update_voltage_dependent_asymmetric_loads(net)
            if max_delta <= tolerance:
                return
            kwargs = {**kwargs, "init": "results"}

    def _update_voltage_dependent_asymmetric_loads(self, net: Any) -> float:
        max_delta = 0.0
        for idx, row in net.asymmetric_load.iterrows():
            model = int(row.get("pmc_model", 1) or 1)
            if model not in (2, 5):
                continue
            voltage_factor = self._asymmetric_load_voltage_factor(net, row)
            exponent = 2.0 if model == 2 else 1.0
            if model == 5:
                vmin = float(row.get("pmc_vminpu", 0.95) or 0.95)
                vmax = float(row.get("pmc_vmaxpu", 1.05) or 1.05)
                if voltage_factor < vmin or voltage_factor > vmax:
                    exponent = 2.0
            scale = voltage_factor**exponent
            for phase in ("a", "b", "c"):
                for quantity in ("p", "q"):
                    column = f"{quantity}_{phase}_mw" if quantity == "p" else f"{quantity}_{phase}_mvar"
                    base_column = f"pmc_base_{quantity}_{phase}_mw" if quantity == "p" else f"pmc_base_{quantity}_{phase}_mvar"
                    if base_column not in net.asymmetric_load.columns:
                        continue
                    base_value = float(net.asymmetric_load.at[idx, base_column])
                    new_value = base_value * scale
                    max_delta = max(max_delta, abs(float(net.asymmetric_load.at[idx, column]) - new_value))
                    net.asymmetric_load.at[idx, column] = new_value
        return max_delta

    def _asymmetric_load_voltage_factor(self, net: Any, row: Any) -> float:
        bus_idx = int(row["bus"])
        phases = self._load_nodes(row)
        if row.get("type", "wye") == "delta":
            values = self._delta_voltage_factors(net, bus_idx, phases)
        else:
            values = self._wye_voltage_factors(net, bus_idx, phases)
        if not values:
            return 1.0
        return max(sum(values) / len(values), 1e-6)

    def _load_nodes(self, row: Any) -> list[int]:
        raw = str(row.get("pmc_nodes", "") or "")
        nodes = [int(part) for part in raw.split(",") if part.strip().isdigit()]
        if nodes:
            return nodes
        active: list[int] = []
        if abs(float(row.get("p_a_mw", 0.0))) > 0 or abs(float(row.get("q_a_mvar", 0.0))) > 0:
            active.append(1)
        if abs(float(row.get("p_b_mw", 0.0))) > 0 or abs(float(row.get("q_b_mvar", 0.0))) > 0:
            active.append(2)
        if abs(float(row.get("p_c_mw", 0.0))) > 0 or abs(float(row.get("q_c_mvar", 0.0))) > 0:
            active.append(3)
        return active or [1, 2, 3]

    def _wye_voltage_factors(self, net: Any, bus_idx: int, phases: list[int]) -> list[float]:
        phase_map = {1: "a", 2: "b", 3: "c"}
        factors: list[float] = []
        for node in phases:
            suffix = phase_map.get(node)
            if suffix is None:
                continue
            value = float(net.res_bus_3ph.at[bus_idx, f"vm_{suffix}_pu"])
            if math.isfinite(value):
                factors.append(value)
        return factors

    def _delta_voltage_factors(self, net: Any, bus_idx: int, phases: list[int]) -> list[float]:
        phase_pairs = {
            (1, 2): ("a", "b"),
            (2, 3): ("b", "c"),
            (1, 3): ("a", "c"),
        }
        if len(phases) >= 3:
            pairs = [(1, 2), (2, 3), (1, 3)]
        elif len(phases) == 2:
            pair = tuple(sorted(phases))
            pairs = [pair] if pair in phase_pairs else []
        else:
            pairs = []
        factors: list[float] = []
        for pair in pairs:
            left_suffix, right_suffix = phase_pairs[pair]
            left = self._phase_voltage_phasor(net, bus_idx, left_suffix)
            right = self._phase_voltage_phasor(net, bus_idx, right_suffix)
            factors.append(abs(left - right) / math.sqrt(3.0))
        return factors

    def _phase_voltage_phasor(self, net: Any, bus_idx: int, suffix: str) -> complex:
        magnitude = float(net.res_bus_3ph.at[bus_idx, f"vm_{suffix}_pu"])
        angle = math.radians(float(net.res_bus_3ph.at[bus_idx, f"va_{suffix}_degree"]))
        return complex(magnitude * math.cos(angle), magnitude * math.sin(angle))
