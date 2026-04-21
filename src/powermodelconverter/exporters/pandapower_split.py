from __future__ import annotations

import math
import re
from pathlib import Path
from typing import Any

import opendssdirect as dss
import pandapower as pp

from powermodelconverter.importers.opendss import (
    _LENGTH_FACTORS_TO_KM,
    _PHASE_BY_NODE,
    OpenDSSImportAdapter,
)
from powermodelconverter.core.exceptions import ConversionError
from powermodelconverter.core.model import CanonicalCase
from powermodelconverter.core.registry import register_target_routes
from powermodelconverter.validation.tolerances import UNBALANCED_PP_TOLERANCES


class PandapowerSplitExportAdapter:
    target_format = "pandapower_split"

    def __init__(self) -> None:
        self._opendss = OpenDSSImportAdapter()

    def export_case(self, case: CanonicalCase, destination: str | Path) -> Path:
        if case.source_format != "opendss" or case.source_path is None:
            raise ValueError("pandapower_split export currently requires an original OpenDSS source path.")

        path = Path(destination)
        path.parent.mkdir(parents=True, exist_ok=True)
        net = self._build_split_net(case.case_id, case.source_path)
        pp.to_json(net, str(path))
        return path

    def _build_split_net(self, case_id: str, source_path: Path) -> Any:
        with self._opendss._prepared_source_path(source_path.resolve()) as prepared:
            self._opendss._compile(prepared)
            dss.Solution.Solve()
            net = pp.create_empty_network(sn_mva=1.0, f_hz=60.0, name=case_id)
            node_lookup = self._create_phase_buses(net)
            self._create_ext_grids(net, node_lookup)
            self._create_lines(net, node_lookup)
            self._create_transformers(net, node_lookup)
            self._create_capacitors(net, node_lookup)
            self._create_loads(net, node_lookup)
            if not len(net.bus):
                raise ConversionError("No phase buses were created for the pandapower_split export.")
            return net

    def _create_phase_buses(self, net: Any) -> dict[tuple[str, int], int]:
        lookup: dict[tuple[str, int], int] = {}
        for bus_name in dss.Circuit.AllBusNames():
            dss.Circuit.SetActiveBus(bus_name)
            vn_kv = float(dss.Bus.kVBase())
            if vn_kv <= 0:
                vn_kv = 0.4 / math.sqrt(3.0)
            for node in dss.Bus.Nodes():
                node_int = int(node)
                lookup[(bus_name.lower(), node_int)] = pp.create_bus(
                    net,
                    vn_kv=vn_kv,
                    name=f"{bus_name}.{node_int}",
                )
        return lookup

    def _create_ext_grids(self, net: Any, node_lookup: dict[tuple[str, int], int]) -> None:
        phase_angles = {1: 0.0, 2: -120.0, 3: 120.0}
        if not dss.Vsources.First():
            return
        while True:
            bus_name, nodes = self._parse_bus_spec(dss.CktElement.BusNames()[0])
            phase_nodes = nodes or [1, 2, 3]
            for node in phase_nodes:
                bus_idx = node_lookup.get((bus_name, node))
                if bus_idx is None:
                    continue
                pp.create_ext_grid(
                    net,
                    bus=bus_idx,
                    vm_pu=float(dss.Vsources.PU()),
                    va_degree=float(dss.Vsources.AngleDeg()) + phase_angles.get(node, 0.0),
                    name=f"{dss.Vsources.Name()}_{node}",
                )
            if not dss.Vsources.Next():
                break

    def _create_lines(self, net: Any, node_lookup: dict[tuple[str, int], int]) -> None:
        if not dss.Lines.First():
            return
        while True:
            name = dss.Lines.Name()
            bus1_name, bus1_nodes = self._parse_bus_spec(dss.Lines.Bus1())
            bus2_name, bus2_nodes = self._parse_bus_spec(dss.Lines.Bus2())
            common_nodes = sorted(set(bus1_nodes or [1, 2, 3]).intersection(bus2_nodes or [1, 2, 3]))
            length = float(dss.Lines.Length())
            length_km = length * _LENGTH_FACTORS_TO_KM.get(int(dss.Lines.Units()), 1.0)
            if length_km <= 0:
                length_km = 0.001
            r_ohm = float(dss.Lines.R1()) * length
            x_ohm = float(dss.Lines.X1()) * length
            c_nf = float(dss.Lines.C1()) * length
            for node in common_nodes:
                from_bus = node_lookup.get((bus1_name, node))
                to_bus = node_lookup.get((bus2_name, node))
                if from_bus is None or to_bus is None:
                    continue
                if self._opendss._is_switch_like_line(name, r_ohm, x_ohm, c_nf):
                    pp.create_switch(
                        net,
                        bus=from_bus,
                        element=to_bus,
                        et="b",
                        closed=True,
                        type="LBS",
                        name=f"{name}_{node}",
                    )
                    continue
            for idx, node in enumerate(common_nodes):
                from_bus = node_lookup.get((bus1_name, node))
                to_bus = node_lookup.get((bus2_name, node))
                if from_bus is None or to_bus is None:
                    continue
                pp.create_line_from_parameters(
                    net,
                    from_bus=from_bus,
                    to_bus=to_bus,
                    length_km=length_km,
                    r_ohm_per_km=r_ohm / length_km,
                    x_ohm_per_km=x_ohm / length_km,
                    c_nf_per_km=c_nf / length_km,
                    max_i_ka=max(float(dss.Lines.NormAmps()) / 1000.0, 0.001),
                    name=f"{name}_{node}",
                )
            if not dss.Lines.Next():
                break

    def _create_transformers(self, net: Any, node_lookup: dict[tuple[str, int], int]) -> None:
        if not dss.Transformers.First():
            return
        while True:
            name = dss.Transformers.Name()
            bus_specs = [self._parse_bus_spec(value) for value in dss.CktElement.BusNames()[:2]]
            if len(bus_specs) < 2:
                raise ConversionError(f"Transformer {name} does not have two windings.")
            (hv_bus_name, hv_nodes), (lv_bus_name, lv_nodes) = bus_specs
            common_nodes = sorted(set(hv_nodes or [1, 2, 3]).intersection(lv_nodes or [1, 2, 3]))
            phase_count = max(len(common_nodes), 1)
            kva_total = float(dss.Transformers.kVA())
            dss.Transformers.Wdg(1)
            percent_r_hv = float(dss.Transformers.R())
            dss.Transformers.Wdg(2)
            percent_r_lv = float(dss.Transformers.R())
            tap = float(dss.Transformers.Tap())
            min_tap = float(dss.Transformers.MinTap())
            max_tap = float(dss.Transformers.MaxTap())
            num_taps = max(int(dss.Transformers.NumTaps()), 1)
            tap_step_percent = ((max_tap - min_tap) / num_taps) * 100.0 if num_taps else 0.0
            tap_pos = int(round((tap - 1.0) / (tap_step_percent / 100.0))) if tap_step_percent else 0
            tap_min = int(round((min_tap - 1.0) / (tap_step_percent / 100.0))) if tap_step_percent else 0
            tap_max = int(round((max_tap - 1.0) / (tap_step_percent / 100.0))) if tap_step_percent else 0
            is_regulator = bool(re.match(r"reg\d+[a-z]$", name, re.IGNORECASE))
            for node in common_nodes:
                hv_bus = node_lookup.get((hv_bus_name, node))
                lv_bus = node_lookup.get((lv_bus_name, node))
                if hv_bus is None or lv_bus is None:
                    continue
                hv_vn = float(net.bus.at[hv_bus, "vn_kv"])
                lv_vn = float(net.bus.at[lv_bus, "vn_kv"])
                vk_percent = max(float(dss.Transformers.Xhl()), 0.3 if is_regulator else 0.01)
                vkr_percent = max(percent_r_hv + percent_r_lv, 0.003 if is_regulator else 0.00001)
                pp.create_transformer_from_parameters(
                    net,
                    hv_bus=hv_bus,
                    lv_bus=lv_bus,
                    sn_mva=max(kva_total / 1000.0 / phase_count, 0.001),
                    vn_hv_kv=hv_vn,
                    vn_lv_kv=lv_vn,
                    vk_percent=vk_percent,
                    vkr_percent=vkr_percent,
                    pfe_kw=0.0,
                    i0_percent=0.0,
                    shift_degree=0.0,
                    tap_pos=tap_pos,
                    tap_neutral=0,
                    tap_min=tap_min,
                    tap_max=tap_max,
                    tap_step_percent=tap_step_percent,
                    tap_side="lv",
                    tap_changer_type="Ratio" if tap_step_percent else None,
                    name=f"{name}_{node}",
                    vector_group="Dyn",
                    vk0_percent=vk_percent,
                    vkr0_percent=vkr_percent,
                    mag0_percent=100.0,
                    mag0_rx=0.0,
                    si0_hv_partial=0.9,
                )
            if not dss.Transformers.Next():
                break

    def _create_capacitors(self, net: Any, node_lookup: dict[tuple[str, int], int]) -> None:
        if not dss.Capacitors.First():
            return
        while True:
            name = dss.Capacitors.Name()
            bus_name, nodes = self._parse_bus_spec(dss.CktElement.BusNames()[0])
            phase_nodes = [node for node in nodes if node in _PHASE_BY_NODE] or [1, 2, 3]
            q_mvar_total = float(dss.Capacitors.kvar()) / 1000.0
            per_phase_q = q_mvar_total / max(len(phase_nodes), 1)
            for node in phase_nodes:
                bus_idx = node_lookup.get((bus_name, node))
                if bus_idx is None:
                    continue
                pp.create_sgen(
                    net,
                    bus=bus_idx,
                    p_mw=0.0,
                    q_mvar=per_phase_q,
                    name=f"{name}_{node}",
                )
            if not dss.Capacitors.Next():
                break

    def _create_loads(self, net: Any, node_lookup: dict[tuple[str, int], int]) -> None:
        if not dss.Loads.First():
            return
        while True:
            name = dss.Loads.Name()
            bus_name, nodes = self._parse_bus_spec(dss.CktElement.BusNames()[0])
            phases = int(dss.Loads.Phases())
            is_delta = bool(dss.Loads.IsDelta())
            load_model = int(dss.Loads.Model())
            p_mw = float(dss.Loads.kW()) / 1000.0
            q_mvar = float(dss.Loads.kvar()) / 1000.0
            phase_nodes = [node for node in nodes if node in _PHASE_BY_NODE]
            if phases >= 3 and not phase_nodes:
                phase_nodes = [1, 2, 3]
            load_count = max(len(phase_nodes), 1)
            per_phase_p = p_mw / load_count
            per_phase_q = q_mvar / load_count
            load_kwargs = self._load_model_kwargs(load_model)
            if is_delta and len(phase_nodes) >= 2:
                # Approximate delta demand by distributing it across the connected phase buses.
                per_phase_p = p_mw / len(phase_nodes)
                per_phase_q = q_mvar / len(phase_nodes)
            for node in phase_nodes:
                bus_idx = node_lookup.get((bus_name, node))
                if bus_idx is None:
                    continue
                pp.create_load(
                    net,
                    bus=bus_idx,
                    p_mw=per_phase_p,
                    q_mvar=per_phase_q,
                    name=f"{name}_{node}",
                    type="wye",
                    **load_kwargs,
                )
            if not dss.Loads.Next():
                break

    def _load_model_kwargs(self, load_model: int) -> dict[str, float]:
        if load_model == 2:
            return {
                "const_z_p_percent": 100.0,
                "const_z_q_percent": 100.0,
            }
        if load_model == 5:
            return {
                "const_i_p_percent": 100.0,
                "const_i_q_percent": 100.0,
            }
        return {}

    def _parse_bus_spec(self, value: str) -> tuple[str, list[int]]:
        return self._opendss._parse_bus_spec(value)


def export_pandapower_split(case: CanonicalCase, output_path: str | Path, **_: object) -> Path:
    return PandapowerSplitExportAdapter().export_case(case, output_path)


register_target_routes(
    target_tool="pandapower_split",
    exporter=export_pandapower_split,
    tolerances=UNBALANCED_PP_TOLERANCES,
)


__all__ = ["PandapowerSplitExportAdapter", "export_pandapower_split"]
