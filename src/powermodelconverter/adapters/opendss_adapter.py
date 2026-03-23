from __future__ import annotations

from dataclasses import dataclass
import math
import os
from pathlib import Path
from typing import Any

import opendssdirect as dss
import pandapower as pp

from powermodelconverter.adapters.base import ImportAdapter
from powermodelconverter.adapters.pandapower_adapter import PandapowerAdapter
from powermodelconverter.core.exceptions import ConversionError
from powermodelconverter.core.model import CanonicalCase


_LENGTH_FACTORS_TO_KM = {
    0: 1.0,
    1: 1.609344,
    2: 0.3048 / 1000.0,
    3: 0.001,
    4: 1.0,
    5: 0.3048,
    6: 0.0254 / 1000.0,
    7: 0.01 / 1000.0,
}

_PHASE_BY_NODE = {1: "a", 2: "b", 3: "c"}


@dataclass(slots=True)
class OpenDSSResultSnapshot:
    slack_p_mw: float
    slack_q_mvar: float
    voltages: dict[str, complex]
    node_voltages: dict[str, complex]


class OpenDSSImportAdapter(ImportAdapter):
    source_format = "opendss"

    def __init__(self) -> None:
        self._pandapower = PandapowerAdapter()

    def import_case(self, source: str | Path, **kwargs: Any) -> CanonicalCase:
        path = Path(source).resolve()
        self._compile(path)
        net = self._build_pandapower_net(case_id=path.stem)
        return self._pandapower.to_canonical(
            net,
            case_id=path.stem,
            source_format=self.source_format,
            metadata={
                "import_backend": "opendssdirect",
                "is_unbalanced": self._is_unbalanced_circuit(),
                "phase_count": 3,
            },
            source_path=path,
        )

    def solve_source_case(self, source: str | Path) -> OpenDSSResultSnapshot:
        path = Path(source).resolve()
        self._compile(path)
        dss.Solution.Solve()
        total_power = dss.Circuit.TotalPower()
        return OpenDSSResultSnapshot(
            slack_p_mw=-float(total_power[0]) / 1000.0,
            slack_q_mvar=-float(total_power[1]) / 1000.0,
            voltages=self._extract_bus_voltages(),
            node_voltages=self._extract_node_voltages(),
        )

    def _compile(self, path: Path) -> None:
        dss.Basic.ClearAll()
        previous_cwd = Path.cwd()
        try:
            os.chdir(path.parent)
            command = f"Compile [{path.name}]"
            result = dss.Command(command)
            if "error" in str(result).lower():
                raise ConversionError(f"OpenDSS compile failed for {path}: {result}")
            if int(dss.Basic.NumCircuits()) < 1:
                raise ConversionError(f"OpenDSS did not create an active circuit for {path}.")
        finally:
            os.chdir(previous_cwd)

    def _build_pandapower_net(self, case_id: str) -> Any:
        net = pp.create_empty_network(sn_mva=1.0, f_hz=50.0, name=case_id)
        bus_lookup = self._create_buses(net)
        self._create_ext_grids(net, bus_lookup)
        self._create_lines(net, bus_lookup)
        self._create_transformers(net, bus_lookup)
        self._create_loads(net, bus_lookup)
        if not len(net.bus):
            raise ConversionError("No buses were discovered in the OpenDSS circuit.")
        return net

    def _create_buses(self, net: Any) -> dict[str, int]:
        lookup: dict[str, int] = {}
        for bus_name in dss.Circuit.AllBusNames():
            dss.Circuit.SetActiveBus(bus_name)
            vn_kv = float(dss.Bus.kVBase()) * (3 ** 0.5)
            if vn_kv == 0.0:
                vn_kv = 0.4
            lookup[bus_name.lower()] = pp.create_bus(net, vn_kv=vn_kv, name=bus_name)
        return lookup

    def _create_ext_grids(self, net: Any, bus_lookup: dict[str, int]) -> None:
        if not dss.Vsources.First():
            return
        while True:
            bus_name = dss.CktElement.BusNames()[0].split(".")[0].lower()
            if bus_name not in bus_lookup:
                raise ConversionError(f"OpenDSS source bus {bus_name} not found in bus lookup.")
            pp.create_ext_grid(
                net,
                bus=bus_lookup[bus_name],
                vm_pu=float(dss.Vsources.PU()),
                va_degree=float(dss.Vsources.AngleDeg()),
                name=dss.Vsources.Name(),
                s_sc_max_mva=10000.0,
                s_sc_min_mva=8000.0,
                rx_max=0.1,
                rx_min=0.1,
                r0x0_max=0.1,
                r0x0_min=0.1,
                x0x_max=1.0,
                x0x_min=1.0,
            )
            if not dss.Vsources.Next():
                break

    def _create_lines(self, net: Any, bus_lookup: dict[str, int]) -> None:
        if not dss.Lines.First():
            return
        while True:
            name = dss.Lines.Name()
            bus1 = dss.Lines.Bus1().split(".")[0].lower()
            bus2 = dss.Lines.Bus2().split(".")[0].lower()
            length = float(dss.Lines.Length())
            length_km = length * _LENGTH_FACTORS_TO_KM.get(int(dss.Lines.Units()), 1.0)
            if length_km <= 0:
                length_km = 0.001
            r_ohm = float(dss.Lines.R1()) * length
            x_ohm = float(dss.Lines.X1()) * length
            c_nf = float(dss.Lines.C1()) * length
            r0_ohm = float(dss.Lines.R0()) * length
            x0_ohm = float(dss.Lines.X0()) * length
            c0_nf = float(dss.Lines.C0()) * length
            pp.create_line_from_parameters(
                net,
                from_bus=bus_lookup[bus1],
                to_bus=bus_lookup[bus2],
                length_km=length_km,
                r_ohm_per_km=r_ohm / length_km,
                x_ohm_per_km=x_ohm / length_km,
                c_nf_per_km=c_nf / length_km,
                max_i_ka=max(float(dss.Lines.NormAmps()) / 1000.0, 0.001),
                name=name,
                r0_ohm_per_km=r0_ohm / length_km,
                x0_ohm_per_km=x0_ohm / length_km,
                c0_nf_per_km=c0_nf / length_km,
            )
            if not dss.Lines.Next():
                break

    def _create_transformers(self, net: Any, bus_lookup: dict[str, int]) -> None:
        if not dss.Transformers.First():
            return
        while True:
            name = dss.Transformers.Name()
            buses = [entry.split(".")[0].lower() for entry in dss.CktElement.BusNames()]
            if len(buses) < 2:
                raise ConversionError(f"Transformer {name} does not have two windings.")
            kva = float(dss.Transformers.kVA())
            dss.Transformers.Wdg(1)
            vn_hv_kv = float(dss.Transformers.kV())
            percent_r_hv = float(dss.Transformers.R())
            dss.Transformers.Wdg(2)
            vn_lv_kv = float(dss.Transformers.kV())
            percent_r_lv = float(dss.Transformers.R())
            tap_pos = float(dss.Transformers.Tap())
            percent_r = percent_r_hv + percent_r_lv
            xhl = float(dss.Transformers.Xhl())
            pp.create_transformer_from_parameters(
                net,
                hv_bus=bus_lookup[buses[0]],
                lv_bus=bus_lookup[buses[1]],
                sn_mva=max(kva / 1000.0, 0.001),
                vn_hv_kv=vn_hv_kv,
                vn_lv_kv=vn_lv_kv,
                vk_percent=max(xhl, 0.001),
                vkr_percent=max(percent_r, 0.0),
                pfe_kw=0.0,
                i0_percent=0.0,
                shift_degree=0.0,
                tap_pos=int(round((tap_pos - 1.0) * 100)),
                name=name,
                vector_group="Dyn",
                vk0_percent=max(xhl, 0.001),
                vkr0_percent=max(percent_r, 0.0),
                mag0_percent=100.0,
                mag0_rx=0.0,
                si0_hv_partial=0.9,
            )
            if not dss.Transformers.Next():
                break

    def _create_loads(self, net: Any, bus_lookup: dict[str, int]) -> None:
        if not dss.Loads.First():
            return
        while True:
            bus_name, nodes = self._parse_bus_spec(dss.CktElement.BusNames()[0])
            p_mw = float(dss.Loads.kW()) / 1000.0
            q_mvar = float(dss.Loads.kvar()) / 1000.0
            phases = int(dss.Loads.Phases())
            is_delta = bool(dss.Loads.IsDelta())

            if phases == 3 and sorted(nodes)[:3] == [1, 2, 3] and not is_delta:
                pp.create_load(
                    net,
                    bus=bus_lookup[bus_name],
                    p_mw=p_mw,
                    q_mvar=q_mvar,
                    name=dss.Loads.Name(),
                )
            else:
                phase_nodes = [node for node in nodes if node in _PHASE_BY_NODE]
                phase_count = max(1, len(phase_nodes))
                per_phase_p = p_mw / phase_count
                per_phase_q = q_mvar / phase_count
                kwargs = {
                    "p_a_mw": 0.0,
                    "q_a_mvar": 0.0,
                    "p_b_mw": 0.0,
                    "q_b_mvar": 0.0,
                    "p_c_mw": 0.0,
                    "q_c_mvar": 0.0,
                }
                for node in phase_nodes:
                    phase = _PHASE_BY_NODE[node]
                    kwargs[f"p_{phase}_mw"] = per_phase_p
                    kwargs[f"q_{phase}_mvar"] = per_phase_q
                pp.create_asymmetric_load(
                    net,
                    bus=bus_lookup[bus_name],
                    name=dss.Loads.Name(),
                    type="delta" if is_delta else "wye",
                    **kwargs,
                )
            if not dss.Loads.Next():
                break

    def _extract_bus_voltages(self) -> dict[str, complex]:
        voltages: dict[str, complex] = {}
        for bus_name in dss.Circuit.AllBusNames():
            dss.Circuit.SetActiveBus(bus_name)
            values = dss.Bus.puVmagAngle()
            if len(values) < 2:
                continue
            vm_pu = float(values[0])
            va_rad = math.radians(float(values[1]))
            voltages[bus_name] = complex(vm_pu * math.cos(va_rad), vm_pu * math.sin(va_rad))
        return voltages

    def _extract_node_voltages(self) -> dict[str, complex]:
        node_voltages: dict[str, complex] = {}
        for bus_name in dss.Circuit.AllBusNames():
            dss.Circuit.SetActiveBus(bus_name)
            nodes = dss.Bus.Nodes()
            values = dss.Bus.puVmagAngle()
            for idx, node in enumerate(nodes):
                offset = idx * 2
                if offset + 1 >= len(values):
                    continue
                vm_pu = float(values[offset])
                va_rad = math.radians(float(values[offset + 1]))
                node_voltages[f"{bus_name}.{int(node)}"] = complex(
                    vm_pu * math.cos(va_rad),
                    vm_pu * math.sin(va_rad),
                )
        return node_voltages

    def _is_unbalanced_circuit(self) -> bool:
        for bus_name in dss.Circuit.AllBusNames():
            dss.Circuit.SetActiveBus(bus_name)
            if len(dss.Bus.Nodes()) != 3:
                return True
        if dss.Loads.First():
            while True:
                _, nodes = self._parse_bus_spec(dss.CktElement.BusNames()[0])
                if int(dss.Loads.Phases()) != 3 or sorted(nodes)[:3] != [1, 2, 3]:
                    return True
                if not dss.Loads.Next():
                    break
        return False

    def _parse_bus_spec(self, value: str) -> tuple[str, list[int]]:
        parts = [part.strip() for part in value.split(".") if part.strip()]
        bus_name = parts[0].lower()
        nodes: list[int] = []
        for part in parts[1:]:
            try:
                nodes.append(int(part))
            except ValueError:
                continue
        return bus_name, nodes
