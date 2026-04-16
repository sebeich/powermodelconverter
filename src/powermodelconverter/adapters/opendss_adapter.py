from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
import math
import os
from pathlib import Path
import re
import shutil
import tempfile
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


@dataclass(slots=True)
class _TransformerSpec:
    name: str
    buses: tuple[str, str]
    sn_mva: float
    vn_hv_kv: float
    vn_lv_kv: float
    vk_percent: float
    vkr_percent: float
    tap_pos: float
    tap_neutral: float
    tap_min: float
    tap_max: float
    tap_step_percent: float
    phase_count: int
    is_regulator: bool


class OpenDSSImportAdapter(ImportAdapter):
    source_format = "opendss"

    def __init__(self) -> None:
        self._pandapower = PandapowerAdapter()

    def import_case(self, source: str | Path, **kwargs: Any) -> CanonicalCase:
        path = Path(source).resolve()
        with self._prepared_source_path(path) as prepared:
            self._compile(prepared)
            dss.Solution.Solve()
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
        with self._prepared_source_path(path) as prepared:
            self._compile(prepared)
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

    @contextmanager
    def _prepared_source_path(self, path: Path):
        with tempfile.TemporaryDirectory(prefix="pmc_opendss_") as tmpdir:
            temp_root = Path(tmpdir) / path.parent.name
            shutil.copytree(path.parent, temp_root)
            self._add_case_insensitive_redirect_aliases(temp_root)
            yield temp_root / path.name

    def _add_case_insensitive_redirect_aliases(self, root: Path) -> None:
        for dss_file in sorted(root.rglob("*")):
            if not dss_file.is_file() or dss_file.suffix.lower() != ".dss":
                continue
            for reference in _extract_dss_path_references(dss_file.read_text()):
                if _is_absolute_dss_reference(reference):
                    continue
                requested = (dss_file.parent / reference).resolve()
                if requested.exists():
                    continue
                resolved = _resolve_case_insensitive_path(dss_file.parent, reference)
                if resolved is None:
                    continue
                requested.parent.mkdir(parents=True, exist_ok=True)
                if not requested.exists():
                    requested.symlink_to(resolved)

    def _build_pandapower_net(self, case_id: str) -> Any:
        net = pp.create_empty_network(sn_mva=1.0, f_hz=60.0, name=case_id)
        bus_lookup = self._create_buses(net)
        self._create_ext_grids(net, bus_lookup)
        self._create_lines(net, bus_lookup)
        self._create_transformers(net, bus_lookup)
        self._create_capacitors(net, bus_lookup)
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
            if self._is_switch_like_line(name, r_ohm, x_ohm, c_nf):
                pp.create_switch(
                    net,
                    bus=bus_lookup[bus1],
                    element=bus_lookup[bus2],
                    et="b",
                    closed=True,
                    type="LBS",
                    name=name,
                )
                if not dss.Lines.Next():
                    break
                continue
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

    def _is_switch_like_line(self, name: str, r_ohm: float, x_ohm: float, c_nf: float) -> bool:
        if name.lower().startswith("sw"):
            return True
        return abs(x_ohm) < 1e-12 and abs(c_nf) < 1e-12 and abs(r_ohm) <= 1e-3

    def _create_transformers(self, net: Any, bus_lookup: dict[str, int]) -> None:
        specs = self._collect_transformer_specs()
        if not specs:
            return
        for spec in self._aggregate_regulator_specs(specs):
            hv_bus_name, lv_bus_name = spec.buses
            pp.create_transformer_from_parameters(
                net,
                hv_bus=bus_lookup[hv_bus_name],
                lv_bus=bus_lookup[lv_bus_name],
                sn_mva=spec.sn_mva,
                vn_hv_kv=spec.vn_hv_kv,
                vn_lv_kv=spec.vn_lv_kv,
                vk_percent=spec.vk_percent,
                vkr_percent=spec.vkr_percent,
                pfe_kw=0.0,
                i0_percent=0.0,
                shift_degree=0.0,
                tap_pos=spec.tap_pos,
                tap_neutral=spec.tap_neutral,
                tap_min=spec.tap_min,
                tap_max=spec.tap_max,
                tap_step_percent=spec.tap_step_percent,
                tap_side="lv",
                tap_changer_type="Ratio" if spec.tap_step_percent else None,
                name=spec.name,
                vector_group="Dyn",
                vk0_percent=spec.vk_percent,
                vkr0_percent=spec.vkr_percent,
                mag0_percent=100.0,
                mag0_rx=0.0,
                si0_hv_partial=0.9,
            )

    def _collect_transformer_specs(self) -> list[_TransformerSpec]:
        specs: list[_TransformerSpec] = []
        if not dss.Transformers.First():
            return specs
        while True:
            name = dss.Transformers.Name()
            buses = tuple(entry.split(".")[0].lower() for entry in dss.CktElement.BusNames()[:2])
            if len(buses) < 2:
                raise ConversionError(f"Transformer {name} does not have two windings.")
            phase_count = int(dss.CktElement.NumPhases())
            kva = float(dss.Transformers.kVA())
            dss.Transformers.Wdg(1)
            vn_hv_kv = float(dss.Transformers.kV())
            percent_r_hv = float(dss.Transformers.R())
            dss.Transformers.Wdg(2)
            vn_lv_kv = float(dss.Transformers.kV())
            percent_r_lv = float(dss.Transformers.R())
            tap_pos = float(dss.Transformers.Tap())
            min_tap = float(dss.Transformers.MinTap())
            max_tap = float(dss.Transformers.MaxTap())
            num_taps = max(int(dss.Transformers.NumTaps()), 1)
            percent_r = percent_r_hv + percent_r_lv
            xhl = float(dss.Transformers.Xhl())
            tap_step_percent = ((max_tap - min_tap) / num_taps) * 100.0 if num_taps else 0.0
            tap_pos_steps = int(round((tap_pos - 1.0) / (tap_step_percent / 100.0))) if tap_step_percent else 0
            tap_min = int(round((min_tap - 1.0) / (tap_step_percent / 100.0))) if tap_step_percent else 0
            tap_max = int(round((max_tap - 1.0) / (tap_step_percent / 100.0))) if tap_step_percent else 0
            specs.append(
                _TransformerSpec(
                    name=name,
                    buses=buses,
                    sn_mva=max(kva / 1000.0, 0.001),
                    vn_hv_kv=vn_hv_kv,
                    vn_lv_kv=vn_lv_kv,
                    vk_percent=max(xhl, 0.001),
                    vkr_percent=max(percent_r, 0.0),
                    tap_pos=float(tap_pos_steps),
                    tap_neutral=0.0,
                    tap_min=float(tap_min),
                    tap_max=float(tap_max),
                    tap_step_percent=tap_step_percent,
                    phase_count=phase_count,
                    is_regulator=bool(re.match(r"reg\d+[a-z]$", name, re.IGNORECASE)),
                )
            )
            if not dss.Transformers.Next():
                break
        return specs

    def _aggregate_regulator_specs(self, specs: list[_TransformerSpec]) -> list[_TransformerSpec]:
        aggregated: list[_TransformerSpec] = []
        grouped: dict[tuple[str, str], list[_TransformerSpec]] = {}
        for spec in specs:
            if spec.is_regulator and spec.phase_count < 3:
                grouped.setdefault(spec.buses, []).append(spec)
                continue
            aggregated.append(spec)

        for group in grouped.values():
            first = group[0]
            aggregated.append(
                _TransformerSpec(
                    name=re.sub(r"[a-z]$", "", first.name, flags=re.IGNORECASE),
                    buses=first.buses,
                    sn_mva=sum(item.sn_mva for item in group),
                    vn_hv_kv=first.vn_hv_kv,
                    vn_lv_kv=first.vn_lv_kv,
                    # Small regulator impedances make pandapower's 3ph solver singular once taps are active.
                    vk_percent=max(max(item.vk_percent for item in group), 0.3),
                    vkr_percent=max(max(item.vkr_percent for item in group), 0.003),
                    tap_pos=sum(item.tap_pos for item in group) / len(group),
                    tap_neutral=0.0,
                    tap_min=min(item.tap_min for item in group),
                    tap_max=max(item.tap_max for item in group),
                    tap_step_percent=first.tap_step_percent,
                    phase_count=3,
                    is_regulator=True,
                )
            )
        return aggregated

    def _create_loads(self, net: Any, bus_lookup: dict[str, int]) -> None:
        if not dss.Loads.First():
            return
        while True:
            bus_name, nodes = self._parse_bus_spec(dss.CktElement.BusNames()[0])
            p_mw = float(dss.Loads.kW()) / 1000.0
            q_mvar = float(dss.Loads.kvar()) / 1000.0
            phases = int(dss.Loads.Phases())
            is_delta = bool(dss.Loads.IsDelta())
            load_model = int(dss.Loads.Model())

            if phases == 3 and sorted(nodes)[:3] == [1, 2, 3] and not is_delta:
                load_kwargs: dict[str, Any] = {}
                if load_model == 2:
                    load_kwargs.update(
                        {
                            "const_z_p_percent": 100.0,
                            "const_z_q_percent": 100.0,
                        }
                    )
                elif load_model == 5:
                    load_kwargs.update(
                        {
                            "const_i_p_percent": 100.0,
                            "const_i_q_percent": 100.0,
                        }
                    )
                pp.create_load(
                    net,
                    bus=bus_lookup[bus_name],
                    p_mw=p_mw,
                    q_mvar=q_mvar,
                    name=dss.Loads.Name(),
                    **load_kwargs,
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
                    pmc_model=load_model,
                    pmc_nodes=",".join(str(node) for node in phase_nodes),
                    pmc_vminpu=float(dss.Loads.Vminpu()),
                    pmc_vmaxpu=float(dss.Loads.Vmaxpu()),
                    pmc_base_p_a_mw=kwargs["p_a_mw"],
                    pmc_base_q_a_mvar=kwargs["q_a_mvar"],
                    pmc_base_p_b_mw=kwargs["p_b_mw"],
                    pmc_base_q_b_mvar=kwargs["q_b_mvar"],
                    pmc_base_p_c_mw=kwargs["p_c_mw"],
                    pmc_base_q_c_mvar=kwargs["q_c_mvar"],
                    **kwargs,
                )
            if not dss.Loads.Next():
                break

    def _create_capacitors(self, net: Any, bus_lookup: dict[str, int]) -> None:
        if not dss.Capacitors.First():
            return
        while True:
            name = dss.Capacitors.Name()
            bus_name, nodes = self._parse_bus_spec(dss.CktElement.BusNames()[0])
            phases = int(dss.CktElement.NumPhases())
            is_delta = bool(dss.Capacitors.IsDelta())
            q_mvar_total = float(dss.Capacitors.kvar()) / 1000.0
            phase_nodes = [node for node in nodes if node in _PHASE_BY_NODE]
            if phases >= 3 and not phase_nodes:
                phase_nodes = [1, 2, 3]
            phase_count = max(1, len(phase_nodes))
            per_phase_q = q_mvar_total / phase_count
            kwargs = {
                "p_a_mw": 0.0,
                "p_b_mw": 0.0,
                "p_c_mw": 0.0,
                "q_a_mvar": 0.0,
                "q_b_mvar": 0.0,
                "q_c_mvar": 0.0,
            }
            for node in phase_nodes:
                phase = _PHASE_BY_NODE[node]
                kwargs[f"q_{phase}_mvar"] = per_phase_q
            pp.create_asymmetric_sgen(
                net,
                bus=bus_lookup[bus_name],
                name=name,
                type="delta" if is_delta else "wye",
                **kwargs,
            )
            if not dss.Capacitors.Next():
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


_DSS_PATH_COMMAND = re.compile(r"^\s*(redirect|compile)\s+(.+?)\s*$", re.IGNORECASE)


def _extract_dss_path_references(contents: str) -> list[str]:
    references: list[str] = []
    for line in contents.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("!") or stripped.startswith("//"):
            continue
        match = _DSS_PATH_COMMAND.match(stripped)
        if not match:
            continue
        candidate = match.group(2).split("!")[0].strip()
        candidate = candidate.strip("\"'()[]")
        if candidate:
            references.append(candidate)
    return references


def _is_absolute_dss_reference(reference: str) -> bool:
    pure = Path(reference)
    return pure.is_absolute() or re.match(r"^[A-Za-z]:[/\\\\]", reference) is not None


def _resolve_case_insensitive_path(base_dir: Path, reference: str) -> Path | None:
    current = base_dir
    for part in Path(reference).parts:
        try:
            current = next(entry for entry in current.iterdir() if entry.name.lower() == part.lower())
        except StopIteration:
            return None
    return current
