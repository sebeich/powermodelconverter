from __future__ import annotations

import math
import shutil
from pathlib import Path

import pandas as pd

from powermodelconverter.core.pandapower_backend import PandapowerAdapter
from powermodelconverter.core.model import CanonicalCase
from powermodelconverter.core.registry import register_target_routes
from powermodelconverter.validation.tolerances import BALANCED_TOLERANCES


class OpenDSSExportAdapter:
    format_name = "opendss"

    def __init__(self) -> None:
        self._pandapower = PandapowerAdapter()

    def export_case(self, case: CanonicalCase, destination: str | Path) -> Path:
        path = Path(destination)
        path.parent.mkdir(parents=True, exist_ok=True)

        if case.source_format == "opendss" and case.source_path is not None and case.source_path.suffix.lower() == ".dss":
            if case.source_path.resolve() == path.resolve():
                return path
            shutil.copy2(case.source_path, path)
            return path

        net = self._pandapower.to_net(case)
        solved_net = self._pandapower.run_power_flow(case) if not case.is_unbalanced else None
        self._validate_supported_net(net)
        path.write_text(self._render_dss(case.case_id, net, solved_net))
        return path

    def _render_dss(self, case_id: str, net: object, solved_net: object | None) -> str:
        source_bus_idx, source_vm_pu, source_va_degree = self._slack_source(net, solved_net)
        source_bus = net.bus.loc[source_bus_idx]
        source_bus_name = self._bus_name(source_bus.get("name"), source_bus_idx)
        base_kv = float(source_bus.vn_kv)
        lines: list[str] = [
            "Clear",
            "",
            f"New Circuit.{self._sanitize_name(case_id)} basekv={base_kv:.12g} pu={source_vm_pu:.12g} "
            f"phases=3 bus1={source_bus_name} angle={source_va_degree:.12g} "
            "mvasc3=1000000 mvasc1=1000000",
            "",
        ]

        for idx, row in net.trafo.iterrows():
            name = self._sanitize_name(self._row_name(row, "Trafo", idx))
            hv_bus_name = self._bus_name(net.bus.loc[int(row.hv_bus)].get("name"), int(row.hv_bus))
            lv_bus_name = self._bus_name(net.bus.loc[int(row.lv_bus)].get("name"), int(row.lv_bus))
            hv_conn, lv_conn = self._transformer_connections(
                str(row.get("vector_group", "") or ""),
                float(row.get("shift_degree", 0.0) or 0.0),
            )
            r_each = max(float(row.vkr_percent) / 2.0, 0.0)
            lines.extend(
                [
                    f"New Transformer.{name} phases=3 windings=2 XHL={float(row.vk_percent):.12g}",
                    f"~ wdg=1 bus={hv_bus_name} conn={hv_conn} kv={float(row.vn_hv_kv):.12g} "
                    f"kva={float(row.sn_mva) * 1000.0:.12g} %r={r_each:.12g}",
                    f"~ wdg=2 bus={lv_bus_name} conn={lv_conn} kv={float(row.vn_lv_kv):.12g} "
                    f"kva={float(row.sn_mva) * 1000.0:.12g} %r={r_each:.12g}",
                    "",
                ]
            )

        for idx, row in net.line.iterrows():
            name = self._sanitize_name(self._row_name(row, "Line", idx))
            from_bus = self._bus_name(net.bus.loc[int(row.from_bus)].get("name"), int(row.from_bus))
            to_bus = self._bus_name(net.bus.loc[int(row.to_bus)].get("name"), int(row.to_bus))
            r1 = float(row.r_ohm_per_km)
            x1 = float(row.x_ohm_per_km)
            c1 = float(row.c_nf_per_km)
            r0 = self._line_param(row, "r0_ohm_per_km", r1)
            x0 = self._line_param(row, "x0_ohm_per_km", x1)
            c0 = self._line_param(row, "c0_nf_per_km", c1)
            lines.extend(
                [
                    f"New Line.{name} phases=3 bus1={from_bus}.1.2.3 bus2={to_bus}.1.2.3",
                    f"~ r1={r1:.12g} x1={x1:.12g} "
                    f"r0={r0:.12g} x0={x0:.12g} "
                    f"c1={c1:.12g} c0={c0:.12g} "
                    f"length={float(row.length_km):.12g} units=km",
                    "",
                ]
            )

        for idx, row in net.load.iterrows():
            bus = net.bus.loc[int(row.bus)]
            bus_name = self._bus_name(bus.get("name"), int(row.bus))
            name = self._sanitize_name(self._row_name(row, "Load", idx))
            lines.append(
                f"New Load.{name} bus1={bus_name}.1.2.3 phases=3 conn=wye model=1 "
                f"kv={float(bus.vn_kv):.12g} kW={float(row.p_mw) * 1000.0:.12g} kvar={float(row.q_mvar) * 1000.0:.12g}"
            )

        for idx, row in net.asymmetric_load.iterrows():
            bus = net.bus.loc[int(row.bus)]
            bus_name = self._bus_name(bus.get("name"), int(row.bus))
            kv_ln = float(bus.vn_kv) / math.sqrt(3.0)
            load_type = "delta" if str(row.get("type", "wye")).lower() == "delta" else "wye"
            for phase_idx, phase in enumerate(("a", "b", "c"), start=1):
                p_mw = float(row[f"p_{phase}_mw"])
                q_mvar = float(row[f"q_{phase}_mvar"])
                if abs(p_mw) < 1e-15 and abs(q_mvar) < 1e-15:
                    continue
                kv = float(bus.vn_kv) if load_type == "delta" else kv_ln
                name = self._sanitize_name(f"{self._row_name(row, 'Load', idx)}_{phase.upper()}")
                lines.append(
                    f"New Load.{name} bus1={bus_name}.{phase_idx} phases=1 conn={load_type} model=1 "
                    f"kv={kv:.12g} kW={p_mw * 1000.0:.12g} kvar={q_mvar * 1000.0:.12g}"
                )

        lines.extend(
            [
                "",
                f"Set Voltagebases=[{', '.join(self._voltage_bases(net.bus['vn_kv']))}]",
                "CalcVoltageBases",
                "Solve",
                "",
            ]
        )
        return "\n".join(lines)

    def _validate_supported_net(self, net: object) -> None:
        ext_grid_count = len(getattr(net, "ext_grid"))
        gen_table = getattr(net, "gen")
        slack_gen_count = int(gen_table.get("slack", False).fillna(False).sum()) if len(gen_table) else 0
        if not ((ext_grid_count == 1 and len(gen_table) == 0) or (ext_grid_count == 0 and slack_gen_count == 1 and len(gen_table) == 1)):
            raise ValueError("OpenDSS export currently supports either one ext_grid or one slack generator.")
        if len(getattr(net, "sgen")) or len(getattr(net, "asymmetric_sgen")):
            raise ValueError("OpenDSS export currently supports load-only cases without static generators.")
        if len(getattr(net, "switch")):
            switch_table = net.switch
            if "closed" in switch_table.columns and len(switch_table[switch_table["closed"] == False]):
                raise ValueError("OpenDSS export currently does not support open switches.")

    def _slack_source(self, net: object, solved_net: object | None) -> tuple[int, float, float]:
        if len(net.ext_grid):
            source = net.ext_grid.iloc[0]
            return int(source.bus), float(source.vm_pu), float(source.va_degree)

        source = net.gen.loc[net.gen["slack"].fillna(False)].iloc[0]
        bus_idx = int(source.bus)
        if solved_net is not None:
            vm_pu = float(solved_net.res_bus.at[bus_idx, "vm_pu"])
            va_degree = float(solved_net.res_bus.at[bus_idx, "va_degree"])
        else:
            vm_pu = float(source.get("vm_pu", 1.0) or 1.0)
            va_degree = 0.0
        return bus_idx, vm_pu, va_degree

    def _row_name(self, row: pd.Series, fallback: str, index: object) -> str:
        value = row.get("name")
        if value is None or (isinstance(value, float) and math.isnan(value)):
            return f"{fallback}_{index}"
        text = str(value).strip()
        return text if text else f"{fallback}_{index}"

    def _bus_name(self, value: object, fallback_idx: int) -> str:
        if value is None or (isinstance(value, float) and math.isnan(value)):
            return f"BUS{fallback_idx}"
        text = str(value).strip()
        return self._sanitize_name(text if text else f"BUS{fallback_idx}")

    def _sanitize_name(self, value: str) -> str:
        safe = "".join(ch if ch.isalnum() or ch == "_" else "_" for ch in value.strip())
        return safe or "UNNAMED"

    def _transformer_connections(self, vector_group: str, shift_degree: float) -> tuple[str, str]:
        if abs(shift_degree) < 1e-9:
            return "wye", "wye"
        text = vector_group.lower()
        if text.startswith("dyn"):
            return "delta", "wye"
        if text.startswith("ynd"):
            return "wye", "delta"
        if text.startswith("dd"):
            return "delta", "delta"
        return "wye", "wye"

    def _voltage_bases(self, values: pd.Series) -> list[str]:
        unique = sorted({float(value) for value in values if float(value) > 0})
        return [f"{value:.12g}" for value in unique]

    def _line_param(self, row: pd.Series, field: str, fallback: float) -> float:
        value = row.get(field, fallback)
        if value is None:
            return fallback
        numeric = float(value)
        if math.isnan(numeric):
            return fallback
        return numeric


def export_opendss(case: CanonicalCase, output_path: str | Path, **kwargs: object) -> Path:
    return OpenDSSExportAdapter().export_case(case, output_path, **kwargs)


register_target_routes(target_tool="opendss", exporter=export_opendss, tolerances=BALANCED_TOLERANCES)


__all__ = ["OpenDSSExportAdapter", "export_opendss"]
