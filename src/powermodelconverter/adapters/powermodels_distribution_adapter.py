from __future__ import annotations

import math
import shutil
from pathlib import Path

import pandas as pd

from powermodelconverter.adapters.pandapower_adapter import PandapowerAdapter
from powermodelconverter.core.model import CanonicalCase


class PowerModelsDistributionAdapter:
    format_name = "powermodelsdistribution"

    def __init__(self) -> None:
        self._pandapower = PandapowerAdapter()

    def export_input(self, case: CanonicalCase, destination: str | Path) -> Path:
        if not case.is_unbalanced:
            raise ValueError("PowerModelsDistribution export is intended for unbalanced cases.")

        source_path = case.source_path
        if case.source_format == "opendss" and source_path is not None and source_path.suffix.lower() == ".dss":
            path = Path(destination)
            path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source_path, path)
            return path

        if case.source_format == "pandapower":
            return self._export_pandapower_unbalanced_to_dss(case, destination)

        raise ValueError(
            "Unbalanced PowerModelsDistribution export is currently implemented for OpenDSS- and pandapower-source cases only."
        )

    def _export_pandapower_unbalanced_to_dss(self, case: CanonicalCase, destination: str | Path) -> Path:
        net = self._pandapower.to_net(case)
        self._validate_supported_unbalanced_net(net)

        path = Path(destination)
        path.parent.mkdir(parents=True, exist_ok=True)

        source = net.ext_grid.iloc[0]
        source_bus = net.bus.loc[int(source.bus)]
        source_bus_name = self._bus_name(source_bus.get("name"), int(source.bus))
        base_kv = float(source_bus.vn_kv)
        angle = float(source.va_degree)
        pu = float(source.vm_pu)

        lines: list[str] = [
            "Clear",
            "",
            f"New Circuit.{self._sanitize_name(case.case_id)} basekv={base_kv:.12g} pu={pu:.12g} phases=3 "
            f"bus1={source_bus_name} angle={angle:.12g}",
            "",
        ]

        for _, row in net.trafo.iterrows():
            name = self._sanitize_name(self._row_name(row, "Trafo"))
            hv_bus_name = self._bus_name(net.bus.loc[int(row.hv_bus)].get("name"), int(row.hv_bus))
            lv_bus_name = self._bus_name(net.bus.loc[int(row.lv_bus)].get("name"), int(row.lv_bus))
            vector_group = str(row.get("vector_group", "") or "").lower()
            hv_conn, lv_conn = self._transformer_connections(vector_group)
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

        for _, row in net.line.iterrows():
            name = self._sanitize_name(self._row_name(row, "Line"))
            from_bus = self._bus_name(net.bus.loc[int(row.from_bus)].get("name"), int(row.from_bus))
            to_bus = self._bus_name(net.bus.loc[int(row.to_bus)].get("name"), int(row.to_bus))
            lines.extend(
                [
                    f"New Line.{name} phases=3 bus1={from_bus}.1.2.3 bus2={to_bus}.1.2.3",
                    f"~ r1={float(row.r_ohm_per_km):.12g} x1={float(row.x_ohm_per_km):.12g} "
                    f"r0={float(row.r0_ohm_per_km):.12g} x0={float(row.x0_ohm_per_km):.12g} "
                    f"c1={float(row.c_nf_per_km):.12g} c0={float(row.c0_nf_per_km):.12g} "
                    f"length={float(row.length_km):.12g} units=km",
                    "",
                ]
            )

        for _, row in net.asymmetric_load.iterrows():
            bus = net.bus.loc[int(row.bus)]
            bus_name = self._bus_name(bus.get("name"), int(row.bus))
            kv_ln = float(bus.vn_kv) / math.sqrt(3.0)
            load_type = "delta" if str(row.get("type", "wye")).lower() == "delta" else "wye"
            for phase_idx, phase in enumerate(("a", "b", "c"), start=1):
                p_mw = float(row[f"p_{phase}_mw"])
                q_mvar = float(row[f"q_{phase}_mvar"])
                if abs(p_mw) < 1e-15 and abs(q_mvar) < 1e-15:
                    continue
                load_name = self._sanitize_name(f"{self._row_name(row, 'Load')}_{phase.upper()}")
                kv = float(bus.vn_kv) if load_type == "delta" else kv_ln
                lines.append(
                    f"New Load.{load_name} bus1={bus_name}.{phase_idx} phases=1 conn={load_type} model=1 "
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
        path.write_text("\n".join(lines))
        return path

    def _validate_supported_unbalanced_net(self, net: object) -> None:
        if len(getattr(net, "ext_grid")) != 1:
            raise ValueError("Pandapower -> PowerModelsDistribution export currently supports exactly one ext_grid.")
        if len(getattr(net, "asymmetric_sgen")):
            raise ValueError("Pandapower -> PowerModelsDistribution export does not yet support asymmetric_sgen.")
        if len(getattr(net, "load")) or len(getattr(net, "sgen")):
            raise ValueError("Pandapower -> PowerModelsDistribution export expects native asymmetric loads only.")
        if len(getattr(net, "switch")):
            active = net.switch[net.switch.get("closed", True) == False]
            if len(active):
                raise ValueError("Pandapower -> PowerModelsDistribution export does not yet support open switches.")

    def _row_name(self, row: pd.Series, fallback: str) -> str:
        value = row.get("name")
        if value is None or (isinstance(value, float) and math.isnan(value)):
            return fallback
        text = str(value).strip()
        return text if text else fallback

    def _bus_name(self, value: object, fallback_idx: int) -> str:
        if value is None or (isinstance(value, float) and math.isnan(value)):
            return f"BUS{fallback_idx}"
        text = str(value).strip()
        return self._sanitize_name(text if text else f"BUS{fallback_idx}")

    def _sanitize_name(self, value: str) -> str:
        safe = "".join(ch if ch.isalnum() or ch == "_" else "_" for ch in value.strip())
        return safe or "UNNAMED"

    def _transformer_connections(self, vector_group: str) -> tuple[str, str]:
        if vector_group.startswith("dyn"):
            return "delta", "wye"
        if vector_group.startswith("ynd"):
            return "wye", "delta"
        return "wye", "wye"

    def _voltage_bases(self, values: pd.Series) -> list[str]:
        unique = sorted({float(value) for value in values if float(value) > 0})
        return [f"{value:.12g}" for value in unique]
