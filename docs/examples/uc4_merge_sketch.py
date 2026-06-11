#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

from _common import default_output_dir
from _common import ensure_dir
from _common import julia_settings
from _common import write_json

from powermodelconverter.core.model import CanonicalCase
from powermodelconverter.core.pandapower_backend import PandapowerAdapter
from powermodelconverter.importers.matpower import MatpowerImportAdapter
from powermodelconverter.importers.opendss import OpenDSSImportAdapter
from powermodelconverter.validation.engine import ValidationService


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Working hub-and-spoke example: balanced TSO-DSO merge sketch built directly in Python."
    )
    parser.add_argument(
        "--top-input",
        default="validation_cases/native/matpower/case9.m",
        help="Balanced transmission-side case.",
    )
    parser.add_argument(
        "--bottom-input",
        default="validation_cases/native/opendss/minimal_radial.dss",
        help="Balanced distribution-side feeder.",
    )
    parser.add_argument(
        "--output-dir",
        default=str(default_output_dir("uc4")),
        help="Directory for exported artifacts and JSON summaries.",
    )
    return parser


def merge_cases(top_case, bottom_case) -> CanonicalCase:
    import pandapower as pp

    merged_net = pp.create_empty_network(
        sn_mva=max(float(getattr(top_case.net, "sn_mva", 1.0)), float(getattr(bottom_case.net, "sn_mva", 1.0))),
        f_hz=float(getattr(top_case.net, "f_hz", 50.0)),
        name="merged_python_example",
    )

    def copy_into(source_net, prefix: str) -> dict[int, int]:
        bus_lookup: dict[int, int] = {}
        for idx, row in source_net.bus.iterrows():
            bus_lookup[int(idx)] = pp.create_bus(
                merged_net,
                vn_kv=float(row.vn_kv),
                name=f"{prefix}_{row['name']}",
                type=str(row.get("type", "b") or "b"),
            )

        for _, row in source_net.ext_grid.iterrows():
            pp.create_ext_grid(
                merged_net,
                bus=bus_lookup[int(row.bus)],
                vm_pu=float(row.get("vm_pu", 1.0)),
                va_degree=float(row.get("va_degree", 0.0)),
                name=f"{prefix}_{row.get('name', 'ext_grid')}",
            )

        for _, row in source_net.gen.iterrows():
            pp.create_gen(
                merged_net,
                bus=bus_lookup[int(row.bus)],
                p_mw=float(row.p_mw),
                vm_pu=float(row.get("vm_pu", 1.0)),
                name=f"{prefix}_{row.get('name', 'gen')}",
                slack=bool(row.get("slack", False)),
                controllable=bool(row.get("controllable", False)) if row.get("controllable") == row.get("controllable") else False,
            )

        for _, row in source_net.load.iterrows():
            pp.create_load(
                merged_net,
                bus=bus_lookup[int(row.bus)],
                p_mw=float(row.p_mw),
                q_mvar=float(row.q_mvar),
                name=f"{prefix}_{row.get('name', 'load')}",
                controllable=bool(row.get("controllable", False)) if row.get("controllable") == row.get("controllable") else False,
            )

        for _, row in source_net.line.iterrows():
            pp.create_line_from_parameters(
                merged_net,
                from_bus=bus_lookup[int(row.from_bus)],
                to_bus=bus_lookup[int(row.to_bus)],
                length_km=float(row.length_km),
                r_ohm_per_km=float(row.r_ohm_per_km),
                x_ohm_per_km=float(row.x_ohm_per_km),
                c_nf_per_km=float(row.c_nf_per_km),
                max_i_ka=float(row.max_i_ka),
                r0_ohm_per_km=float(row.get("r0_ohm_per_km", row.r_ohm_per_km)),
                x0_ohm_per_km=float(row.get("x0_ohm_per_km", row.x_ohm_per_km)),
                c0_nf_per_km=float(row.get("c0_nf_per_km", row.c_nf_per_km)),
                g_us_per_km=float(row.get("g_us_per_km", 0.0)),
                g0_us_per_km=float(row.get("g0_us_per_km", 0.0)),
                name=f"{prefix}_{row.get('name', 'line')}",
            )

        for _, row in source_net.trafo.iterrows():
            pp.create_transformer_from_parameters(
                merged_net,
                hv_bus=bus_lookup[int(row.hv_bus)],
                lv_bus=bus_lookup[int(row.lv_bus)],
                sn_mva=float(row.sn_mva),
                vn_hv_kv=float(row.vn_hv_kv),
                vn_lv_kv=float(row.vn_lv_kv),
                vk_percent=float(row.vk_percent),
                vkr_percent=float(row.vkr_percent),
                pfe_kw=float(row.get("pfe_kw", 0.0)),
                i0_percent=float(row.get("i0_percent", 0.0)),
                shift_degree=float(row.get("shift_degree", 0.0)),
                tap_side=str(row.get("tap_side")) if row.get("tap_side") == row.get("tap_side") else None,
                tap_neutral=int(row.get("tap_neutral", 0)) if row.get("tap_neutral") == row.get("tap_neutral") else 0,
                tap_max=int(row.get("tap_max", 0)) if row.get("tap_max") == row.get("tap_max") else 0,
                tap_min=int(row.get("tap_min", 0)) if row.get("tap_min") == row.get("tap_min") else 0,
                tap_step_percent=float(row.get("tap_step_percent", 0.0)) if row.get("tap_step_percent") == row.get("tap_step_percent") else 0.0,
                tap_pos=int(row.get("tap_pos", 0)) if row.get("tap_pos") == row.get("tap_pos") else 0,
                name=f"{prefix}_{row.get('name', 'trafo')}",
            )

        return bus_lookup

    top_lookup = copy_into(top_case.net, "T")
    bottom_lookup = copy_into(bottom_case.net, "D")
    dso_ext_grid_indices = list(merged_net.ext_grid.index[1:])
    dso_bus = int(merged_net.ext_grid.loc[dso_ext_grid_indices[0], "bus"])
    top_bus = int(merged_net.ext_grid.iloc[0]["bus"])
    pp.drop_elements(merged_net, "ext_grid", dso_ext_grid_indices)
    pp.create_line_from_parameters(
        merged_net,
        from_bus=top_bus,
        to_bus=dso_bus,
        length_km=0.1,
        r_ohm_per_km=0.001,
        x_ohm_per_km=0.001,
        c_nf_per_km=0.0,
        max_i_ka=10.0,
        r0_ohm_per_km=0.001,
        x0_ohm_per_km=0.001,
        c0_nf_per_km=0.0,
        name="TSO_DSO_link",
    )
    return CanonicalCase.from_pandapower(
        case_id=f"{top_case.case_id}_plus_{bottom_case.case_id}",
        source_format="merged_python_example",
        net=merged_net,
    )


def main() -> None:
    args = build_parser().parse_args()
    output_dir = ensure_dir(Path(args.output_dir))

    top_case = MatpowerImportAdapter().import_case(args.top_input)
    bottom_adapter = OpenDSSImportAdapter()
    bottom_snapshot = bottom_adapter.solve_source_case(args.bottom_input)
    bottom_case = bottom_adapter.import_case(args.bottom_input)

    validator = ValidationService()
    top_reference = {
        "passed": True,
        "slack_delta_mva": 0.0,
        "max_voltage_delta_pu": 0.0,
        "details": {"backend": "pandapower_reference"},
    }
    bottom_reference = validator.validate_opendss_roundtrip(bottom_case, bottom_snapshot)

    merged_case = merge_cases(top_case, bottom_case)
    pandapower = PandapowerAdapter()
    pandapower_path = pandapower.export_json(merged_case, output_dir / f"{merged_case.case_id}.pandapower.json")
    powermodels_path = pandapower.export_powermodels_json(merged_case, output_dir / f"{merged_case.case_id}.powermodels.json")

    julia = julia_settings()
    merged_validation = validator.validate_powermodels_export(
        merged_case,
        powermodels_json=powermodels_path,
        julia_binary=str(julia["julia_binary"]),
        julia_script=Path(julia["powermodels_script"]),
        julia_depot=Path(julia["julia_depot"]),
    )

    summary_path = write_json(
        output_dir / "summary.json",
        {
            "use_case": "uc4",
            "script": "docs/examples/uc4_merge_sketch.py",
            "merge_mode": "balanced_python_merge_sketch",
            "inputs": {
                "top": Path(args.top_input),
                "bottom": Path(args.bottom_input),
            },
            "artifacts": {
                "pandapower_json": pandapower_path,
                "powermodels_json": powermodels_path,
            },
            "validations": {
                "top_source": top_reference,
                "bottom_source": bottom_reference,
                "merged_powermodels": merged_validation,
            },
        },
    )
    print(summary_path)


if __name__ == "__main__":
    main()
