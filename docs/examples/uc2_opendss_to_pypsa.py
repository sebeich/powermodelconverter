#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

from _common import default_output_dir
from _common import ensure_dir
from _common import write_csv
from _common import write_json

from powermodelconverter.core.pandapower_backend import PandapowerAdapter
from powermodelconverter.importers.opendss import OpenDSSImportAdapter
from powermodelconverter.importers.pypsa import PypsaAdapter
from powermodelconverter.validation.engine import ValidationService


UNIT_COST_EUR_PER_MW = {
    "pv": 700_000,
    "bess": 450_000,
    "heat_pump": 300_000,
}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Working hub-and-spoke example: OpenDSS to pandapower to PyPSA with planning-ready exports."
    )
    parser.add_argument(
        "--input",
        default="validation_cases/native/opendss/minimal_radial.dss",
        help="OpenDSS input case.",
    )
    parser.add_argument(
        "--output-dir",
        default=str(default_output_dir("uc2")),
        help="Directory for exported artifacts and CSV summaries.",
    )
    return parser


def build_screening_table(case) -> list[list[object]]:
    bus_names = {int(idx): str(row["name"]) for idx, row in case.net.bus.iterrows()}
    load_by_bus: dict[int, float] = {}
    for _, row in case.net.load.iterrows():
        bus = int(row.bus)
        load_by_bus[bus] = load_by_bus.get(bus, 0.0) + float(row.p_mw)

    rows: list[list[object]] = []
    for bus_idx, bus_name in sorted(bus_names.items()):
        load_mw = load_by_bus.get(bus_idx, 0.0)
        rows.append([bus_name, "pv", round(max(load_mw * 0.8, 0.05), 6), UNIT_COST_EUR_PER_MW["pv"]])
        rows.append([bus_name, "bess", round(max(load_mw * 0.4, 0.02), 6), UNIT_COST_EUR_PER_MW["bess"]])
        rows.append([bus_name, "heat_pump", round(max(load_mw * 0.25, 0.01), 6), UNIT_COST_EUR_PER_MW["heat_pump"]])
    return rows


def main() -> None:
    args = build_parser().parse_args()
    output_dir = ensure_dir(Path(args.output_dir))

    adapter = OpenDSSImportAdapter()
    source_snapshot = adapter.solve_source_case(args.input)
    case = adapter.import_case(args.input)

    validator = ValidationService()
    opendss_to_pandapower = validator.validate_opendss_roundtrip(case, source_snapshot)

    pandapower = PandapowerAdapter()
    pypsa = PypsaAdapter()
    pandapower_path = pandapower.export_json(case, output_dir / f"{case.case_id}.pandapower.json")
    pypsa_path = pypsa.export_netcdf(case, output_dir / f"{case.case_id}.pypsa.nc")
    pypsa_validation = validator.validate_pypsa_export(case, pypsa_path=pypsa_path)

    screening_rows = build_screening_table(case)
    expansion_path = write_csv(
        output_dir / "expansion.csv",
        ["bus", "technology", "installed_mw", "unit_cost_eur_per_mw"],
        screening_rows,
    )
    summary_path = write_json(
        output_dir / "summary.json",
        {
            "use_case": "uc2",
            "script": "docs/examples/uc2_opendss_to_pypsa.py",
            "planning_mode": "deterministic_screening_proxy",
            "input": Path(args.input),
            "case_id": case.case_id,
            "artifacts": {
                "pandapower_json": pandapower_path,
                "pypsa_netcdf": pypsa_path,
                "expansion_csv": expansion_path,
            },
            "validations": {
                "opendss_to_pandapower": opendss_to_pandapower,
                "pandapower_to_pypsa": pypsa_validation,
            },
            "cost_basis": UNIT_COST_EUR_PER_MW,
        },
    )
    print(summary_path)


if __name__ == "__main__":
    main()
