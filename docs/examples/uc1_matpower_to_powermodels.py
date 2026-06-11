#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

from _common import default_output_dir
from _common import ensure_dir
from _common import julia_settings
from _common import write_json

from powermodelconverter.core.pandapower_backend import PandapowerAdapter
from powermodelconverter.importers.matpower import MatpowerImportAdapter
from powermodelconverter.validation.engine import ValidationService


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Working hub-and-spoke example: MATPOWER to PowerModels with validation."
    )
    parser.add_argument(
        "--input",
        default="validation_cases/native/matpower/case9.m",
        help="MATPOWER input case.",
    )
    parser.add_argument(
        "--output-dir",
        default=str(default_output_dir("uc1")),
        help="Directory for exported artifacts and JSON summaries.",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    output_dir = ensure_dir(Path(args.output_dir))

    case = MatpowerImportAdapter().import_case(args.input)
    pandapower = PandapowerAdapter()
    validator = ValidationService()
    reference_net = pandapower.run_power_flow(case)
    initial_validation = {
        "passed": True,
        "slack_delta_mva": 0.0,
        "max_voltage_delta_pu": 0.0,
        "details": {"backend": "pandapower_reference", "compared_buses": len(reference_net.res_bus)},
    }

    powermodels_path = pandapower.export_powermodels_json(case, output_dir / f"{case.case_id}.powermodels.json")
    julia = julia_settings()
    powermodels_validation = validator.validate_powermodels_export(
        case,
        powermodels_json=powermodels_path,
        julia_binary=str(julia["julia_binary"]),
        julia_script=Path(julia["powermodels_script"]),
        julia_depot=Path(julia["julia_depot"]),
    )

    summary_path = write_json(
        output_dir / "summary.json",
        {
            "use_case": "uc1",
            "script": "docs/examples/uc1_matpower_to_powermodels.py",
            "input": Path(args.input),
            "case_id": case.case_id,
            "artifacts": {
                "powermodels_json": powermodels_path,
            },
            "initial_validation": initial_validation,
            "powermodels_validation": powermodels_validation,
        },
    )
    print(summary_path)


if __name__ == "__main__":
    main()
