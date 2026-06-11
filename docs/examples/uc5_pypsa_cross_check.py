#!/usr/bin/env python3
from __future__ import annotations

import argparse
import tempfile
from pathlib import Path

import pypsa

from _common import default_output_dir
from _common import ensure_dir
from _common import julia_settings
from _common import write_json

from powermodelconverter.core.pandapower_backend import PandapowerAdapter
from powermodelconverter.importers.pypsa import PypsaAdapter
from powermodelconverter.validation.engine import ValidationService
from powermodelconverter.validation.runners import run_julia_powermodels
from powermodelconverter.validation.runners import run_pypsa_pf


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Working hub-and-spoke example: PyPSA-origin cross-check against pandapower and PowerModels."
    )
    parser.add_argument(
        "--output-dir",
        default=str(default_output_dir("uc5")),
        help="Directory for exported artifacts and JSON summaries.",
    )
    return parser


def build_network() -> pypsa.Network:
    network = pypsa.Network()
    network.set_snapshots(["now"])
    network.add("Bus", "B1", v_nom=110.0)
    network.add("Bus", "B2", v_nom=110.0)
    network.add("Bus", "B3", v_nom=110.0)
    network.add("Line", "L12", bus0="B1", bus1="B2", x=0.08, r=0.01, b=0.0, g=0.0, s_nom=100.0, length=1.0)
    network.add("Line", "L23", bus0="B2", bus1="B3", x=0.06, r=0.01, b=0.0, g=0.0, s_nom=100.0, length=1.0)
    network.add("Generator", "Slack", bus="B1", control="Slack", p_set=0.0, q_set=0.0, vm_pu=1.0)
    network.add("Load", "Load2", bus="B2", p_set=35.0, q_set=8.0)
    network.add("Load", "Load3", bus="B3", p_set=20.0, q_set=5.0)
    return network


def main() -> None:
    args = build_parser().parse_args()
    output_dir = ensure_dir(Path(args.output_dir))

    network = build_network()
    with tempfile.TemporaryDirectory(prefix="pmc_uc5_") as tmp:
        pypsa_path = Path(tmp) / "synthetic.pypsa.nc"
        network.export_to_netcdf(pypsa_path)

        pypsa_adapter = PypsaAdapter()
        case = pypsa_adapter.import_case(pypsa_path)
        pandapower = PandapowerAdapter()
        validator = ValidationService()

        pandapower_path = pandapower.export_json(case, output_dir / "synthetic.pandapower.json")
        powermodels_path = pandapower.export_powermodels_json(case, output_dir / "synthetic.powermodels.json")
        pypsa_export_path = pypsa_adapter.export_netcdf(case, output_dir / "synthetic.pypsa.nc")

        pypsa_result = run_pypsa_pf(pypsa_export_path)
        pandapower_validation = validator.validate_pypsa_export(case, pypsa_path=pypsa_export_path)

        julia = julia_settings()
        powermodels_payload = run_julia_powermodels(
            powermodels_path,
            julia_binary=str(julia["julia_binary"]),
            julia_script=Path(julia["powermodels_script"]),
            julia_depot=Path(julia["julia_depot"]),
        )
        powermodels_validation = validator.validate_powermodels_export(
            case,
            powermodels_json=powermodels_path,
            julia_binary=str(julia["julia_binary"]),
            julia_script=Path(julia["powermodels_script"]),
            julia_depot=Path(julia["julia_depot"]),
        )

    summary_path = write_json(
        output_dir / "crosscheck.json",
        {
            "use_case": "uc5",
            "script": "docs/examples/uc5_pypsa_cross_check.py",
            "artifacts": {
                "pandapower_json": pandapower_path,
                "powermodels_json": powermodels_path,
                "pypsa_netcdf": pypsa_export_path,
            },
            "crosscheck": {
                "pandapower_against_pypsa": pandapower_validation,
                "powermodels_against_pandapower": powermodels_validation,
                "pypsa_slack_mw": pypsa_result.slack_p_mw,
                "powermodels_slack_mw": powermodels_payload["slack_p_mw"],
            },
        },
    )
    print(summary_path)


if __name__ == "__main__":
    main()
