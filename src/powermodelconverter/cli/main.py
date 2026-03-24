from __future__ import annotations

import argparse
from dataclasses import asdict
import json
import math
from pathlib import Path

from powermodelconverter.adapters.matpower_adapter import MatpowerImportAdapter
from powermodelconverter.adapters.opendss_adapter import OpenDSSImportAdapter
from powermodelconverter.adapters.pandapower_adapter import PandapowerAdapter
from powermodelconverter.adapters.pandapower_import_adapter import PandapowerImportAdapter
from powermodelconverter.adapters.powermodels_distribution_adapter import PowerModelsDistributionAdapter
from powermodelconverter.adapters.pypsa_adapter import PypsaAdapter
from powermodelconverter.adapters.pypsa_import_adapter import PypsaImportAdapter
from powermodelconverter.adapters.simbench_adapter import SimbenchImportAdapter
from powermodelconverter.core.capabilities import capability_rows
from powermodelconverter.validation.powerflow import ValidationService


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Power model converter CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    caps = subparsers.add_parser("capabilities", help="Show supported balanced/unbalanced routes")
    caps.add_argument("--format", choices=["json"], default="json")

    validate = subparsers.add_parser("validate", help="Import, export, and validate a case")
    validate.add_argument(
        "--source-format",
        choices=["matpower", "opendss", "simbench", "pandapower", "pypsa"],
        required=True,
    )
    validate.add_argument("--source", required=True)
    validate.add_argument("--export-dir", default="src/powermodelconverter/data/exports")
    validate.add_argument(
        "--julia-binary",
        default=str(Path.home() / ".julia/juliaup/julia-1.12.3+0.x64.linux.gnu/bin/julia"),
    )
    validate.add_argument("--julia-script", default="src/powermodelconverter/julia/run_powermodels_pf.jl")
    validate.add_argument("--julia-depot", default=".julia_depot")
    validate.add_argument(
        "--julia-pmd-script",
        default="src/powermodelconverter/julia_pmd/run_powermodels_distribution_pf.jl",
    )
    validate.add_argument("--julia-pmd-project", default="src/powermodelconverter/julia_pmd")
    validate.add_argument("--pypsa-export", action="store_true", help="Export and validate a PyPSA .nc file for balanced cases")
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    if args.command == "capabilities":
        print(json.dumps(capability_rows(), indent=2))
        return

    pandapower = PandapowerAdapter()
    pypsa = PypsaAdapter()
    validator = ValidationService()

    if args.source_format == "matpower":
        case = MatpowerImportAdapter().import_case(args.source)
        reference = pandapower.run_power_flow(case)
        reference_slack_p = float(reference.res_ext_grid.p_mw.sum())
        reference_slack_q = float(reference.res_ext_grid.q_mvar.sum())
        reference_voltages = {
            validator._bus_key(reference, int(idx)): complex(
                float(row.vm_pu) * math.cos(math.radians(float(row.va_degree))),
                float(row.vm_pu) * math.sin(math.radians(float(row.va_degree))),
            )
            for idx, row in reference.res_bus.iterrows()
        }
        initial_validation = validator.validate_against_pandapower(
            case,
            reference_slack_p_mw=reference_slack_p,
            reference_slack_q_mvar=reference_slack_q,
            reference_voltages=reference_voltages,
        )
    elif args.source_format == "simbench":
        case = SimbenchImportAdapter().import_case(args.source)
        reference = pandapower.run_power_flow(case)
        reference_slack_p = float(reference.res_ext_grid.p_mw.sum())
        reference_slack_q = float(reference.res_ext_grid.q_mvar.sum())
        reference_voltages = {
            validator._bus_key(reference, int(idx)): complex(
                float(row.vm_pu) * math.cos(math.radians(float(row.va_degree))),
                float(row.vm_pu) * math.sin(math.radians(float(row.va_degree))),
            )
            for idx, row in reference.res_bus.iterrows()
        }
        initial_validation = validator.validate_against_pandapower(
            case,
            reference_slack_p_mw=reference_slack_p,
            reference_slack_q_mvar=reference_slack_q,
            reference_voltages=reference_voltages,
        )
    elif args.source_format == "pandapower":
        case = PandapowerImportAdapter().import_case(args.source)
        if case.is_unbalanced:
            initial_validation = validator.validate_pandapower_unbalanced_roundtrip(case)
        else:
            reference = pandapower.run_power_flow(case)
            reference_slack_p = float(reference.res_ext_grid.p_mw.sum())
            reference_slack_q = float(reference.res_ext_grid.q_mvar.sum())
            reference_voltages = {
                validator._bus_key(reference, int(idx)): complex(
                    float(row.vm_pu) * math.cos(math.radians(float(row.va_degree))),
                    float(row.vm_pu) * math.sin(math.radians(float(row.va_degree))),
                )
                for idx, row in reference.res_bus.iterrows()
            }
            initial_validation = validator.validate_against_pandapower(
                case,
                reference_slack_p_mw=reference_slack_p,
                reference_slack_q_mvar=reference_slack_q,
                reference_voltages=reference_voltages,
            )
    elif args.source_format == "pypsa":
        source_reference = pypsa.solve_source_case(args.source)
        case = PypsaImportAdapter().import_case(args.source)
        initial_validation = validator.validate_against_pandapower(
            case,
            reference_slack_p_mw=source_reference.slack_p_mw,
            reference_slack_q_mvar=source_reference.slack_q_mvar,
            reference_voltages=source_reference.voltages,
        )
    else:
        adapter = OpenDSSImportAdapter()
        source_reference = adapter.solve_source_case(args.source)
        case = adapter.import_case(args.source)
        if case.is_unbalanced:
            initial_validation = validator.validate_opendss_unbalanced_roundtrip(case, source_reference)
        else:
            initial_validation = validator.validate_opendss_roundtrip(case, source_reference)

    export_dir = Path(args.export_dir)
    pandapower_path = pandapower.export_json(case, export_dir / f"{case.case_id}.pandapower.json")
    powermodels_path = None
    if not case.is_unbalanced:
        powermodels_path = pandapower.export_powermodels_json(
            case,
            export_dir / f"{case.case_id}.powermodels.json",
        )
    powermodelsdistribution_path = None
    if case.is_unbalanced:
        try:
            powermodelsdistribution_path = PowerModelsDistributionAdapter().export_input(
                case,
                export_dir / f"{case.case_id}_powermodelsdistribution.dss",
            )
        except ValueError:
            powermodelsdistribution_path = None
    pypsa_path = None
    if not case.is_unbalanced and args.pypsa_export:
        pypsa_path = pypsa.export_netcdf(case, export_dir / f"{case.case_id}.pypsa.nc")
    powermodels_validation = None
    if not case.is_unbalanced:
        powermodels_validation = validator.validate_powermodels_export(
            case,
            powermodels_json=powermodels_path,
            julia_binary=args.julia_binary,
            julia_script=Path(args.julia_script),
            julia_depot=Path(args.julia_depot),
        )
    powermodelsdistribution_validation = None
    if case.is_unbalanced and powermodelsdistribution_path is not None:
        if args.source_format == "opendss":
            reference_slack_p_mw = source_reference.slack_p_mw
            reference_slack_q_mvar = source_reference.slack_q_mvar
            reference_node_voltages = source_reference.node_voltages
        else:
            reference_net = pandapower.run_power_flow_3ph(case)
            slack = validator._extract_3ph_slack(reference_net)
            reference_slack_p_mw = sum(slack[key] for key in ("p_a_mw", "p_b_mw", "p_c_mw"))
            reference_slack_q_mvar = sum(slack[key] for key in ("q_a_mvar", "q_b_mvar", "q_c_mvar"))
            reference_node_voltages = validator._extract_3ph_node_voltages(reference_net)
        powermodelsdistribution_validation = validator.validate_powermodelsdistribution_export(
            case,
            pmd_input_path=powermodelsdistribution_path,
            julia_binary=args.julia_binary,
            julia_script=Path(args.julia_pmd_script),
            julia_depot=Path(args.julia_depot),
            julia_project=Path(args.julia_pmd_project),
            reference_slack_p_mw=reference_slack_p_mw,
            reference_slack_q_mvar=reference_slack_q_mvar,
            reference_node_voltages=reference_node_voltages,
        )
    pypsa_validation = None
    if pypsa_path is not None:
        pypsa_validation = validator.validate_pypsa_export(case, pypsa_path=pypsa_path)

    print(
        json.dumps(
            {
                "case_id": case.case_id,
                "source_format": case.source_format,
                "is_unbalanced": case.is_unbalanced,
                "pandapower_export": str(pandapower_path),
                "powermodels_export": str(powermodels_path) if powermodels_path else None,
                "pypsa_export": str(pypsa_path) if pypsa_path else None,
                "powermodelsdistribution_export": (
                    str(powermodelsdistribution_path) if powermodelsdistribution_path else None
                ),
                "initial_validation": asdict(initial_validation),
                "powermodels_validation": asdict(powermodels_validation) if powermodels_validation else None,
                "pypsa_validation": asdict(pypsa_validation) if pypsa_validation else None,
                "powermodelsdistribution_validation": (
                    asdict(powermodelsdistribution_validation) if powermodelsdistribution_validation else None
                ),
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
