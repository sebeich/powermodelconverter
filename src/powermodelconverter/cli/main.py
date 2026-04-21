from __future__ import annotations

import argparse
from dataclasses import asdict
import importlib.util
import json
import math
from pathlib import Path
import re
import sys
from tempfile import TemporaryDirectory
from datetime import datetime, timezone
from typing import Any

from powermodelconverter.exporters.cgmes import CGMESExportAdapter
from powermodelconverter.exporters.matpower import MatpowerExportAdapter
from powermodelconverter.importers.cgmes import CGMESImportAdapter
from powermodelconverter.importers.matpower import MatpowerImportAdapter
from powermodelconverter.exporters.opendss import OpenDSSExportAdapter
from powermodelconverter.importers.opendss import OpenDSSImportAdapter
from powermodelconverter.core.pandapower_backend import PandapowerAdapter
from powermodelconverter.importers.pandapower_json import PandapowerImportAdapter
from powermodelconverter.exporters.pandapower_split import PandapowerSplitExportAdapter
from powermodelconverter.exporters.powermodels_distribution import PowerModelsDistributionAdapter
from powermodelconverter.importers.powermodels_distribution import PowerModelsDistributionImportAdapter
from powermodelconverter.exporters.powersystems import PowerSystemsExportAdapter
from powermodelconverter.importers.powersystems import PowerSystemsImportAdapter
from powermodelconverter.importers.pypower import PypowerImportAdapter
from powermodelconverter.importers.pypsa import PypsaAdapter
from powermodelconverter.importers.pypsa import PypsaImportAdapter
from powermodelconverter.importers.simbench import SimbenchImportAdapter
from powermodelconverter.core.capabilities import capability_rows
from powermodelconverter.core.exceptions import ConversionError
from powermodelconverter.core.model import CanonicalCase
from powermodelconverter.core.registry import Route, ensure_routes_loaded, get_routes
from powermodelconverter.report import generate_full_report, list_report_records, merge_partial_results
from powermodelconverter.runtime import resolve_julia_binary
from powermodelconverter.validation.powerflow import ValidationResult, ValidationService

REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_EXPORT_DIR = REPO_ROOT / "src" / "powermodelconverter" / "data" / "exports"
DEFAULT_REPORT_PATH = REPO_ROOT / "docs" / "validation_report.json"
DEFAULT_JULIA_SCRIPT = REPO_ROOT / "src" / "powermodelconverter" / "julia" / "run_powermodels_pf.jl"
DEFAULT_JULIA_DEPOT = REPO_ROOT / ".julia_depot"
DEFAULT_JULIA_PSI_SCRIPT = REPO_ROOT / "src" / "powermodelconverter" / "julia_psi" / "run_powersimulations_pf.jl"
DEFAULT_JULIA_PSI_PROJECT = REPO_ROOT / "src" / "powermodelconverter" / "julia_psi"
DEFAULT_JULIA_PMD_SCRIPT = REPO_ROOT / "src" / "powermodelconverter" / "julia_pmd" / "run_powermodels_distribution_pf.jl"
DEFAULT_JULIA_PMD_PROJECT = REPO_ROOT / "src" / "powermodelconverter" / "julia_pmd"
LEGACY_REPORT_SCRIPT = REPO_ROOT / "scripts" / "generate_validation_report.py"


SOURCE_FORMATS = [
    "matpower",
    "powersystems",
    "pypower",
    "opendss",
    "simbench",
    "pandapower",
    "pypsa",
    "cgmes",
    "powermodelsdistribution",
]
TARGET_FORMATS = [
    "pandapower",
    "pandapower_split",
    "powermodels",
    "powersystems",
    "powermodelsdistribution",
    "pypsa",
    "opendss",
    "cgmes",
]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Power model converter CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    caps = subparsers.add_parser("capabilities", help="Show supported balanced/unbalanced routes")
    caps.add_argument("--format", choices=["json"], default="json")

    validate_route = subparsers.add_parser("validate-route", help="Validate one registered route/case pair")
    validate_route.add_argument("--source", required=True)
    validate_route.add_argument("--target", required=True)
    validate_route.add_argument("--case", required=True)
    validate_route.add_argument("--output", required=True)
    add_runtime_arguments(validate_route)

    validate_batch = subparsers.add_parser("validate-batch", help="Validate a filtered set of registered routes")
    validate_batch.add_argument("--routes-filter", default="")
    validate_batch.add_argument("--output", required=True)
    validate_batch.add_argument("--worker-id", default="")
    add_runtime_arguments(validate_batch)

    report = subparsers.add_parser("report", help="Generate, inspect, or merge validation reports")
    report.add_argument("action", nargs="?", choices=["merge"])
    report.add_argument("--full", action="store_true")
    report.add_argument("--dry-run", action="store_true")
    report.add_argument("--input-dir")
    report.add_argument("--existing", default=str(DEFAULT_REPORT_PATH))
    report.add_argument("--output", default=str(DEFAULT_REPORT_PATH))

    validate = subparsers.add_parser("validate", help="Import, export, and validate all currently supported routes")
    add_source_arguments(validate)
    add_runtime_arguments(validate)
    validate.add_argument("--export-dir", default=str(DEFAULT_EXPORT_DIR))
    validate.add_argument("--pypsa-export", action="store_true", help="Export and validate a PyPSA .nc file for balanced cases")

    precheck = subparsers.add_parser("precheck", help="Check whether a source case is supported for one target route")
    add_source_arguments(precheck)
    precheck.add_argument("--target-format", choices=TARGET_FORMATS, required=True)
    add_runtime_arguments(precheck)

    translate = subparsers.add_parser(
        "translate",
        help="Convert a source case to one chosen target and validate that route",
    )
    add_source_arguments(translate)
    translate.add_argument("--target-format", choices=TARGET_FORMATS, required=True)
    translate.add_argument("--output", help="Optional explicit output path for the chosen target artifact")
    translate.add_argument("--export-dir", default=str(DEFAULT_EXPORT_DIR))
    translate.add_argument(
        "--skip-source-validation",
        action="store_true",
        help="Skip re-running the source-side validation before export",
    )
    add_runtime_arguments(translate)
    return parser


def add_source_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--source-format", choices=SOURCE_FORMATS)
    parser.add_argument("--source", required=True)


def add_runtime_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--julia-binary",
        default=resolve_julia_binary(),
    )
    parser.add_argument("--julia-script", default=str(DEFAULT_JULIA_SCRIPT))
    parser.add_argument("--julia-depot", default=str(DEFAULT_JULIA_DEPOT))
    parser.add_argument("--julia-psi-script", default=str(DEFAULT_JULIA_PSI_SCRIPT))
    parser.add_argument("--julia-psi-project", default=str(DEFAULT_JULIA_PSI_PROJECT))
    parser.add_argument("--julia-pmd-script", default=str(DEFAULT_JULIA_PMD_SCRIPT))
    parser.add_argument("--julia-pmd-project", default=str(DEFAULT_JULIA_PMD_PROJECT))


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    if args.command == "capabilities":
        print(json.dumps(capability_rows(), indent=2))
        return

    if args.command == "report":
        result = run_report_command(args)
        print(json.dumps(result, indent=2))
        return

    pandapower = PandapowerAdapter()
    pypsa = PypsaAdapter()
    cgmes = CGMESExportAdapter()
    validator = ValidationService()

    if args.command == "validate-route":
        result = run_validate_route_command(args, pandapower, pypsa, cgmes, validator)
    elif args.command == "validate-batch":
        result = run_validate_batch_command(args, pandapower, pypsa, cgmes, validator)
    elif args.command == "validate":
        result = run_validate_command(args, pandapower, pypsa, cgmes, validator)
    elif args.command == "precheck":
        loaded = load_case_with_reference(args, pandapower, pypsa, validator)
        result = build_precheck_result(
            loaded["case"],
            args.target_format,
            pandapower,
            pypsa,
            cgmes,
            validator,
            args,
            source_validation=loaded["initial_validation"],
        )
    elif args.command == "translate":
        loaded = load_case_with_reference(args, pandapower, pypsa, validator)
        precheck = build_precheck_result(
            loaded["case"],
            args.target_format,
            pandapower,
            pypsa,
            cgmes,
            validator,
            args,
            source_validation=None if args.skip_source_validation else loaded["initial_validation"],
        )
        if not precheck["supported"]:
            raise ValueError(
                f"Route {loaded['case'].source_format} -> {args.target_format} is not currently supported: "
                + "; ".join(precheck["issues"])
            )
        target_output = determine_output_path(loaded["case"], args.target_format, args.output, args.export_dir)
        exported_path, target_validation = export_and_validate_target(
            loaded["case"],
            args.target_format,
            target_output,
            loaded["source_reference"],
            pandapower,
            pypsa,
            cgmes,
            validator,
            args,
        )
        result = {
            "case_id": loaded["case"].case_id,
            "source_format": loaded["case"].source_format,
            "target_format": args.target_format,
            "is_unbalanced": loaded["case"].is_unbalanced,
            "precheck": precheck,
            "source_validation": None if args.skip_source_validation else asdict(loaded["initial_validation"]),
            "target_export": str(exported_path),
            "target_validation": asdict(target_validation) if target_validation else None,
        }
    else:
        raise ValueError(f"Unsupported command {args.command}")

    print(json.dumps(result, indent=2))


def run_report_command(args: argparse.Namespace) -> dict[str, Any]:
    ensure_routes_loaded()
    if args.action == "merge":
        payload = merge_partial_results(
            input_dir=args.input_dir or ".",
            output_path=args.output,
            existing_report=args.existing,
        )
        return {
            "mode": "merge",
            "output": str(args.output),
            "records": len(payload.get("records", [])),
        }
    if args.full:
        payload = generate_full_report()
        return {
            "mode": "full",
            "output": "docs/validation_report.json",
            "records": len(payload.get("records", [])),
            "summary": payload.get("summary", {}),
        }
    if args.dry_run:
        records = list_report_records(args.existing)
        index = {
            (str(record.get("source_tool", "")), str(record.get("export_tool", "")), str(record.get("case_id", ""))): record
            for record in records
        }
        route_rows = []
        for route in get_routes():
            case_id = route.test_cases[0] if route.test_cases else ""
            existing = index.get((route.source_tool, route.target_tool, case_id))
            staleness = describe_route_staleness(route, existing)
            route_rows.append(
                {
                    "route_id": route.route_id,
                    "source_tool": route.source_tool,
                    "target_tool": route.target_tool,
                    "case_id": case_id,
                    "model_type": route.model_type,
                    "requires": route.requires,
                    "result_present": existing is not None,
                    "status": None if existing is None else existing.get("status"),
                    "timestamp": None if existing is None else existing.get("timestamp"),
                    "stale": staleness["stale"],
                    "staleness_reason": staleness["reason"],
                }
            )
        return {"mode": "dry-run", "routes": route_rows, "count": len(route_rows)}
    raise ValueError("Use `report --full`, `report --dry-run`, or `report merge ...`.")


def run_validate_route_command(
    args: argparse.Namespace,
    pandapower: PandapowerAdapter,
    pypsa: PypsaAdapter,
    cgmes: CGMESExportAdapter,
    validator: ValidationService,
) -> dict[str, Any]:
    route = find_registered_route(args.source, args.target, args.case)
    loaded = load_registered_case_with_reference(route, pandapower, pypsa, cgmes, validator)
    exported_path, target_validation = export_and_validate_target(
        loaded["case"],
        route.target_tool,
        Path(args.output),
        loaded["source_reference"],
        pandapower,
        pypsa,
        cgmes,
        validator,
        args,
    )
    record = validation_result_to_record(
        case_id=args.case,
        source_tool=route.source_tool,
        export_tool=route.target_tool,
        model_type=route.model_type,
        notes=route.notes,
        result=target_validation,
        worker_id="",
    )
    record["output"] = str(exported_path)
    return record


def run_validate_batch_command(
    args: argparse.Namespace,
    pandapower: PandapowerAdapter,
    pypsa: PypsaAdapter,
    cgmes: CGMESExportAdapter,
    validator: ValidationService,
) -> dict[str, Any]:
    routes = filter_registered_routes(args.routes_filter)
    records: list[dict[str, Any]] = []
    for route in routes:
        case_id = route.test_cases[0] if route.test_cases else ""
        try:
            loaded = load_registered_case_with_reference(route, pandapower, pypsa, cgmes, validator)
            output_path = Path(args.output).parent / f"{route.source_tool}_{route.target_tool}_{sanitize_case_id(case_id)}.artifact"
            _, target_validation = export_and_validate_target(
                loaded["case"],
                route.target_tool,
                output_path,
                loaded["source_reference"],
                pandapower,
                pypsa,
                cgmes,
                validator,
                args,
            )
            records.append(
                validation_result_to_record(
                    case_id=case_id,
                    source_tool=route.source_tool,
                    export_tool=route.target_tool,
                    model_type=route.model_type,
                    notes=route.notes,
                    result=target_validation,
                    worker_id=args.worker_id,
                )
            )
        except Exception as exc:
            records.append(
                {
                    "case_id": case_id,
                    "source_tool": route.source_tool,
                    "export_tool": route.target_tool,
                    "model_type": route.model_type,
                    "status": "error",
                    "slack_delta_mva": None,
                    "max_voltage_delta_pu": None,
                    "compared_points": 0,
                    "notes": f"{route.notes} Error: {exc}".strip(),
                    "timestamp": ValidationResult(
                        case_id=case_id,
                        passed=False,
                        slack_delta_mva=0.0,
                        max_voltage_delta_pu=0.0,
                    ).timestamp,
                    "duration_seconds": 0.0,
                    "worker_id": args.worker_id,
                }
            )
    output_file = Path(args.output)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    payload = {"records": records}
    output_file.write_text(json.dumps(payload, indent=2))
    return {"output": str(output_file), "records": len(records)}


def load_registered_case_with_reference(
    route: Route,
    pandapower: PandapowerAdapter,
    pypsa: PypsaAdapter,
    cgmes: CGMESExportAdapter,
    validator: ValidationService,
) -> dict[str, Any]:
    source_tool = route.source_tool
    case_id = route.test_cases[0] if route.test_cases else ""
    normalized_source_tool = {
        "pm.jl": "matpower",
        "powersystems.jl": "matpower",
    }.get(source_tool, source_tool)

    try:
        source_ref = resolve_registered_case_source(route)
        return load_case_with_reference(
            argparse.Namespace(source=str(source_ref), source_format=normalized_source_tool),
            pandapower,
            pypsa,
            validator,
        )
    except ValueError:
        pass

    legacy = load_legacy_report_module()

    def balanced_reference(case: CanonicalCase) -> dict[str, Any]:
        case.source_path = None
        reference = pandapower.run_power_flow(case)
        return {
            "case": case,
            "initial_validation": validate_against_pandapower_reference(case, reference, validator),
            "source_reference": {"backend": "pandapower", "reference_net": reference},
        }

    def unbalanced_reference(case: CanonicalCase) -> dict[str, Any]:
        case.source_path = None
        reference = pandapower.run_power_flow_3ph(case)
        return {
            "case": case,
            "initial_validation": validator.validate_pandapower_unbalanced_roundtrip(case),
            "source_reference": {"backend": "pandapower_3ph", "reference_net": reference},
        }

    def find_case(cases: list[CanonicalCase], target_case_id: str) -> CanonicalCase:
        for candidate in cases:
            if candidate.case_id == target_case_id:
                return candidate
        raise ValueError(f"Legacy case builder could not find case {target_case_id}.")

    with TemporaryDirectory(prefix="pmc_registered_case_") as tmp:
        tmpdir = Path(tmp)

        if case_id.startswith("core_bal_"):
            base_case_id = case_id.removesuffix(".cgmes").removesuffix(".pandapower")
            source_case = find_case(legacy.build_balanced_core_cases(), base_case_id)
            materialized = legacy.materialize_balanced_source_case(
                source_case,
                source_tool=source_tool,
                pandapower=pandapower,
                pypsa=pypsa,
                cgmes_export=cgmes,
                tmpdir=tmpdir,
            )
            return balanced_reference(materialized)

        if case_id.startswith("core_unb_"):
            base_case_id = case_id.removesuffix(".pandapower").removesuffix("_pmd")
            source_case = find_case(legacy.build_unbalanced_core_cases(), base_case_id)
            materialized = legacy.materialize_unbalanced_source_case(
                source_case,
                source_tool=source_tool,
                tmpdir=tmpdir,
            )
            return unbalanced_reference(materialized)

        if source_tool == "pandapower":
            if case_id == "cgmes_smoke":
                return balanced_reference(legacy.build_cgmes_subset_case())
            if case_id in {"case4gs", "case5", "case6ww", "case33bw"}:
                case = CanonicalCase.from_pandapower(
                    case_id=case_id,
                    source_format="pandapower",
                    net=getattr(legacy.pn, case_id)(),
                )
                return balanced_reference(case)
            if case_id == "case9_from_matpower.pandapower":
                case9 = MatpowerImportAdapter().import_case(REPO_ROOT / "src/powermodelconverter/data/samples/matpower/case9.m")
                path = pandapower.export_json(case9, tmpdir / f"{case_id}.json")
                return balanced_reference(PandapowerImportAdapter().import_case(path))
            if case_id == "minimal_radial.pandapower":
                imported = OpenDSSImportAdapter().import_case(REPO_ROOT / "src/powermodelconverter/data/samples/opendss/minimal_radial.dss")
                path = pandapower.export_json(imported, tmpdir / f"{case_id}.json")
                return balanced_reference(PandapowerImportAdapter().import_case(path))

        if source_tool == "pypsa":
            if case_id in {"pypsa_triangle_native", "pypsa_radial_native", "pypsa_five_bus_ring_native"}:
                builders = {
                    "pypsa_triangle_native": legacy.build_pypsa_triangle_network,
                    "pypsa_radial_native": legacy.build_pypsa_radial_network,
                    "pypsa_five_bus_ring_native": legacy.build_pypsa_five_bus_ring_network,
                }
                case, snapshot = legacy.build_pypsa_origin_case(case_id, builders[case_id], pypsa, tmpdir)
                initial_validation = validator.validate_against_pandapower(
                    case,
                    reference_slack_p_mw=snapshot.slack_p_mw,
                    reference_slack_q_mvar=snapshot.slack_q_mvar,
                    reference_voltages=snapshot.voltages,
                )
                case.source_path = None
                return {
                    "case": case,
                    "initial_validation": initial_validation,
                    "source_reference": {"backend": "pypsa", "snapshot": snapshot},
                }
            if case_id == "case9_from_matpower.pandapower.pypsa":
                case9 = MatpowerImportAdapter().import_case(REPO_ROOT / "src/powermodelconverter/data/samples/matpower/case9.m")
                pp_path = pandapower.export_json(case9, tmpdir / "case9_from_matpower.pandapower.json")
                pp_case = PandapowerImportAdapter().import_case(pp_path)
                pypsa_path = pypsa.export_netcdf(pp_case, tmpdir / f"{case_id}.nc")
                snapshot = pypsa.solve_source_case(pypsa_path)
                case = PypsaImportAdapter().import_case(pypsa_path)
                initial_validation = validator.validate_against_pandapower(
                    case,
                    reference_slack_p_mw=snapshot.slack_p_mw,
                    reference_slack_q_mvar=snapshot.slack_q_mvar,
                    reference_voltages=snapshot.voltages,
                )
                case.source_path = None
                return {
                    "case": case,
                    "initial_validation": initial_validation,
                    "source_reference": {"backend": "pypsa", "snapshot": snapshot},
                }

        if source_tool in {"pm.jl", "powersystems.jl"}:
            pmjl_native = legacy.find_pm_jl_native_case()
            if pmjl_native is None:
                raise ValueError("Could not locate a PowerModels.jl package-native MATPOWER case.")
            _, source = pmjl_native
            case = MatpowerImportAdapter().import_case(source)
            return balanced_reference(case)

        raise ValueError(f"No registered-case materializer is defined for route {route.route_id}.")


def load_legacy_report_module() -> Any:
    spec = importlib.util.spec_from_file_location("pmc_legacy_generate_validation_report", LEGACY_REPORT_SCRIPT)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load legacy report generator at {LEGACY_REPORT_SCRIPT}.")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def run_validate_command(
    args: argparse.Namespace,
    pandapower: PandapowerAdapter,
    pypsa: PypsaAdapter,
    cgmes: CGMESExportAdapter,
    validator: ValidationService,
) -> dict[str, Any]:
    loaded = load_case_with_reference(args, pandapower, pypsa, validator)
    case = loaded["case"]
    source_reference = loaded["source_reference"]
    initial_validation = loaded["initial_validation"]

    export_dir = Path(args.export_dir)
    pandapower_path = pandapower.export_json(case, export_dir / f"{case.case_id}.pandapower.json")
    opendss_path = None
    opendss_validation = None
    if args.source_format == "pandapower":
        try:
            opendss_path, opendss_validation = export_and_validate_target(
                case,
                "opendss",
                export_dir / f"{case.case_id}.dss",
                source_reference,
                pandapower,
                pypsa,
                cgmes,
                validator,
                args,
            )
        except ValueError:
            opendss_path = None
            opendss_validation = None
    powermodels_path = None
    if not case.is_unbalanced:
        powermodels_path = pandapower.export_powermodels_json(case, export_dir / f"{case.case_id}.powermodels.json")
    powersystems_path = None
    if not case.is_unbalanced:
        powersystems_path = PowerSystemsExportAdapter().export_case(case, export_dir / f"{case.case_id}.powersystems.m")
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
    cgmes_path = None
    cgmes_validation = None
    if not case.is_unbalanced and args.source_format == "pandapower":
        try:
            cgmes_path, cgmes_validation = export_and_validate_target(
                case,
                "cgmes",
                export_dir / f"{case.case_id}.cgmes.zip",
                source_reference,
                pandapower,
                pypsa,
                cgmes,
                validator,
                args,
            )
        except ValueError:
            cgmes_path = None
            cgmes_validation = None

    powermodels_validation = None
    if powermodels_path is not None:
        powermodels_validation = validator.validate_powermodels_export(
            case,
            powermodels_json=powermodels_path,
            julia_binary=args.julia_binary,
            julia_script=Path(args.julia_script),
            julia_depot=Path(args.julia_depot),
        )
    powermodelsdistribution_validation = None
    if powermodelsdistribution_path is not None:
        powermodelsdistribution_validation = validate_powermodelsdistribution_target(
            case,
            powermodelsdistribution_path,
            source_reference,
            validator,
            pandapower,
            args,
        )
    powersystems_validation = None
    if powersystems_path is not None:
        powersystems_validation = validator.validate_powersystems_export(
            case,
            powersystems_case=powersystems_path,
            julia_binary=args.julia_binary,
            julia_script=Path(args.julia_psi_script),
            julia_depot=Path(args.julia_depot),
            julia_project=Path(args.julia_psi_project),
        )
    pypsa_validation = None
    if pypsa_path is not None:
        pypsa_validation = validator.validate_pypsa_export(case, pypsa_path=pypsa_path)

    return {
        "case_id": case.case_id,
        "source_format": case.source_format,
        "is_unbalanced": case.is_unbalanced,
        "pandapower_export": str(pandapower_path),
        "opendss_export": str(opendss_path) if opendss_path else None,
        "powermodels_export": str(powermodels_path) if powermodels_path else None,
        "powersystems_export": str(powersystems_path) if powersystems_path else None,
        "pypsa_export": str(pypsa_path) if pypsa_path else None,
        "cgmes_export": str(cgmes_path) if cgmes_path else None,
        "powermodelsdistribution_export": str(powermodelsdistribution_path) if powermodelsdistribution_path else None,
        "initial_validation": asdict(initial_validation),
        "opendss_validation": asdict(opendss_validation) if opendss_validation else None,
        "powermodels_validation": asdict(powermodels_validation) if powermodels_validation else None,
        "powersystems_validation": asdict(powersystems_validation) if powersystems_validation else None,
        "pypsa_validation": asdict(pypsa_validation) if pypsa_validation else None,
        "cgmes_validation": asdict(cgmes_validation) if cgmes_validation else None,
        "powermodelsdistribution_validation": (
            asdict(powermodelsdistribution_validation) if powermodelsdistribution_validation else None
        ),
    }


def load_case_with_reference(
    args: argparse.Namespace,
    pandapower: PandapowerAdapter,
    pypsa: PypsaAdapter,
    validator: ValidationService,
) -> dict[str, Any]:
    source_format = detect_source_format(args.source, args.source_format)
    if source_format == "matpower":
        case = MatpowerImportAdapter().import_case(args.source)
        reference = pandapower.run_power_flow(case)
        initial_validation = validate_against_pandapower_reference(case, reference, validator)
        source_reference = {
            "backend": "pandapower",
            "reference_net": reference,
        }
    elif source_format == "simbench":
        case = SimbenchImportAdapter().import_case(args.source)
        reference = pandapower.run_power_flow(case)
        initial_validation = validate_against_pandapower_reference(case, reference, validator)
        source_reference = {
            "backend": "pandapower",
            "reference_net": reference,
        }
    elif source_format == "pypower":
        adapter = PypowerImportAdapter()
        source_reference_snapshot = adapter.solve_source_case(args.source)
        case = adapter.import_case(args.source)
        initial_validation = validator.validate_against_pandapower(
            case,
            reference_slack_p_mw=source_reference_snapshot.slack_p_mw,
            reference_slack_q_mvar=source_reference_snapshot.slack_q_mvar,
            reference_voltages=source_reference_snapshot.voltages,
        )
        source_reference = {
            "backend": "pypower",
            "snapshot": source_reference_snapshot,
        }
    elif source_format == "pandapower":
        case = PandapowerImportAdapter().import_case(args.source)
        if case.is_unbalanced:
            initial_validation = validator.validate_pandapower_unbalanced_roundtrip(case)
            source_reference = {
                "backend": "pandapower_3ph",
                "reference_net": pandapower.run_power_flow_3ph(case),
            }
        else:
            reference = pandapower.run_power_flow(case)
            initial_validation = validate_against_pandapower_reference(case, reference, validator)
            source_reference = {
                "backend": "pandapower",
                "reference_net": reference,
            }
    elif source_format == "pypsa":
        source_reference_snapshot = pypsa.solve_source_case(args.source)
        case = PypsaImportAdapter().import_case(args.source)
        initial_validation = validator.validate_against_pandapower(
            case,
            reference_slack_p_mw=source_reference_snapshot.slack_p_mw,
            reference_slack_q_mvar=source_reference_snapshot.slack_q_mvar,
            reference_voltages=source_reference_snapshot.voltages,
        )
        source_reference = {
            "backend": "pypsa",
            "snapshot": source_reference_snapshot,
        }
    elif source_format == "cgmes":
        case = CGMESImportAdapter().import_case(args.source)
        reference = pandapower.run_power_flow(case)
        initial_validation = validate_against_pandapower_reference(case, reference, validator)
        source_reference = {
            "backend": "pandapower",
            "reference_net": reference,
        }
    elif source_format == "powersystems":
        case = PowerSystemsImportAdapter().import_case(args.source)
        reference = pandapower.run_power_flow(case)
        initial_validation = validate_against_pandapower_reference(case, reference, validator)
        source_reference = {
            "backend": "pandapower",
            "reference_net": reference,
        }
    elif source_format == "powermodelsdistribution":
        adapter = PowerModelsDistributionImportAdapter()
        source_reference_snapshot = adapter.solve_source_case(args.source)
        case = adapter.import_case(args.source)
        initial_validation = safe_validation(
            lambda: validator.validate_opendss_unbalanced_roundtrip(case, source_reference_snapshot),
            source_format,
            args.source,
        )
        source_reference = {
            "backend": "opendss_3ph",
            "snapshot": source_reference_snapshot,
        }
    else:
        adapter = OpenDSSImportAdapter()
        source_reference_snapshot = adapter.solve_source_case(args.source)
        case = adapter.import_case(args.source)
        if case.is_unbalanced:
            initial_validation = safe_validation(
                lambda: validator.validate_opendss_unbalanced_roundtrip(case, source_reference_snapshot),
                source_format,
                args.source,
            )
            source_reference = {
                "backend": "opendss_3ph",
                "snapshot": source_reference_snapshot,
            }
        else:
            initial_validation = safe_validation(
                lambda: validator.validate_opendss_roundtrip(case, source_reference_snapshot),
                source_format,
                args.source,
            )
            source_reference = {
                "backend": "opendss",
                "snapshot": source_reference_snapshot,
            }
    return {
        "case": case,
        "initial_validation": initial_validation,
        "source_reference": source_reference,
    }


def detect_source_format(source: str, requested_format: str | None) -> str:
    if requested_format:
        return requested_format
    source_text = str(source)
    if "::" in source_text:
        source_text = source_text.rsplit("::", 1)[0]
    source_path = Path(source_text)
    if source_path.is_dir():
        if any(path.suffix.lower() in {".zip", ".xml"} for path in source_path.iterdir()):
            return "cgmes"
        raise ValueError(
            "Could not auto-detect the source format for the directory. "
            "Use --source-format explicitly for non-CGMES directory inputs."
        )
    suffixes = [suffix.lower() for suffix in source_path.suffixes]
    if suffixes and suffixes[-1] == ".m":
        return "matpower"
    if suffixes and suffixes[-1] == ".py":
        text = source_path.read_text()
        if "pypower" in text.lower():
            return "pypower"
    if suffixes and suffixes[-1] == ".dss":
        return "opendss"
    if suffixes[-2:] == [".pypsa", ".nc"] or (suffixes and suffixes[-1] in {".nc", ".netcdf", ".h5", ".hdf5"}):
        return "pypsa"
    if suffixes and suffixes[-1] == ".zip":
        return "cgmes"
    if suffixes and suffixes[-1] == ".json":
        return detect_json_source_format(source_path)
    raise ValueError(
        f"Could not auto-detect the source format for {source_path}. "
        "Use --source-format explicitly."
    )


def safe_validation(factory: callable, source_format: str, source: str) -> ValidationResult:
    try:
        return factory()
    except Exception as exc:
        raise ConversionError(
            f"{source_format} source validation failed for {source}. "
            f"This model is likely outside the currently validated support subset. Root cause: {exc}"
        ) from exc


def detect_json_source_format(source_path: Path) -> str:
    text = source_path.read_text()
    if "\"_module\": \"pandapower.auxiliary\"" in text or "\"bus\":" in text:
        return "pandapower"
    raise ValueError(
        f"Could not auto-detect the JSON source format for {source_path}. "
        "Use --source-format explicitly."
    )


def describe_route_staleness(route: Route, existing: dict[str, Any] | None) -> dict[str, Any]:
    if existing is None:
        return {"stale": True, "reason": "missing result"}
    timestamp_text = existing.get("timestamp")
    if not timestamp_text:
        return {"stale": None, "reason": "record has no timestamp"}
    try:
        validated_at = datetime.fromisoformat(str(timestamp_text).replace("Z", "+00:00"))
    except ValueError:
        return {"stale": None, "reason": f"invalid timestamp {timestamp_text}"}

    source_files = route_source_files(route)
    if not source_files:
        return {"stale": None, "reason": "no importer/exporter source files available"}

    latest_path = max(source_files, key=lambda path: path.stat().st_mtime)
    latest_mtime = datetime.fromtimestamp(latest_path.stat().st_mtime, tz=timezone.utc)
    if latest_mtime > validated_at.astimezone(timezone.utc):
        return {
            "stale": True,
            "reason": f"{latest_path.name} modified after validation",
        }
    return {"stale": False, "reason": None}


def route_source_files(route: Route) -> list[Path]:
    files: list[Path] = []
    for callable_obj in (route.importer, route.exporter):
        if callable_obj is None:
            continue
        code = getattr(callable_obj, "__code__", None)
        if code is None:
            continue
        path = Path(code.co_filename)
        if path.exists():
            files.append(path)
    unique_files: list[Path] = []
    for path in files:
        if path not in unique_files:
            unique_files.append(path)
    return unique_files


def build_precheck_result(
    case: CanonicalCase,
    target_format: str,
    pandapower: PandapowerAdapter,
    pypsa: PypsaAdapter,
    cgmes: CGMESExportAdapter,
    validator: ValidationService,
    args: argparse.Namespace,
    *,
    source_validation: ValidationResult | None,
) -> dict[str, Any]:
    issues: list[str] = []
    if case.source_format == target_format:
        issues.append("source and target formats are identical")

    component_counts = summarize_case_components(case)
    expected_validation = describe_target_validation(case, target_format)

    try:
        with TemporaryDirectory(prefix="pmc_precheck_") as tmp:
            destination = determine_output_path(case, target_format, None, tmp)
            export_and_validate_target(
                case,
                target_format,
                destination,
                None,
                pandapower,
                pypsa,
                cgmes,
                validator,
                args,
                validate_target=False,
            )
    except ValueError as exc:
        issues.append(str(exc))

    return {
        "case_id": case.case_id,
        "source_format": case.source_format,
        "target_format": target_format,
        "is_unbalanced": case.is_unbalanced,
        "phase_count": case.phase_count,
        "component_counts": component_counts,
        "supported": len(issues) == 0,
        "issues": issues,
        "source_validation": asdict(source_validation) if source_validation else None,
        "target_validation_mode": expected_validation,
    }


def summarize_case_components(case: CanonicalCase) -> dict[str, int]:
    summary: dict[str, int] = {}
    for table_name in sorted(case.tables):
        if table_name.startswith("res_"):
            continue
        table = case.table(table_name)
        if len(table):
            summary[table_name] = len(table)
    return summary


def describe_target_validation(case: CanonicalCase, target_format: str) -> str:
    if target_format == "pandapower":
        return "source-backed full deterministic slack and voltage validation against the imported pandapower case"
    if target_format == "pandapower_split":
        return "source-backed full deterministic node-voltage validation against a phase-split pandapower approximation"
    if target_format == "powermodels":
        return "Julia PowerModels AC power flow with full deterministic bus-voltage comparison"
    if target_format == "powersystems":
        return "Julia PowerSystems + PowerSimulations AC power flow with full deterministic bus-voltage comparison"
    if target_format == "pypsa":
        return "PyPSA AC power flow with full deterministic bus-voltage comparison"
    if target_format == "powermodelsdistribution":
        return "Julia PowerModelsDistribution power flow with full deterministic node-voltage comparison"
    if target_format == "opendss":
        return "OpenDSS source solve with full deterministic voltage comparison after export"
    if target_format == "cgmes":
        return "CGMES export followed by re-import and full deterministic pandapower voltage comparison"
    return "no validation mode declared"


def determine_output_path(case: CanonicalCase, target_format: str, output: str | None, export_dir: str | Path) -> Path:
    if output:
        return Path(output)
    source_path = case.source_path
    if source_path is not None:
        directory = source_path.parent if source_path.is_file() else source_path
        base_name = infer_output_basename(source_path)
    else:
        directory = Path(export_dir)
        base_name = case.case_id
    suffixes = {
        "pandapower": ".pandapower.json",
        "pandapower_split": ".pandapower-split.json",
        "powermodels": ".powermodels.json",
        "powersystems": ".powersystems.m",
        "powermodelsdistribution": "_powermodelsdistribution.dss",
        "pypsa": ".pypsa.nc",
        "opendss": ".dss",
        "cgmes": ".cgmes.zip",
    }
    return directory / f"{base_name}{suffixes[target_format]}"


def infer_output_basename(source_path: Path) -> str:
    name = source_path.name
    compound_suffixes = [
        ".pandapower.json",
        ".powermodels.json",
        ".powersystems.m",
        ".pypsa.nc",
        ".cgmes.zip",
        ".netcdf",
        ".hdf5",
    ]
    for suffix in compound_suffixes:
        if name.lower().endswith(suffix):
            return name[: -len(suffix)]
    if source_path.suffix:
        return source_path.stem
    return source_path.name


def export_and_validate_target(
    case: CanonicalCase,
    target_format: str,
    output_path: Path,
    source_reference: Any,
    pandapower: PandapowerAdapter,
    pypsa: PypsaAdapter,
    cgmes: CGMESExportAdapter,
    validator: ValidationService,
    args: argparse.Namespace,
    *,
    validate_target: bool = True,
) -> tuple[Path, ValidationResult | None]:
    if target_format == "pandapower":
        exported_path = pandapower.export_json(case, output_path)
        if not validate_target:
            return exported_path, None
        target_case = PandapowerImportAdapter().import_case(exported_path)
        target_validation = validate_pandapower_target(case, target_case, source_reference, pandapower, validator)
        return exported_path, target_validation

    if target_format == "matpower":
        exported_path = MatpowerExportAdapter().export_case(case, output_path)
        if not validate_target:
            return exported_path, None
        target_case = MatpowerImportAdapter().import_case(exported_path)
        reference_net = pandapower.run_power_flow(case)
        target_validation = validate_against_pandapower_reference(target_case, reference_net, validator)
        return exported_path, target_validation

    if target_format == "pandapower_split":
        exported_path = PandapowerSplitExportAdapter().export_case(case, output_path)
        if not validate_target:
            return exported_path, None
        target_case = PandapowerImportAdapter().import_case(exported_path)
        if not source_reference or source_reference.get("backend") != "opendss_3ph":
            raise ValueError("pandapower_split validation currently requires an OpenDSS unbalanced source reference.")
        target_validation = validator.validate_pandapower_split_against_opendss(
            target_case,
            source_reference["snapshot"],
        )
        return exported_path, target_validation

    if target_format == "powermodels":
        exported_path = pandapower.export_powermodels_json(case, output_path)
        target_validation = None
        if validate_target:
            target_validation = validator.validate_powermodels_export(
                case,
                powermodels_json=exported_path,
                julia_binary=args.julia_binary,
                julia_script=Path(args.julia_script),
                julia_depot=Path(args.julia_depot),
            )
        return exported_path, target_validation

    if target_format == "powersystems":
        exported_path = PowerSystemsExportAdapter().export_case(case, output_path)
        target_validation = None
        if validate_target:
            target_validation = validator.validate_powersystems_export(
                case,
                powersystems_case=exported_path,
                julia_binary=args.julia_binary,
                julia_script=Path(args.julia_psi_script),
                julia_depot=Path(args.julia_depot),
                julia_project=Path(args.julia_psi_project),
            )
        return exported_path, target_validation

    if target_format == "powermodelsdistribution":
        exported_path = PowerModelsDistributionAdapter().export_input(case, output_path)
        target_validation = None
        if validate_target:
            target_validation = validate_powermodelsdistribution_target(
                case,
                exported_path,
                source_reference,
                validator,
                pandapower,
                args,
            )
        return exported_path, target_validation

    if target_format == "pypsa":
        exported_path = pypsa.export_netcdf(case, output_path)
        target_validation = validator.validate_pypsa_export(case, pypsa_path=exported_path) if validate_target else None
        return exported_path, target_validation

    if target_format == "opendss":
        exported_path = OpenDSSExportAdapter().export_case(case, output_path)
        target_validation = None
        if validate_target:
            opendss_reference = OpenDSSImportAdapter().solve_source_case(exported_path)
            if case.is_unbalanced:
                target_validation = validator.validate_pandapower_unbalanced_against_opendss(case, opendss_reference)
            else:
                target_validation = validator.validate_pandapower_case_against_opendss(case, opendss_reference)
        return exported_path, target_validation

    if target_format == "cgmes":
        exported_path = cgmes.export_case(case, output_path)
        target_validation = None
        if validate_target:
            imported_case = CGMESImportAdapter().import_case(exported_path)
            reference_net = pandapower.run_power_flow(case)
            target_validation = validate_against_pandapower_reference(imported_case, reference_net, validator)
        return exported_path, target_validation

    raise ValueError(f"Unsupported target format {target_format}")


def validate_pandapower_target(
    source_case: CanonicalCase,
    target_case: CanonicalCase,
    source_reference: Any,
    pandapower: PandapowerAdapter,
    validator: ValidationService,
) -> ValidationResult:
    if source_case.source_format in {"pypsa", "pypower"}:
        return validator.validate_against_pandapower(
            target_case,
            reference_slack_p_mw=source_reference["snapshot"].slack_p_mw,
            reference_slack_q_mvar=source_reference["snapshot"].slack_q_mvar,
            reference_voltages=source_reference["snapshot"].voltages,
        )
    if source_reference and source_reference.get("backend") == "opendss":
        return validator.validate_opendss_roundtrip(target_case, source_reference["snapshot"])
    if source_reference and source_reference.get("backend") == "opendss_3ph":
        return validator.validate_opendss_unbalanced_roundtrip(target_case, source_reference["snapshot"])
    if target_case.is_unbalanced:
        return validator.validate_pandapower_unbalanced_roundtrip(target_case)
    reference_net = source_reference["reference_net"] if source_reference and "reference_net" in source_reference else pandapower.run_power_flow(source_case)
    return validate_against_pandapower_reference(target_case, reference_net, validator)


def validate_against_pandapower_reference(
    case: CanonicalCase,
    reference_net: Any,
    validator: ValidationService,
) -> ValidationResult:
    reference_slack_p, reference_slack_q = validator._extract_balanced_slack(reference_net)
    reference_voltages = {
        validator._bus_key(reference_net, int(idx)): complex(
            float(row.vm_pu) * math.cos(math.radians(float(row.va_degree))),
            float(row.vm_pu) * math.sin(math.radians(float(row.va_degree))),
        )
        for idx, row in reference_net.res_bus.iterrows()
    }
    return validator.validate_against_pandapower(
        case,
        reference_slack_p_mw=reference_slack_p,
        reference_slack_q_mvar=reference_slack_q,
        reference_voltages=reference_voltages,
    )


def validate_powermodelsdistribution_target(
    case: CanonicalCase,
    exported_path: Path,
    source_reference: Any,
    validator: ValidationService,
    pandapower: PandapowerAdapter,
    args: argparse.Namespace,
) -> ValidationResult:
    if source_reference and source_reference.get("backend") == "opendss_3ph":
        reference_slack_p_mw = source_reference["snapshot"].slack_p_mw
        reference_slack_q_mvar = source_reference["snapshot"].slack_q_mvar
        reference_node_voltages = source_reference["snapshot"].node_voltages
    else:
        reference_net = pandapower.run_power_flow_3ph(case)
        slack = validator._extract_3ph_slack(reference_net)
        reference_slack_p_mw = sum(slack[key] for key in ("p_a_mw", "p_b_mw", "p_c_mw"))
        reference_slack_q_mvar = sum(slack[key] for key in ("q_a_mvar", "q_b_mvar", "q_c_mvar"))
        reference_node_voltages = validator._extract_3ph_node_voltages(reference_net)
    return validator.validate_powermodelsdistribution_export(
        case,
        pmd_input_path=exported_path,
        julia_binary=args.julia_binary,
        julia_script=Path(args.julia_pmd_script),
        julia_depot=Path(args.julia_depot),
        julia_project=Path(args.julia_pmd_project),
        reference_slack_p_mw=reference_slack_p_mw,
        reference_slack_q_mvar=reference_slack_q_mvar,
        reference_node_voltages=reference_node_voltages,
    )


def find_registered_route(source_tool: str, target_tool: str, case_id: str) -> Route:
    ensure_routes_loaded()
    for route in get_routes(source=source_tool, target=target_tool):
        if route.test_cases and route.test_cases[0] == case_id:
            return route
    raise ValueError(f"No registered route found for {source_tool} -> {target_tool} case {case_id}.")


def filter_registered_routes(expression: str) -> list[Route]:
    ensure_routes_loaded()
    routes = get_routes()
    expr = (expression or "").strip()
    if not expr:
        return routes
    if expr == "not julia":
        return [route for route in routes if not any(req.startswith("julia") for req in route.requires)]
    filtered = routes
    for part in [segment.strip() for segment in expr.split(",") if segment.strip()]:
        if "!=" in part:
            key, value = [item.strip() for item in part.split("!=", 1)]
            if key == "target":
                filtered = [route for route in filtered if route.target_tool != value]
            elif key == "source":
                filtered = [route for route in filtered if route.source_tool != value]
            elif key == "model_type":
                filtered = [route for route in filtered if route.model_type != value]
            elif key == "requires":
                filtered = [route for route in filtered if value not in route.requires]
        elif "=" in part:
            key, value = [item.strip() for item in part.split("=", 1)]
            if key == "target":
                filtered = [route for route in filtered if route.target_tool == value]
            elif key == "source":
                filtered = [route for route in filtered if route.source_tool == value]
            elif key == "model_type":
                filtered = [route for route in filtered if route.model_type == value]
            elif key == "requires":
                filtered = [route for route in filtered if value in route.requires]
        elif part == "julia":
            filtered = [route for route in filtered if any(req.startswith("julia") for req in route.requires)]
    return filtered


def resolve_registered_case_source(route: Route) -> str | Path:
    case_id = route.test_cases[0] if route.test_cases else ""
    sample_root = REPO_ROOT / "src" / "powermodelconverter" / "data" / "samples"
    if route.source_tool == "matpower" and case_id == "case9":
        return sample_root / "matpower/case9.m"
    if route.source_tool == "opendss":
        known = {
            "minimal_radial": sample_root / "opendss/minimal_radial.dss",
            "minimal_chain": sample_root / "opendss/minimal_chain.dss",
            "minimal_unbalanced_3ph": sample_root / "opendss/minimal_unbalanced_3ph.dss",
            "minimal_unbalanced_branch": sample_root / "opendss/minimal_unbalanced_branch.dss",
            "IEEE13Nodeckt": sample_root / "opendss/IEEE13Nodeckt.dss",
        }
        if case_id in known:
            return known[case_id]
    if route.source_tool == "pandapower":
        known = {
            "ieee_european_lv_asymmetric": sample_root / "pandapower/ieee_european_lv_asymmetric.json",
        }
        if case_id in known:
            return known[case_id]
    if route.source_tool == "cgmes" and case_id == "cgmes":
        return sample_root / "cgmes"
    if route.source_tool == "pypsa" and case_id in {"case9", "case9_from_matpower.pandapower"}:
        return sample_root / "matpower/case9.pypsa.nc"
    if route.source_tool == "pypower":
        if "26" in case_id:
            return str(REPO_ROOT / "input" / "DTU7K.py") + "::26"
        return REPO_ROOT / "input" / "DTU7K.py"
    if route.source_tool == "simbench":
        return "1-HV-mixed--0-no_sw"
    if route.source_tool == "powersystems":
        return sample_root / "matpower/case9.m"
    if route.source_tool == "powermodelsdistribution":
        return sample_root / "opendss/minimal_unbalanced_3ph.dss"
    raise ValueError(f"No concrete source resolver is defined for route {route.route_id}.")


def sanitize_case_id(case_id: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", case_id)


def validation_result_to_record(
    *,
    case_id: str,
    source_tool: str,
    export_tool: str,
    model_type: str,
    notes: str,
    result: ValidationResult | None,
    worker_id: str,
) -> dict[str, Any]:
    compared_points = 0
    if result is not None:
        compared_points = int(
            result.details.get("compared_buses")
            or result.details.get("compared_nodes")
            or result.details.get("compared_points")
            or 0
        )
    return {
        "case_id": case_id,
        "source_tool": source_tool,
        "export_tool": export_tool,
        "model_type": model_type,
        "status": "validated" if result and result.passed else "failed",
        "slack_delta_mva": None if result is None else result.slack_delta_mva,
        "max_voltage_delta_pu": None if result is None else result.max_voltage_delta_pu,
        "compared_points": compared_points,
        "notes": notes,
        "timestamp": ValidationResult(
            case_id=case_id,
            passed=False,
            slack_delta_mva=0.0,
            max_voltage_delta_pu=0.0,
        ).timestamp if result is None else result.timestamp,
        "duration_seconds": 0.0 if result is None else result.duration_seconds,
        "worker_id": worker_id,
    }


if __name__ == "__main__":
    main()
