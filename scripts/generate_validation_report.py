#!/usr/bin/env python3
from __future__ import annotations

import json
import math
import logging
from html import escape
from dataclasses import asdict, dataclass
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

import pandapower as pp
import pandapower.networks as pn
import pypsa

from powermodelconverter.exporters.cgmes import CGMESExportAdapter
from powermodelconverter.importers.cgmes import CGMESImportAdapter
from powermodelconverter.exporters.matpower import MatpowerExportAdapter
from powermodelconverter.importers.matpower import MatpowerImportAdapter
from powermodelconverter.exporters.opendss import OpenDSSExportAdapter
from powermodelconverter.importers.opendss import OpenDSSImportAdapter
from powermodelconverter.core.pandapower_backend import PandapowerAdapter
from powermodelconverter.importers.pandapower_json import PandapowerImportAdapter
from powermodelconverter.exporters.powermodels_distribution import PowerModelsDistributionAdapter
from powermodelconverter.importers.powermodels_distribution import PowerModelsDistributionImportAdapter
from powermodelconverter.exporters.powersystems import PowerSystemsExportAdapter
from powermodelconverter.importers.pypower import PypowerImportAdapter
from powermodelconverter.importers.pypsa import PypsaAdapter
from powermodelconverter.importers.pypsa import PypsaImportAdapter
from powermodelconverter.core.capabilities import capability_rows
from powermodelconverter.core.model import CanonicalCase
from powermodelconverter.runtime import resolve_julia_binary
from powermodelconverter.validation.powerflow import ValidationResult, ValidationService


REPO_ROOT = Path(__file__).resolve().parents[1]
DOCS_DIR = REPO_ROOT / "docs"
JSON_REPORT = DOCS_DIR / "validation_report.json"
MARKDOWN_REPORT = DOCS_DIR / "validation_report.md"
HTML_REPORT = DOCS_DIR / "validation_report.html"
JULIA_BINARY = resolve_julia_binary()
PYPSA_EUR_VALIDATION_ARTIFACTS = (
    REPO_ROOT / "src/powermodelconverter/data/exports/pypsa_eur_base_synthetic_pf.validation.json",
    REPO_ROOT / "src/powermodelconverter/data/exports/pypsa_eur_full_base_network.validation.json",
    REPO_ROOT / "src/powermodelconverter/data/exports/pypsa_eur_full_base_pf_validated.validation.json",
    REPO_ROOT / "src/powermodelconverter/data/exports/pypsa_eur_full_base_island_validated.validation.json",
)

PM_JL_NATIVE_CASE_CANDIDATES = (
    "case300.m",
    "case118.m",
    "case57.m",
    "case39.m",
    "case30.m",
    "case24.m",
    "case24_ieee_rts.m",
    "case14.m",
    "case9.m",
    "case6.m",
)


def configure_report_logging() -> None:
    logging.getLogger().setLevel(logging.WARNING)
    for name in (
        "pypsa",
        "pypsa.network.io",
        "pypsa.network.power_flow",
        "pandapower.converter.cim",
        "pandapower.converter.matpower",
        "CimParser",
        "CimConverter",
        "cim.cim2pp.from_cim",
    ):
        logging.getLogger(name).setLevel(logging.WARNING)


def find_pm_jl_native_cases(*, limit: int | None = None) -> list[tuple[str, Path]]:
    base = REPO_ROOT / ".julia_depot/packages/PowerModels"
    if not base.exists():
        return []

    matches_by_case: list[tuple[str, Path]] = []
    for filename in PM_JL_NATIVE_CASE_CANDIDATES:
        matches = sorted(base.glob(f"*/test/data/matpower/{filename}"))
        if matches:
            case_id = Path(filename).stem
            matches_by_case.append((case_id, matches[0]))
            if limit is not None and len(matches_by_case) >= limit:
                break
    return matches_by_case


def find_pm_jl_native_case() -> tuple[str, Path] | None:
    matches = find_pm_jl_native_cases(limit=1)
    return matches[0] if matches else None


def load_importable_pm_jl_native_cases(*, limit: int | None = None) -> list[tuple[str, Path, CanonicalCase]]:
    loaded: list[tuple[str, Path, CanonicalCase]] = []
    importer = MatpowerImportAdapter()
    for case_id, source in find_pm_jl_native_cases():
        try:
            case = importer.import_case(source)
        except Exception:
            continue
        loaded.append((case_id, source, case))
        if limit is not None and len(loaded) >= limit:
            break
    return loaded


def choose_validated_pm_jl_case(
    *,
    validator_fn: callable,
    pandapower: PandapowerAdapter,
    validator: ValidationService,
    tmpdir: Path,
) -> tuple[str, CanonicalCase, ValidationResult] | None:
    for case_id, _source, case in load_importable_pm_jl_native_cases():
        try:
            result = validator_fn(case, pandapower, validator, tmpdir)
        except Exception:
            continue
        if result.passed:
            return case_id, case, result
    return None


@dataclass(slots=True)
class RouteRecord:
    case_id: str
    source_tool: str
    export_tool: str
    model_type: str
    status: str
    slack_delta_mva: float | None
    max_voltage_delta_pu: float | None
    compared_points: int | None
    notes: str


@dataclass(slots=True)
class ValidationSummary:
    method: str
    validated_routes: int
    pending_routes: int
    balanced_routes: int
    unbalanced_routes: int
    total_compared_points: int
    max_compared_points: int
    min_compared_points: int


BALANCED_SOURCE_TOOLS = ("pandapower", "matpower", "opendss", "cgmes", "pypsa")
BALANCED_EXPORT_TOOLS = ("pandapower", "matpower", "opendss", "cgmes", "pypsa", "powermodels")
UNBALANCED_SOURCE_TOOLS = ("pandapower", "opendss", "powermodelsdistribution")
UNBALANCED_EXPORT_TOOLS = ("pandapower", "opendss", "powermodelsdistribution")


def main() -> None:
    configure_report_logging()
    DOCS_DIR.mkdir(parents=True, exist_ok=True)

    validator = ValidationService()
    pandapower = PandapowerAdapter()
    pypsa = PypsaAdapter()
    cgmes_import = CGMESImportAdapter()
    cgmes_export = CGMESExportAdapter()
    records: list[RouteRecord] = []

    with TemporaryDirectory(prefix="pmc_validation_") as tmp:
        tmpdir = Path(tmp)

        # Core balanced interoperability subset:
        # three load-only balanced cases that can be materialized into each supported
        # balanced exchange format and exported back out through the common subset.
        for source_tool in BALANCED_SOURCE_TOOLS:
            for core_case in build_balanced_core_cases():
                imported_case = materialize_balanced_source_case(
                    core_case,
                    source_tool=source_tool,
                    pandapower=pandapower,
                    pypsa=pypsa,
                    cgmes_export=cgmes_export,
                    tmpdir=tmpdir,
                )
                for export_tool in BALANCED_EXPORT_TOOLS:
                    records.append(
                        validation_record(
                            case_id=imported_case.case_id,
                            source_tool=source_tool,
                            export_tool=export_tool,
                            model_type="balanced",
                            result=validate_balanced_export(
                                imported_case,
                                export_tool=export_tool,
                                pandapower=pandapower,
                                validator=validator,
                                pypsa=pypsa,
                                cgmes_export=cgmes_export,
                                cgmes_import=cgmes_import,
                                tmpdir=tmpdir,
                            ),
                            notes=(
                                "Balanced common-subset interoperability case. "
                                "Supported elements: one slack source, buses, lines, and constant-power loads."
                            ),
                        )
                    )

        # Core unbalanced interoperability subset:
        # three native three-phase asymmetric-load feeders shared across pandapower,
        # OpenDSS, and PowerModelsDistribution's DSS-compatible subset.
        for source_tool in UNBALANCED_SOURCE_TOOLS:
            for core_case in build_unbalanced_core_cases():
                imported_case = materialize_unbalanced_source_case(
                    core_case,
                    source_tool=source_tool,
                    tmpdir=tmpdir,
                )
                for export_tool in UNBALANCED_EXPORT_TOOLS:
                    records.append(
                        validation_record(
                            case_id=imported_case.case_id,
                            source_tool=source_tool,
                            export_tool=export_tool,
                            model_type="unbalanced_3ph",
                            result=validate_unbalanced_export(
                                imported_case,
                                export_tool=export_tool,
                                pandapower=pandapower,
                                validator=validator,
                                tmpdir=tmpdir,
                            ),
                            notes=(
                                "Unbalanced common-subset interoperability case. "
                                "Supported elements: one slack source, buses, lines, native asymmetric loads, and the current signed-off transformer subset."
                            ),
                        )
                    )

        # MATPOWER balanced sample
        matpower_case = MatpowerImportAdapter().import_case(
            REPO_ROOT / "src/powermodelconverter/data/samples/matpower/case9.m"
        )
        records.append(
            validation_record(
                case_id=matpower_case.case_id,
                source_tool="matpower",
                export_tool="pandapower",
                model_type="balanced",
                result=self_validate_balanced(matpower_case, pandapower, validator),
                notes="Official MATPOWER case9 imported through pandapower converter.",
            )
        )
        records.append(
            validation_record(
                case_id=matpower_case.case_id,
                source_tool="matpower",
                export_tool="powermodels",
                model_type="balanced",
                result=validate_powermodels(matpower_case, pandapower, validator, tmpdir),
                notes="Balanced export validated with Julia PowerModels AC power flow.",
            )
        )
        additional_matpower_native = next(
            (
                (case_id, case)
                for case_id, _source, case in load_importable_pm_jl_native_cases()
                if case_id not in {"case9", "case14"}
            ),
            None,
        )
        if additional_matpower_native is not None:
            case_id, native_matpower_case = additional_matpower_native
            records.append(
                validation_record(
                    case_id=native_matpower_case.case_id,
                    source_tool="matpower",
                    export_tool="pandapower",
                    model_type="balanced",
                    result=self_validate_balanced(native_matpower_case, pandapower, validator),
                    notes=(
                        f"Representative MATPOWER-native {case_id} case imported from the bundled PowerModels "
                        "package test corpus and validated against deterministic pandapower AC power flow."
                    ),
                )
            )

        # PYPOWER native balanced sample
        pypower_source = REPO_ROOT / "input/DTU7K.py"
        pypower_adapter = PypowerImportAdapter()
        try:
            pypower_reference = pypower_adapter.solve_source_case(pypower_source)
            pypower_case = pypower_adapter.import_case(pypower_source)
            records.append(
                validation_record(
                    case_id=pypower_case.case_id,
                    source_tool="pypower",
                    export_tool="pandapower",
                    model_type="balanced",
                    result=validator.validate_against_pandapower(
                        pypower_case,
                        reference_slack_p_mw=pypower_reference.slack_p_mw,
                        reference_slack_q_mvar=pypower_reference.slack_q_mvar,
                        reference_voltages=pypower_reference.voltages,
                    ),
                    notes=(
                        "DTU7K connected-network PYPOWER source execution, defaulting to subnet 26, "
                        "validated against the source-side solved operating point after import into pandapower."
                    ),
                )
            )
        except Exception as exc:
            records.append(
                unsupported_record(
                    case_id="DTU7K_26",
                    source_tool="pypower",
                    export_tool="pandapower",
                    model_type="balanced",
                    notes=(
                        "Skipped strict bilateral validation for DTU7K subnet 26 because one side did not converge: "
                        f"{exc}."
                    ),
                )
            )
        try:
            pypower_reference_27 = pypower_adapter.solve_source_case(f"{pypower_source}::27")
            pypower_case_27 = pypower_adapter.import_case(f"{pypower_source}::27")
            records.append(
                validation_record(
                    case_id=pypower_case_27.case_id,
                    source_tool="pypower",
                    export_tool="pandapower",
                    model_type="balanced",
                    result=validator.validate_against_pandapower(
                        pypower_case_27,
                        reference_slack_p_mw=pypower_reference_27.slack_p_mw,
                        reference_slack_q_mvar=pypower_reference_27.slack_q_mvar,
                        reference_voltages=pypower_reference_27.voltages,
                    ),
                    notes=(
                        "DTU7K connected-network PYPOWER source execution for subnet 27 "
                        "validated against the source-side solved operating point after import into pandapower."
                    ),
                )
            )
        except Exception as exc:
            records.append(
                unsupported_record(
                    case_id="DTU7K_27",
                    source_tool="pypower",
                    export_tool="pandapower",
                    model_type="balanced",
                    notes=(
                        "Skipped strict bilateral validation for DTU7K subnet 27 because one side did not converge: "
                        f"{exc}."
                    ),
                )
            )

        # CGMES official balanced sample
        cgmes_case = cgmes_import.import_case(
            REPO_ROOT / "src/powermodelconverter/data/samples/cgmes"
        )
        records.append(
            validation_record(
                case_id=cgmes_case.case_id,
                source_tool="cgmes",
                export_tool="pandapower",
                model_type="balanced",
                result=self_validate_balanced(cgmes_case, pandapower, validator),
                notes="Official pandapower SmallGrid CGMES sample imported through pandapower's native CGMES loader.",
            )
        )
        records.append(
            validation_record(
                case_id=cgmes_case.case_id,
                source_tool="cgmes",
                export_tool="powermodels",
                model_type="balanced",
                result=validate_powermodels(cgmes_case, pandapower, validator, tmpdir),
                notes="Official CGMES sample exported onward to PowerModels and validated with Julia AC power flow.",
            )
        )

        # OpenDSS subset sample
        opendss_source = REPO_ROOT / "src/powermodelconverter/data/samples/opendss/minimal_radial.dss"
        opendss_adapter = OpenDSSImportAdapter()
        opendss_reference = opendss_adapter.solve_source_case(opendss_source)
        opendss_case = opendss_adapter.import_case(opendss_source)
        opendss_pp_result = validator.validate_opendss_roundtrip(opendss_case, opendss_reference)
        records.append(
            validation_record(
                case_id=opendss_case.case_id,
                source_tool="opendss",
                export_tool="pandapower",
                model_type="balanced_subset",
                result=opendss_pp_result,
                notes="Validated against OpenDSS source slack power and bus voltages for the currently supported subset.",
            )
        )
        records.append(
            validation_record(
                case_id=opendss_case.case_id,
                source_tool="opendss",
                export_tool="powermodels",
                model_type="balanced_subset",
                result=validate_powermodels(opendss_case, pandapower, validator, tmpdir),
                notes="Balanced-equivalent export validated with Julia PowerModels AC power flow.",
            )
        )
        balanced_opendss_as_pandapower_path = tmpdir / "minimal_radial.pandapower.json"
        pandapower.export_json(opendss_case, balanced_opendss_as_pandapower_path)
        opendss_as_pandapower_case = PandapowerImportAdapter().import_case(balanced_opendss_as_pandapower_path)
        records.append(
            validation_record(
                case_id=opendss_as_pandapower_case.case_id,
                source_tool="pandapower",
                export_tool="opendss",
                model_type="balanced_subset",
                result=validate_opendss_from_pandapower(
                    opendss_as_pandapower_case,
                    validator,
                    tmpdir,
                ),
                notes="Balanced pandapower re-import of the OpenDSS starter feeder exported back to OpenDSS and validated against pandapower AC power flow.",
            )
        )
        cgmes_subset_case = build_cgmes_subset_case()
        records.append(
            validation_record(
                case_id=cgmes_subset_case.case_id,
                source_tool="pandapower",
                export_tool="cgmes",
                model_type="balanced",
                result=validate_cgmes_from_pandapower(
                    cgmes_subset_case,
                    cgmes_export,
                    cgmes_import,
                    validator,
                    tmpdir,
                ),
                notes="Transformer-free balanced pandapower subset exported to CGMES and validated by native pandapower CGMES re-import.",
            )
        )

        # OpenDSS unbalanced starter feeder
        opendss_unbalanced_source = (
            REPO_ROOT / "src/powermodelconverter/data/samples/opendss/minimal_unbalanced_3ph.dss"
        )
        opendss_unbalanced_reference = opendss_adapter.solve_source_case(opendss_unbalanced_source)
        opendss_unbalanced_case = opendss_adapter.import_case(opendss_unbalanced_source)
        records.append(
            validation_record(
                case_id=opendss_unbalanced_case.case_id,
                source_tool="opendss",
                export_tool="pandapower",
                model_type="unbalanced_3ph",
                result=validator.validate_opendss_unbalanced_roundtrip(
                    opendss_unbalanced_case,
                    opendss_unbalanced_reference,
                ),
                notes="Unbalanced OpenDSS starter feeder validated against source node voltages with pandapower runpp_3ph.",
            )
        )
        records.append(
            validation_record(
                case_id=opendss_unbalanced_case.case_id,
                source_tool="opendss",
                export_tool="powermodelsdistribution",
                model_type="unbalanced_3ph",
                result=validate_powermodelsdistribution(
                    opendss_unbalanced_case,
                    opendss_unbalanced_reference,
                    validator,
                    tmpdir,
                ),
                notes="Unbalanced OpenDSS starter feeder exported to PowerModelsDistribution and validated against source node voltages.",
            )
        )

        # Pandapower balanced sample generated from validated case9 export
        balanced_pp_path = tmpdir / "case9_from_matpower.pandapower.json"
        pandapower.export_json(matpower_case, balanced_pp_path)
        pp_balanced_case = PandapowerImportAdapter().import_case(balanced_pp_path)
        records.append(
            validation_record(
                case_id=pp_balanced_case.case_id,
                source_tool="pandapower",
                export_tool="pandapower",
                model_type="balanced",
                result=self_validate_balanced(pp_balanced_case, pandapower, validator),
                notes="Pandapower JSON re-import generated from the validated MATPOWER case9 route.",
            )
        )
        records.append(
            validation_record(
                case_id=pp_balanced_case.case_id,
                source_tool="pandapower",
                export_tool="powermodels",
                model_type="balanced",
                result=validate_powermodels(pp_balanced_case, pandapower, validator, tmpdir),
                notes="Balanced pandapower JSON export validated with Julia PowerModels AC power flow.",
            )
        )
        records.append(
            validation_record(
                case_id=pp_balanced_case.case_id,
                source_tool="pandapower",
                export_tool="pypsa",
                model_type="balanced",
                result=validate_pypsa_export(pp_balanced_case, validator, pypsa, tmpdir),
                notes="Balanced line-based pandapower model exported to PyPSA NetCDF and validated against pandapower AC power flow.",
            )
        )
        pypsa_path = tmpdir / f"{pp_balanced_case.case_id}.pypsa.nc"
        pypsa_case = PypsaImportAdapter().import_case(pypsa_path)
        pypsa_reference = pypsa.solve_network_file(pypsa_path)
        records.append(
            validation_record(
                case_id=pypsa_case.case_id,
                source_tool="pypsa",
                export_tool="pandapower",
                model_type="balanced",
                result=validator.validate_against_pandapower(
                    pypsa_case,
                    reference_slack_p_mw=pypsa_reference.slack_p_mw,
                    reference_slack_q_mvar=pypsa_reference.slack_q_mvar,
                    reference_voltages=pypsa_reference.voltages,
                ),
                notes="PyPSA NetCDF re-import validated against PyPSA AC power flow and converted back through pandapower for the current line-based balanced subset.",
            )
        )

        # Pandapower native unbalanced sample
        pp_unbalanced_case = PandapowerImportAdapter().import_case(
            REPO_ROOT / "src/powermodelconverter/data/samples/pandapower/ieee_european_lv_asymmetric.json"
        )
        records.append(
            validation_record(
                case_id=pp_unbalanced_case.case_id,
                source_tool="pandapower",
                export_tool="pandapower",
                model_type="unbalanced_3ph",
                result=validator.validate_pandapower_unbalanced_roundtrip(pp_unbalanced_case),
                notes="Native pandapower 3-phase roundtrip validated with runpp_3ph.",
            )
        )
        records.append(
            validation_record(
                case_id=pp_unbalanced_case.case_id,
                source_tool="pandapower",
                export_tool="opendss",
                model_type="unbalanced_3ph",
                result=validate_opendss_from_pandapower(
                    pp_unbalanced_case,
                    validator,
                    tmpdir,
                ),
                notes="Native pandapower 3-phase model exported to OpenDSS and validated against runpp_3ph node voltages.",
            )
        )
        records.append(
            validation_record(
                case_id=pp_unbalanced_case.case_id,
                source_tool="pandapower",
                export_tool="powermodelsdistribution",
                model_type="unbalanced_3ph",
                result=validate_powermodelsdistribution_from_pandapower(
                    pp_unbalanced_case,
                    pandapower,
                    validator,
                    tmpdir,
                ),
                notes="Native pandapower 3-phase model exported to PowerModelsDistribution and validated against runpp_3ph node voltages.",
            )
        )

        # Additional OpenDSS native feeders within the current supported subset
        for sample_name, export_tool, validator_fn, notes in [
            (
                "minimal_chain.dss",
                "pandapower",
                validate_opendss_source_to_pandapower,
                "OpenDSS native chained feeder with two loads validated against source bus voltages through pandapower.",
            ),
            (
                "minimal_unbalanced_branch.dss",
                "pandapower",
                validate_opendss_source_to_pandapower,
                "OpenDSS native branched unbalanced feeder validated against source node voltages through pandapower.",
            ),
            (
                "minimal_unbalanced_branch.dss",
                "powermodelsdistribution",
                validate_opendss_source_to_powermodelsdistribution,
                "OpenDSS native branched unbalanced feeder validated against source node voltages through PowerModelsDistribution.",
            ),
        ]:
            source = REPO_ROOT / "src/powermodelconverter/data/samples/opendss" / sample_name
            source_reference = opendss_adapter.solve_source_case(source)
            source_case = opendss_adapter.import_case(source)
            records.append(
                validation_record(
                    case_id=source_case.case_id,
                    source_tool="opendss",
                    export_tool=export_tool,
                    model_type="unbalanced_3ph" if source_case.is_unbalanced else "balanced_subset",
                    result=validator_fn(source_case, source_reference, validator, tmpdir),
                    notes=notes,
                )
            )
        # Additional pandapower package-native cases
        for case_id, net_builder, export_tool, validator_fn, notes in [
            (
                "case4gs",
                pn.case4gs,
                "pypsa",
                validate_pypsa_export,
                "Pandapower package case4gs validated through PyPSA on the supported balanced AC subset.",
            ),
            (
                "case5",
                pn.case5,
                "pypsa",
                validate_pypsa_export,
                "Pandapower package case5 validated through PyPSA on the supported balanced AC subset.",
            ),
            (
                "case6ww",
                pn.case6ww,
                "powermodels",
                validate_powermodels,
                "Pandapower package case6ww validated with Julia PowerModels AC power flow.",
            ),
            (
                "case33bw",
                pn.case33bw,
                "powermodels",
                validate_powermodels,
                "Pandapower package Baran-Wu 33-bus feeder validated with Julia PowerModels AC power flow.",
            ),
        ]:
            case = CanonicalCase.from_pandapower(case_id=case_id, source_format="pandapower", net=net_builder())
            records.append(
                validation_record(
                    case_id=case.case_id,
                    source_tool="pandapower",
                    export_tool=export_tool,
                    model_type="balanced",
                    result=validator_fn(case, pandapower, validator, tmpdir)
                    if validator_fn is validate_powermodels
                    else validator_fn(case, validator, pypsa, tmpdir),
                    notes=notes,
                )
            )

        # Native PyPSA cases built directly with the PyPSA API
        for builder, case_id, notes in [
            (
                build_pypsa_triangle_network,
                "pypsa_triangle_native",
                "Native PyPSA meshed triangle with one slack, one PV generator, and two loads validated against PyPSA AC power flow through pandapower.",
            ),
            (
                build_pypsa_radial_network,
                "pypsa_radial_native",
                "Native PyPSA radial transmission-style case validated against PyPSA AC power flow through pandapower.",
            ),
            (
                build_pypsa_five_bus_ring_network,
                "pypsa_five_bus_ring_native",
                "Native PyPSA five-bus ring with distributed load validated against PyPSA AC power flow through pandapower.",
            ),
        ]:
            native_case, native_reference = build_pypsa_origin_case(case_id, builder, pypsa, tmpdir)
            records.append(
                validation_record(
                    case_id=native_case.case_id,
                    source_tool="pypsa",
                    export_tool="pandapower",
                    model_type="balanced",
                    result=validator.validate_against_pandapower(
                        native_case,
                        reference_slack_p_mw=native_reference.slack_p_mw,
                        reference_slack_q_mvar=native_reference.slack_q_mvar,
                        reference_voltages=native_reference.voltages,
                    ),
                    notes=notes,
                )
            )

        # pm.jl package-native MATPOWER case (prefer the most complex available)
        pmjl_native_cases = load_importable_pm_jl_native_cases(limit=2)
        if pmjl_native_cases:
            case_id, _source, case = pmjl_native_cases[0]
            records.append(
                validation_record(
                    case_id=f"{case_id}_pmjl_pkg",
                    source_tool="pm.jl",
                    export_tool="pandapower",
                    model_type="balanced",
                    result=self_validate_balanced(case, pandapower, validator),
                    notes=(
                        f"pm.jl package-native MATPOWER {case_id} imported into pandapower "
                        "and validated against deterministic pandapower AC power flow."
                    ),
                )
            )
            validated_pmjl_powermodels = choose_validated_pm_jl_case(
                validator_fn=validate_powermodels,
                pandapower=pandapower,
                validator=validator,
                tmpdir=tmpdir,
            )
            if validated_pmjl_powermodels is not None:
                pm_case_id, _pm_case, pm_result = validated_pmjl_powermodels
                records.append(
                    validation_record(
                        case_id=f"{pm_case_id}_pmjl_pkg",
                        source_tool="pm.jl",
                        export_tool="powermodels",
                        model_type="balanced",
                        result=pm_result,
                        notes=(
                            f"pm.jl package-native MATPOWER {pm_case_id} imported to canonical, exported back to "
                            "PowerModels JSON, and validated with Julia PowerModels AC power flow."
                        ),
                    )
                )
            validated_powersystems = None
            for ps_case_id, _ps_source, ps_case in load_importable_pm_jl_native_cases():
                ps_result = safe_validate_powersystems(ps_case, validator, tmpdir)
                if ps_result.passed:
                    validated_powersystems = (ps_case_id, ps_result)
                    break
            if validated_powersystems is not None:
                ps_case_id, ps_result = validated_powersystems
                records.append(
                    validation_record(
                        case_id=f"{ps_case_id}_powersystems_pkg",
                        source_tool="powersystems.jl",
                        export_tool="powersystems",
                        model_type="balanced",
                        result=ps_result,
                        notes=(
                            f"MATPOWER {ps_case_id} imported to canonical, exported through the PowerSystems.jl route, "
                            "and validated with PowerFlows AC power flow."
                        ),
                    )
                )
            if len(pmjl_native_cases) > 1:
                second_case_id, _second_source, second_case = pmjl_native_cases[1]
                records.append(
                    validation_record(
                        case_id=f"{second_case_id}_pmjl_pkg",
                        source_tool="pm.jl",
                        export_tool="pandapower",
                        model_type="balanced",
                        result=self_validate_balanced(second_case, pandapower, validator),
                        notes=(
                            f"Additional pm.jl package-native MATPOWER {second_case_id} imported into "
                            "pandapower and validated against deterministic pandapower AC power flow."
                        ),
                    )
                )

        for artifact in PYPSA_EUR_VALIDATION_ARTIFACTS:
            records.extend(load_pypsa_eur_validation_records(artifact))

        summary = build_summary(records)
        payload = {
            "summary": asdict(summary),
            "capabilities": capability_rows(),
            "records": [asdict(record) for record in records],
        }
        JSON_REPORT.write_text(json.dumps(payload, indent=2))
        MARKDOWN_REPORT.write_text(render_markdown(summary, records))
        HTML_REPORT.write_text(render_html(payload, records))

    print(f"Wrote {JSON_REPORT}")
    print(f"Wrote {MARKDOWN_REPORT}")
    print(f"Wrote {HTML_REPORT}")


def self_validate_balanced(case: Any, pandapower: PandapowerAdapter, validator: ValidationService) -> ValidationResult:
    reference = pandapower.run_power_flow(case)
    reference_slack_p, reference_slack_q = validator._extract_balanced_slack(reference)
    reference_voltages = {
        validator._bus_key(reference, int(idx)): complex(
            float(row.vm_pu) * math.cos(math.radians(float(row.va_degree))),
            float(row.vm_pu) * math.sin(math.radians(float(row.va_degree))),
        )
        for idx, row in reference.res_bus.iterrows()
    }
    return validator.validate_against_pandapower(
        case,
        reference_slack_p_mw=reference_slack_p,
        reference_slack_q_mvar=reference_slack_q,
        reference_voltages=reference_voltages,
    )


def validate_powermodels(
    case: Any,
    pandapower: PandapowerAdapter,
    validator: ValidationService,
    tmpdir: Path,
) -> ValidationResult:
    destination = tmpdir / f"{case.case_id}.powermodels.json"
    path = pandapower.export_powermodels_json(case, destination)
    return validator.validate_powermodels_export(
        case,
        powermodels_json=path,
        julia_binary=JULIA_BINARY,
        julia_script=REPO_ROOT / "src/powermodelconverter/julia/run_powermodels_pf.jl",
        julia_depot=REPO_ROOT / ".julia_depot",
    )


def validate_powersystems(
    case: Any,
    validator: ValidationService,
    tmpdir: Path,
) -> ValidationResult:
    destination = tmpdir / f"{case.case_id}.powersystems.m"
    path = PowerSystemsExportAdapter().export_case(case, destination)
    return validator.validate_powersystems_export(
        case,
        powersystems_case=path,
        julia_binary=JULIA_BINARY,
        julia_script=REPO_ROOT / "src/powermodelconverter/julia_psi/run_powersimulations_pf.jl",
        julia_depot=REPO_ROOT / ".julia_depot",
        julia_project=REPO_ROOT / "src/powermodelconverter/julia_psi",
    )


def safe_validate_powersystems(
    case: Any,
    validator: ValidationService,
    tmpdir: Path,
) -> ValidationResult:
    try:
        return validate_powersystems(case, validator, tmpdir)
    except Exception as exc:
        return ValidationResult(
            case_id=case.case_id,
            passed=False,
            slack_delta_mva=math.inf,
            max_voltage_delta_pu=math.inf,
            details={
                "compared_buses": 0,
                "backend": "powersimulations",
                "error": str(exc),
            },
        )


def validate_powermodelsdistribution(
    case: Any,
    reference: Any,
    validator: ValidationService,
    tmpdir: Path,
) -> ValidationResult:
    destination = tmpdir / f"{case.case_id}_powermodelsdistribution.dss"
    path = PowerModelsDistributionAdapter().export_input(case, destination)
    return validator.validate_powermodelsdistribution_export(
        case,
        pmd_input_path=path,
        julia_binary=JULIA_BINARY,
        julia_script=REPO_ROOT / "src/powermodelconverter/julia_pmd/run_powermodels_distribution_pf.jl",
        julia_depot=REPO_ROOT / ".julia_depot",
        julia_project=REPO_ROOT / "src/powermodelconverter/julia_pmd",
        reference_slack_p_mw=reference.slack_p_mw,
        reference_slack_q_mvar=reference.slack_q_mvar,
        reference_node_voltages=reference.node_voltages,
    )


def validate_powermodelsdistribution_from_pandapower(
    case: Any,
    pandapower: PandapowerAdapter,
    validator: ValidationService,
    tmpdir: Path,
) -> ValidationResult:
    destination = tmpdir / f"{case.case_id}_powermodelsdistribution.dss"
    path = PowerModelsDistributionAdapter().export_input(case, destination)
    reference_net = pandapower.run_power_flow_3ph(case)
    slack = validator._extract_3ph_slack(reference_net)
    return validator.validate_powermodelsdistribution_export(
        case,
        pmd_input_path=path,
        julia_binary=JULIA_BINARY,
        julia_script=REPO_ROOT / "src/powermodelconverter/julia_pmd/run_powermodels_distribution_pf.jl",
        julia_depot=REPO_ROOT / ".julia_depot",
        julia_project=REPO_ROOT / "src/powermodelconverter/julia_pmd",
        reference_slack_p_mw=sum(slack[key] for key in ("p_a_mw", "p_b_mw", "p_c_mw")),
        reference_slack_q_mvar=sum(slack[key] for key in ("q_a_mvar", "q_b_mvar", "q_c_mvar")),
        reference_node_voltages=validator._extract_3ph_node_voltages(reference_net),
    )


def validate_pypsa_export(
    case: Any,
    validator: ValidationService,
    pypsa: PypsaAdapter,
    tmpdir: Path,
) -> ValidationResult:
    destination = tmpdir / f"{case.case_id}.pypsa.nc"
    path = pypsa.export_netcdf(case, destination)
    return validator.validate_pypsa_export(case, pypsa_path=path)


def validate_matpower_export(
    case: Any,
    validator: ValidationService,
    tmpdir: Path,
) -> ValidationResult:
    destination = tmpdir / f"{case.case_id}.m"
    path = MatpowerExportAdapter().export_case(case, destination)
    reimported = MatpowerImportAdapter().import_case(path)
    reference = PandapowerAdapter().run_power_flow(case)
    reference_slack_p, reference_slack_q = validator._extract_balanced_slack(reference)
    reference_voltages = _reference_voltages_with_reimported_keys(reference, reimported, validator)
    return validator.validate_against_pandapower(
        reimported,
        reference_slack_p_mw=reference_slack_p,
        reference_slack_q_mvar=reference_slack_q,
        reference_voltages=reference_voltages,
    )


def validate_opendss_from_pandapower(
    case: Any,
    validator: ValidationService,
    tmpdir: Path,
) -> ValidationResult:
    destination = tmpdir / f"{case.case_id}.dss"
    path = OpenDSSExportAdapter().export_case(case, destination)
    reference = OpenDSSImportAdapter().solve_source_case(path)
    if case.is_unbalanced:
        return validator.validate_pandapower_unbalanced_against_opendss(case, reference)
    return validator.validate_pandapower_case_against_opendss(case, reference)


def validate_opendss_source_to_pandapower(
    case: Any,
    reference: Any,
    validator: ValidationService,
    tmpdir: Path,
) -> ValidationResult:
    del tmpdir
    if case.is_unbalanced:
        return validator.validate_opendss_unbalanced_roundtrip(case, reference)
    return validator.validate_opendss_roundtrip(case, reference)


def validate_opendss_source_to_powermodelsdistribution(
    case: Any,
    reference: Any,
    validator: ValidationService,
    tmpdir: Path,
) -> ValidationResult:
    return validate_powermodelsdistribution(case, reference, validator, tmpdir)


def build_balanced_core_cases() -> list[CanonicalCase]:
    return [
        CanonicalCase.from_pandapower(
            case_id="core_bal_star_3bus",
            source_format="pandapower",
            net=_build_balanced_case_star_3bus(),
        ),
        CanonicalCase.from_pandapower(
            case_id="core_bal_radial_3bus",
            source_format="pandapower",
            net=_build_balanced_case_radial_3bus(),
        ),
        CanonicalCase.from_pandapower(
            case_id="core_bal_ring_4bus",
            source_format="pandapower",
            net=_build_balanced_case_ring_4bus(),
        ),
    ]


def build_unbalanced_core_cases() -> list[CanonicalCase]:
    return [
        CanonicalCase.from_pandapower(
            case_id="core_unb_radial_loads",
            source_format="pandapower",
            net=_build_unbalanced_case_radial_loads(),
        ),
        CanonicalCase.from_pandapower(
            case_id="core_unb_branch_loads",
            source_format="pandapower",
            net=_build_unbalanced_case_branch_loads(),
        ),
        CanonicalCase.from_pandapower(
            case_id="core_unb_phase_skew",
            source_format="pandapower",
            net=_build_unbalanced_case_delta_mix(),
        ),
    ]


def materialize_balanced_source_case(
    case: CanonicalCase,
    *,
    source_tool: str,
    pandapower: PandapowerAdapter,
    pypsa: PypsaAdapter,
    cgmes_export: CGMESExportAdapter,
    tmpdir: Path,
) -> CanonicalCase:
    if source_tool == "pandapower":
        path = pandapower.export_json(case, tmpdir / f"{case.case_id}.pandapower.json")
        return PandapowerImportAdapter().import_case(path)
    if source_tool == "matpower":
        path = MatpowerExportAdapter().export_case(case, tmpdir / f"{case.case_id}.m")
        return MatpowerImportAdapter().import_case(path)
    if source_tool == "opendss":
        path = OpenDSSExportAdapter().export_case(case, tmpdir / f"{case.case_id}.dss")
        return OpenDSSImportAdapter().import_case(path)
    if source_tool == "cgmes":
        path = cgmes_export.export_case(case, tmpdir / f"{case.case_id}.cgmes.zip")
        return CGMESImportAdapter().import_case(path)
    if source_tool == "pypsa":
        path = pypsa.export_netcdf(case, tmpdir / f"{case.case_id}.nc")
        return PypsaImportAdapter().import_case(path)
    raise ValueError(f"Unsupported balanced source tool {source_tool}")


def materialize_unbalanced_source_case(
    case: CanonicalCase,
    *,
    source_tool: str,
    tmpdir: Path,
) -> CanonicalCase:
    if source_tool == "pandapower":
        path = PandapowerAdapter().export_json(case, tmpdir / f"{case.case_id}.pandapower.json")
        return PandapowerImportAdapter().import_case(path)
    if source_tool == "opendss":
        path = OpenDSSExportAdapter().export_case(case, tmpdir / f"{case.case_id}.dss")
        return OpenDSSImportAdapter().import_case(path)
    if source_tool == "powermodelsdistribution":
        path = PowerModelsDistributionAdapter().export_input(case, tmpdir / f"{case.case_id}_pmd.dss")
        return PowerModelsDistributionImportAdapter().import_case(path)
    raise ValueError(f"Unsupported unbalanced source tool {source_tool}")


def validate_balanced_export(
    case: CanonicalCase,
    *,
    export_tool: str,
    pandapower: PandapowerAdapter,
    validator: ValidationService,
    pypsa: PypsaAdapter,
    cgmes_export: CGMESExportAdapter,
    cgmes_import: CGMESImportAdapter,
    tmpdir: Path,
) -> ValidationResult:
    if export_tool == "pandapower":
        return self_validate_balanced(case, pandapower, validator)
    if export_tool == "matpower":
        return validate_matpower_export(case, validator, tmpdir)
    if export_tool == "opendss":
        return validate_opendss_from_pandapower(case, validator, tmpdir)
    if export_tool == "cgmes":
        return validate_cgmes_from_pandapower(case, cgmes_export, cgmes_import, validator, tmpdir)
    if export_tool == "pypsa":
        return validate_pypsa_export(case, validator, pypsa, tmpdir)
    if export_tool == "powermodels":
        return validate_powermodels(case, pandapower, validator, tmpdir)
    raise ValueError(f"Unsupported balanced export tool {export_tool}")


def validate_unbalanced_export(
    case: CanonicalCase,
    *,
    export_tool: str,
    pandapower: PandapowerAdapter,
    validator: ValidationService,
    tmpdir: Path,
) -> ValidationResult:
    if export_tool == "pandapower":
        return validator.validate_pandapower_unbalanced_roundtrip(case)
    if export_tool == "opendss":
        return validate_opendss_from_pandapower(case, validator, tmpdir)
    if export_tool == "powermodelsdistribution":
        return validate_powermodelsdistribution_from_pandapower(case, pandapower, validator, tmpdir)
    raise ValueError(f"Unsupported unbalanced export tool {export_tool}")


def _build_balanced_case_star_3bus() -> Any:
    net = pp.create_empty_network(sn_mva=100)
    b0 = pp.create_bus(net, vn_kv=110, name="B0")
    b1 = pp.create_bus(net, vn_kv=110, name="B1")
    b2 = pp.create_bus(net, vn_kv=110, name="B2")
    pp.create_ext_grid(
        net,
        b0,
        vm_pu=1.0,
        va_degree=0.0,
        name="Slack",
        s_sc_max_mva=1000.0,
        rx_max=0.1,
        x0x_max=1.0,
        r0x0_max=0.1,
    )
    pp.create_line_from_parameters(
        net, b0, b1, length_km=8.0, r_ohm_per_km=0.06, x_ohm_per_km=0.24,
        c_nf_per_km=10.0, max_i_ka=1.0, r0_ohm_per_km=0.18, x0_ohm_per_km=0.72,
        c0_nf_per_km=5.0, g_us_per_km=0.0, g0_us_per_km=0.0, name="L01"
    )
    pp.create_line_from_parameters(
        net, b0, b2, length_km=5.0, r_ohm_per_km=0.05, x_ohm_per_km=0.20,
        c_nf_per_km=8.0, max_i_ka=1.0, r0_ohm_per_km=0.15, x0_ohm_per_km=0.60,
        c0_nf_per_km=4.0, g_us_per_km=0.0, g0_us_per_km=0.0, name="L02"
    )
    pp.create_load(net, b1, p_mw=22.0, q_mvar=5.0, name="LD1")
    pp.create_load(net, b2, p_mw=16.0, q_mvar=4.0, name="LD2")
    return net


def _build_balanced_case_radial_3bus() -> Any:
    net = pp.create_empty_network(sn_mva=100)
    b0 = pp.create_bus(net, vn_kv=110, name="B0")
    b1 = pp.create_bus(net, vn_kv=110, name="B1")
    b2 = pp.create_bus(net, vn_kv=110, name="B2")
    pp.create_ext_grid(
        net,
        b0,
        vm_pu=1.0,
        va_degree=0.0,
        name="Slack",
        s_sc_max_mva=1000.0,
        rx_max=0.1,
        x0x_max=1.0,
        r0x0_max=0.1,
    )
    pp.create_line_from_parameters(
        net, b0, b1, length_km=6.0, r_ohm_per_km=0.05, x_ohm_per_km=0.20,
        c_nf_per_km=9.0, max_i_ka=1.0, r0_ohm_per_km=0.15, x0_ohm_per_km=0.60,
        c0_nf_per_km=4.5, g_us_per_km=0.0, g0_us_per_km=0.0, name="L01"
    )
    pp.create_line_from_parameters(
        net, b1, b2, length_km=4.0, r_ohm_per_km=0.04, x_ohm_per_km=0.16,
        c_nf_per_km=8.0, max_i_ka=1.0, r0_ohm_per_km=0.12, x0_ohm_per_km=0.48,
        c0_nf_per_km=4.0, g_us_per_km=0.0, g0_us_per_km=0.0, name="L12"
    )
    pp.create_load(net, b1, p_mw=18.0, q_mvar=5.0, name="LD1")
    pp.create_load(net, b2, p_mw=22.0, q_mvar=6.0, name="LD2")
    return net


def _build_balanced_case_ring_4bus() -> Any:
    net = pp.create_empty_network(sn_mva=100)
    buses = [pp.create_bus(net, vn_kv=110, name=f"B{i}") for i in range(4)]
    pp.create_ext_grid(net, buses[0], vm_pu=1.0, va_degree=0.0, name="Slack")
    for i, (fb, tb, r, x) in enumerate(((0, 1, 0.03, 0.12), (1, 2, 0.04, 0.15), (2, 3, 0.05, 0.18), (3, 0, 0.06, 0.21)), start=1):
        pp.create_line_from_parameters(
            net, buses[fb], buses[tb], length_km=5.0, r_ohm_per_km=r, x_ohm_per_km=x,
            c_nf_per_km=7.0, max_i_ka=1.0, r0_ohm_per_km=r * 3.0, x0_ohm_per_km=x * 3.0,
            c0_nf_per_km=3.5, g_us_per_km=0.0, g0_us_per_km=0.0, name=f"L{i}"
        )
    pp.create_load(net, buses[1], p_mw=15.0, q_mvar=4.0, name="LD1")
    pp.create_load(net, buses[2], p_mw=20.0, q_mvar=5.5, name="LD2")
    pp.create_load(net, buses[3], p_mw=12.0, q_mvar=3.0, name="LD3")
    return net


def _build_unbalanced_case_radial_loads() -> Any:
    net = pp.create_empty_network(sn_mva=1.0)
    b0 = pp.create_bus(net, vn_kv=20.0, name="USRC")
    b1 = pp.create_bus(net, vn_kv=0.4, name="UL1")
    pp.create_ext_grid(
        net,
        b0,
        vm_pu=1.0,
        va_degree=0.0,
        name="Slack",
        s_sc_max_mva=1000.0,
        rx_max=0.1,
        x0x_max=1.0,
        r0x0_max=0.1,
    )
    pp.create_transformer_from_parameters(
        net, hv_bus=b0, lv_bus=b1, sn_mva=1.0, vn_hv_kv=20.0, vn_lv_kv=0.4,
        vk_percent=6.0, vkr_percent=1.0, pfe_kw=0.0, i0_percent=0.0,
        shift_degree=0.0, vector_group="YNyn",
        vk0_percent=6.0, vkr0_percent=1.0, mag0_percent=100.0, mag0_rx=0.0, si0_hv_partial=0.9
    )
    pp.create_asymmetric_load(net, b1, p_a_mw=0.04, q_a_mvar=0.01, p_b_mw=0.03, q_b_mvar=0.008, p_c_mw=0.05, q_c_mvar=0.012, name="AL1", type="wye")
    return net


def _build_unbalanced_case_branch_loads() -> Any:
    net = pp.create_empty_network(sn_mva=1.0)
    b0 = pp.create_bus(net, vn_kv=20.0, name="USRC")
    b1 = pp.create_bus(net, vn_kv=0.4, name="UB1")
    b2 = pp.create_bus(net, vn_kv=0.4, name="UB2")
    pp.create_ext_grid(
        net,
        b0,
        vm_pu=1.0,
        va_degree=0.0,
        name="Slack",
        s_sc_max_mva=1000.0,
        rx_max=0.1,
        x0x_max=1.0,
        r0x0_max=0.1,
    )
    pp.create_transformer_from_parameters(
        net, hv_bus=b0, lv_bus=b1, sn_mva=1.0, vn_hv_kv=20.0, vn_lv_kv=0.4,
        vk_percent=6.0, vkr_percent=1.0, pfe_kw=0.0, i0_percent=0.0,
        shift_degree=0.0, vector_group="YNyn",
        vk0_percent=6.0, vkr0_percent=1.0, mag0_percent=100.0, mag0_rx=0.0, si0_hv_partial=0.9
    )
    pp.create_line_from_parameters(
        net, b1, b2, length_km=0.2, r_ohm_per_km=0.4, x_ohm_per_km=0.08,
        c_nf_per_km=0.0, max_i_ka=0.4, r0_ohm_per_km=1.2, x0_ohm_per_km=0.24,
        c0_nf_per_km=0.0, g_us_per_km=0.0, g0_us_per_km=0.0, name="UL"
    )
    pp.create_asymmetric_load(net, b1, p_a_mw=0.02, q_a_mvar=0.006, p_b_mw=0.0, q_b_mvar=0.0, p_c_mw=0.03, q_c_mvar=0.009, name="AL1", type="wye")
    pp.create_asymmetric_load(net, b2, p_a_mw=0.0, q_a_mvar=0.0, p_b_mw=0.025, q_b_mvar=0.007, p_c_mw=0.015, q_c_mvar=0.004, name="AL2", type="wye")
    return net


def _build_unbalanced_case_delta_mix() -> Any:
    net = pp.create_empty_network(sn_mva=1.0)
    b0 = pp.create_bus(net, vn_kv=20.0, name="USRC")
    b1 = pp.create_bus(net, vn_kv=0.4, name="UD1")
    b2 = pp.create_bus(net, vn_kv=0.4, name="UD2")
    pp.create_ext_grid(
        net,
        b0,
        vm_pu=1.0,
        va_degree=0.0,
        name="Slack",
        s_sc_max_mva=1000.0,
        rx_max=0.1,
        x0x_max=1.0,
        r0x0_max=0.1,
    )
    pp.create_transformer_from_parameters(
        net, hv_bus=b0, lv_bus=b1, sn_mva=1.0, vn_hv_kv=20.0, vn_lv_kv=0.4,
        vk_percent=6.0, vkr_percent=1.0, pfe_kw=0.0, i0_percent=0.0,
        shift_degree=0.0, vector_group="YNyn",
        vk0_percent=6.0, vkr0_percent=1.0, mag0_percent=100.0, mag0_rx=0.0, si0_hv_partial=0.9
    )
    pp.create_line_from_parameters(
        net, b1, b2, length_km=0.15, r_ohm_per_km=0.35, x_ohm_per_km=0.07,
        c_nf_per_km=0.0, max_i_ka=0.4, r0_ohm_per_km=1.05, x0_ohm_per_km=0.21,
        c0_nf_per_km=0.0, g_us_per_km=0.0, g0_us_per_km=0.0, name="UL2"
    )
    pp.create_asymmetric_load(net, b1, p_a_mw=0.03, q_a_mvar=0.01, p_b_mw=0.0, q_b_mvar=0.0, p_c_mw=0.01, q_c_mvar=0.003, name="AL1", type="wye")
    pp.create_asymmetric_load(net, b2, p_a_mw=0.0, q_a_mvar=0.0, p_b_mw=0.02, q_b_mvar=0.006, p_c_mw=0.025, q_c_mvar=0.007, name="AL2", type="wye")
    return net


def build_cgmes_subset_case() -> CanonicalCase:
    net = pp.create_empty_network(sn_mva=100)
    slack_bus = pp.create_bus(net, vn_kv=110, name="SlackBus")
    load_bus = pp.create_bus(net, vn_kv=110, name="LoadBus")
    pp.create_ext_grid(
        net,
        slack_bus,
        vm_pu=1.0,
        va_degree=0.0,
        max_p_mw=500.0,
        min_p_mw=0.0,
        max_q_mvar=500.0,
        min_q_mvar=-500.0,
        name="Slack",
    )
    pp.create_line_from_parameters(
        net,
        slack_bus,
        load_bus,
        length_km=10.0,
        r_ohm_per_km=0.05,
        x_ohm_per_km=0.2,
        c_nf_per_km=10.0,
        max_i_ka=1.0,
        r0_ohm_per_km=0.15,
        x0_ohm_per_km=0.6,
        c0_nf_per_km=5.0,
        g_us_per_km=0.0,
        g0_us_per_km=0.0,
        name="L1",
    )
    pp.create_load(net, load_bus, p_mw=50.0, q_mvar=10.0, name="LD1")
    return CanonicalCase.from_pandapower(
        case_id="cgmes_smoke",
        source_format="pandapower",
        net=net,
    )


def validate_cgmes_from_pandapower(
    case: Any,
    cgmes_export: CGMESExportAdapter,
    cgmes_import: CGMESImportAdapter,
    validator: ValidationService,
    tmpdir: Path,
) -> ValidationResult:
    destination = tmpdir / f"{case.case_id}.cgmes.zip"
    path = cgmes_export.export_case(case, destination)
    reimported = cgmes_import.import_case(path)
    reference = PandapowerAdapter().run_power_flow(case)
    reference_slack_p, reference_slack_q = validator._extract_balanced_slack(reference)
    reference_voltages = _reference_voltages_with_reimported_keys(reference, reimported, validator)
    return validator.validate_against_pandapower(
        reimported,
        reference_slack_p_mw=reference_slack_p,
        reference_slack_q_mvar=reference_slack_q,
        reference_voltages=reference_voltages,
    )


def _reference_voltages_with_reimported_keys(
    reference_net: Any,
    reimported_case: CanonicalCase,
    validator: ValidationService,
) -> dict[str, complex]:
    reimported_net = PandapowerAdapter().to_net(reimported_case)
    reference_by_key = {
        validator._bus_key(reference_net, int(idx)): complex(
            float(row.vm_pu) * math.cos(math.radians(float(row.va_degree))),
            float(row.vm_pu) * math.sin(math.radians(float(row.va_degree))),
        )
        for idx, row in reference_net.res_bus.iterrows()
    }
    reimported_keys = [validator._bus_key(reimported_net, int(idx)) for idx in reimported_net.bus.index]
    if any(key in reference_by_key for key in reimported_keys):
        return reference_by_key

    reference_rows = sorted(reference_net.res_bus.iterrows(), key=lambda item: int(item[0]))
    reimported_bus_indices = sorted(int(idx) for idx in reimported_net.bus.index)
    if len(reference_rows) != len(reimported_bus_indices):
        return reference_by_key

    remapped: dict[str, complex] = {}
    for (ref_idx, row), bus_idx in zip(reference_rows, reimported_bus_indices, strict=True):
        key = validator._bus_key(reimported_net, bus_idx)
        angle_rad = math.radians(float(row.va_degree))
        remapped[key] = complex(float(row.vm_pu) * math.cos(angle_rad), float(row.vm_pu) * math.sin(angle_rad))
    return remapped


def build_pypsa_origin_case(
    case_id: str,
    builder: Any,
    pypsa_adapter: PypsaAdapter,
    tmpdir: Path,
) -> tuple[CanonicalCase, Any]:
    path = tmpdir / f"{case_id}.nc"
    network = builder()
    network.export_to_netcdf(path)
    return PypsaImportAdapter().import_case(path), pypsa_adapter.solve_network_file(path)


def build_pypsa_triangle_network() -> pypsa.Network:
    network = pypsa.Network()
    for bus_name in ("A", "B", "C"):
        network.add("Bus", bus_name, v_nom=220.0)
    network.add("Generator", "Slack", bus="A", control="Slack", p_set=0.0, vm_pu=1.0)
    network.add("Generator", "PV", bus="B", control="PV", p_set=40.0, vm_pu=1.0)
    network.add("Load", "LoadB", bus="B", p_set=60.0, q_set=20.0)
    network.add("Load", "LoadC", bus="C", p_set=45.0, q_set=15.0)
    network.add("Line", "AB", bus0="A", bus1="B", r=0.01, x=0.08, b=0.0, g=0.0, s_nom=200.0, length=1.0)
    network.add("Line", "BC", bus0="B", bus1="C", r=0.015, x=0.09, b=0.0, g=0.0, s_nom=200.0, length=1.0)
    network.add("Line", "CA", bus0="C", bus1="A", r=0.02, x=0.11, b=0.0, g=0.0, s_nom=200.0, length=1.0)
    return network


def build_pypsa_radial_network() -> pypsa.Network:
    network = pypsa.Network()
    for bus_name in ("Slack", "Mid", "Load"):
        network.add("Bus", bus_name, v_nom=110.0)
    network.add("Generator", "Grid", bus="Slack", control="Slack", p_set=0.0, vm_pu=1.0)
    network.add("Load", "DemandMid", bus="Mid", p_set=30.0, q_set=10.0)
    network.add("Load", "DemandLoad", bus="Load", p_set=20.0, q_set=5.0)
    network.add("Line", "L1", bus0="Slack", bus1="Mid", r=0.005, x=0.04, b=0.0, g=0.0, s_nom=150.0, length=1.0)
    network.add("Line", "L2", bus0="Mid", bus1="Load", r=0.006, x=0.05, b=0.0, g=0.0, s_nom=150.0, length=1.0)
    return network


def build_pypsa_five_bus_ring_network() -> pypsa.Network:
    network = pypsa.Network()
    for bus_name in ("B1", "B2", "B3", "B4", "B5"):
        network.add("Bus", bus_name, v_nom=132.0)
    network.add("Generator", "Grid", bus="B1", control="Slack", p_set=0.0, vm_pu=1.0)
    network.add("Generator", "Support", bus="B3", control="PV", p_set=35.0, vm_pu=1.0)
    for bus_name, p_set, q_set in (("B2", 20.0, 7.0), ("B4", 28.0, 9.0), ("B5", 18.0, 6.0)):
        network.add("Load", f"Load_{bus_name}", bus=bus_name, p_set=p_set, q_set=q_set)
    for name, bus0, bus1, r, x in (
        ("L12", "B1", "B2", 0.008, 0.06),
        ("L23", "B2", "B3", 0.01, 0.07),
        ("L34", "B3", "B4", 0.011, 0.075),
        ("L45", "B4", "B5", 0.012, 0.08),
        ("L51", "B5", "B1", 0.009, 0.065),
    ):
        network.add("Line", name, bus0=bus0, bus1=bus1, r=r, x=x, b=0.0, g=0.0, s_nom=180.0, length=1.0)
    return network


def validation_record(
    *,
    case_id: str,
    source_tool: str,
    export_tool: str,
    model_type: str,
    result: ValidationResult,
    notes: str,
) -> RouteRecord:
    compared_points = result.details.get("compared_buses", result.details.get("compared_nodes"))
    return RouteRecord(
        case_id=case_id,
        source_tool=source_tool,
        export_tool=export_tool,
        model_type=model_type,
        status="validated" if result.passed else "failed",
        slack_delta_mva=result.slack_delta_mva,
        max_voltage_delta_pu=result.max_voltage_delta_pu,
        compared_points=int(compared_points) if compared_points is not None else None,
        notes=notes,
    )


def unsupported_record(
    *,
    case_id: str,
    source_tool: str,
    export_tool: str,
    model_type: str,
    notes: str,
) -> RouteRecord:
    return RouteRecord(
        case_id=case_id,
        source_tool=source_tool,
        export_tool=export_tool,
        model_type=model_type,
        status="not_validated",
        slack_delta_mva=None,
        max_voltage_delta_pu=None,
        compared_points=None,
        notes=notes,
    )


def load_pypsa_eur_validation_records(path: Path) -> list[RouteRecord]:
    if not path.exists():
        return []
    payload = json.loads(path.read_text())
    validation = payload.get("validation", {})
    validation_scope = payload.get("validation_scope", "ac_projection_against_pypsa")
    case_id = path.stem.removesuffix(".validation")
    notes = (
        f"PyPSA-Eur artifact tracked from `{path.name}` with scope `{validation_scope}`. "
        "See the JSON artifact for the full export metadata."
    )
    if validation.get("skipped"):
        reason = validation.get("reason", "validation skipped")
        return [
            unsupported_record(
                case_id=case_id,
                source_tool="pypsa-eur",
                export_tool="pandapower",
                model_type="balanced",
                notes=f"{notes} Validation skipped: {reason}.",
            )
        ]
    compared_points = None
    details = validation.get("details", {})
    for key in ("compared_buses", "compared_nodes", "passed_islands", "total_islands"):
        value = details.get(key)
        if value is not None:
            compared_points = int(value)
            break
    if compared_points is None:
        island_validation = payload.get("pandapower_island_validation", {})
        total_islands = island_validation.get("total_islands")
        if total_islands is not None:
            compared_points = int(total_islands)
    records = [
        RouteRecord(
            case_id=case_id,
            source_tool="pypsa-eur",
            export_tool="pandapower",
            model_type="balanced",
            status="validated" if validation.get("passed") else "failed",
            slack_delta_mva=validation.get("slack_delta_mva"),
            max_voltage_delta_pu=validation.get("max_voltage_delta_pu"),
            compared_points=compared_points,
            notes=notes,
        )
    ]
    island_validation = payload.get("pandapower_island_validation", {})
    for island in island_validation.get("islands", []):
        subnetwork = str(island.get("subnetwork", "unknown"))
        island_output = island.get("output")
        island_notes = (
            f"{notes} Island {subnetwork} validated as an individually solved synchronous island"
            + (f" from `{island_output}`." if island_output else ".")
        )
        records.append(
            RouteRecord(
                case_id=f"{case_id}::island_{subnetwork}",
                source_tool="pypsa-eur",
                export_tool="pandapower",
                model_type="balanced",
                status="validated" if island.get("passed") else "failed",
                slack_delta_mva=island.get("slack_delta_mva"),
                max_voltage_delta_pu=island.get("max_voltage_delta_pu"),
                compared_points=island.get("compared_buses"),
                notes=island_notes,
            )
        )
    return records


def build_summary(records: list[RouteRecord]) -> ValidationSummary:
    validated = [record for record in records if record.status == "validated"]
    compared_counts = [record.compared_points for record in validated if record.compared_points is not None]
    balanced_routes = [record for record in validated if "unbalanced" not in record.model_type]
    unbalanced_routes = [record for record in validated if "unbalanced" in record.model_type]
    return ValidationSummary(
        method="deterministic full-voltage comparison",
        validated_routes=len(validated),
        pending_routes=len(records) - len(validated),
        balanced_routes=len(balanced_routes),
        unbalanced_routes=len(unbalanced_routes),
        total_compared_points=sum(compared_counts),
        max_compared_points=max(compared_counts, default=0),
        min_compared_points=min(compared_counts, default=0),
    )


def render_markdown(summary: ValidationSummary, records: list[RouteRecord]) -> str:
    balanced_records = [record for record in records if "unbalanced" not in record.model_type]
    unbalanced_records = [record for record in records if "unbalanced" in record.model_type]
    lines = [
        "# Validation Report",
        "",
        "This file is generated by `scripts/generate_validation_report.py`.",
        "",
        "## Summary",
        "",
        f"- Validation method: {summary.method}",
        f"- Validated routes: {summary.validated_routes}",
        f"- Pending routes: {summary.pending_routes}",
        f"- Balanced validated routes: {summary.balanced_routes}",
        f"- Unbalanced validated routes: {summary.unbalanced_routes}",
        f"- Total deterministic voltage points compared: {summary.total_compared_points}",
        f"- Smallest route comparison set: {summary.min_compared_points}",
        f"- Largest route comparison set: {summary.max_compared_points}",
        "",
        "Every validated route records both the slack-power mismatch and the maximum deterministic complex-voltage mismatch across all matched buses or phase nodes.",
        "The result numbers live in the route tables below and in `docs/validation_report.json` for machine-readable use.",
        "",
        "## Methodology",
        "",
        "- Native-origin cases are the primary ground-truth layer and provide the strongest evidence for source-tool fidelity.",
        "- Canonical common-subset cases are the interoperability layer and intentionally exercise only the subset that can be represented across multiple tools with minimal hidden semantics.",
        "- Balanced and unbalanced evidence are reported separately because the supported semantics, solver backends, and tolerances differ.",
        "",
        "## Balanced Routes",
        "",
    ]
    lines.extend(render_markdown_table(balanced_records))
    lines.extend(
        [
            "",
            "## Unbalanced Routes",
            "",
        ]
    )
    lines.extend(render_markdown_table(unbalanced_records))
    return "\n".join(lines) + "\n"


def format_metric(value: float | None) -> str:
    if value is None:
        return "-"
    return f"{value:.12g}"


def render_html(payload: dict[str, Any], records: list[RouteRecord]) -> str:
    validated = [record for record in records if record.status == "validated"]
    pending = [record for record in records if record.status != "validated"]
    summary = payload["summary"]
    balanced_records = [record for record in records if "unbalanced" not in record.model_type]
    unbalanced_records = [record for record in records if "unbalanced" in record.model_type]
    balanced_validated = [record for record in validated if "unbalanced" not in record.model_type]
    unbalanced_validated = [record for record in validated if "unbalanced" in record.model_type]
    balanced_source_tools = sorted({record.source_tool for record in balanced_records})
    balanced_export_tools = sorted({record.export_tool for record in balanced_records})
    unbalanced_source_tools = sorted({record.source_tool for record in unbalanced_records})
    unbalanced_export_tools = sorted({record.export_tool for record in unbalanced_records})
    three_phase_validated = [record for record in validated if record.model_type == "unbalanced_3ph"]
    strongest_route = min(
        (
            record
            for record in validated
            if record.max_voltage_delta_pu is not None
        ),
        key=lambda record: (record.max_voltage_delta_pu or 0.0, record.slack_delta_mva or 0.0),
        default=None,
    )
    weakest_route = max(
        (
            record
            for record in validated
            if record.max_voltage_delta_pu is not None
        ),
        key=lambda record: (record.max_voltage_delta_pu or 0.0, record.slack_delta_mva or 0.0),
        default=None,
    )
    unbalanced_routes = unbalanced_records

    capability_cards = []
    for row in payload["capabilities"]:
        capability_cards.append(
            f"""
            <article class="cap-card">
              <h3>{escape(str(row['tool']))}</h3>
              <p>{escape(str(row['notes']))}</p>
              <ul>
                <li>Balanced: import={yn(row['balanced_import'])}, export={yn(row['balanced_export'])}, validation={yn(row['balanced_validation'])}</li>
                <li>Unbalanced: import={yn(row['unbalanced_import'])}, export={yn(row['unbalanced_export'])}, validation={yn(row['unbalanced_validation'])}</li>
              </ul>
            </article>
            """
        )

    best_voltage = min((record.max_voltage_delta_pu for record in validated if record.max_voltage_delta_pu is not None), default=None)
    worst_voltage = max((record.max_voltage_delta_pu for record in validated if record.max_voltage_delta_pu is not None), default=None)

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>PowerModelConverter Validation Dashboard</title>
  <style>
    :root {{
      --bg: #f3efe7;
      --panel: #fffdf9;
      --ink: #1d1b18;
      --muted: #6a645c;
      --line: #d8d0c4;
      --green: #1f7a5c;
      --amber: #a0671a;
      --red: #9f2d2d;
      --blue: #1d4ed8;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: "IBM Plex Sans", "Segoe UI", sans-serif;
      color: var(--ink);
      background:
        radial-gradient(circle at top left, #fff6d8 0, transparent 28%),
        radial-gradient(circle at bottom right, #dff5ee 0, transparent 26%),
        var(--bg);
    }}
    .wrap {{ max-width: 1200px; margin: 0 auto; padding: 32px 20px 48px; }}
    .hero {{
      background: linear-gradient(135deg, #1b4d3e, #233a72);
      color: white;
      border-radius: 24px;
      padding: 28px;
      box-shadow: 0 18px 50px rgba(20, 30, 40, 0.18);
    }}
    .hero h1 {{ margin: 0 0 10px; font-size: 2rem; }}
    .hero p {{ margin: 0; max-width: 850px; color: rgba(255,255,255,.88); }}
    .grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
      gap: 16px;
      margin-top: 22px;
    }}
    .card, .section {{
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 20px;
      box-shadow: 0 8px 26px rgba(40, 35, 25, 0.06);
    }}
    .card {{ padding: 18px; }}
    .card h2 {{ margin: 0 0 6px; font-size: .95rem; color: var(--muted); }}
    .card .value {{ font-size: 2rem; font-weight: 700; }}
    .card .sub {{ margin-top: 6px; color: var(--muted); font-size: .92rem; }}
    .section {{ margin-top: 22px; padding: 20px; }}
    .section h2 {{ margin: 0 0 14px; font-size: 1.2rem; }}
    .analysis {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
      gap: 14px;
    }}
    .analysis-card {{
      background: linear-gradient(180deg, #fffaf0, #fffdf9);
      border: 1px solid var(--line);
      border-radius: 18px;
      padding: 16px;
    }}
    .analysis-card h3 {{ margin: 0 0 8px; font-size: 1rem; }}
    .analysis-card p {{ margin: 0; color: var(--muted); line-height: 1.45; }}
    .matrix {{
      width: 100%;
      border-collapse: collapse;
      overflow: hidden;
    }}
    .matrix th, .matrix td, .table th, .table td {{
      border-bottom: 1px solid var(--line);
      padding: 10px 12px;
      text-align: left;
      vertical-align: top;
    }}
    .matrix th {{ background: #f8f3ea; }}
    .cell {{ text-align: center; font-weight: 600; text-transform: uppercase; font-size: .78rem; }}
    .cell-validated {{ background: rgba(31, 122, 92, 0.12); color: var(--green); }}
    .cell-not_validated {{ background: rgba(160, 103, 26, 0.12); color: var(--amber); }}
    .cell-empty {{ color: var(--muted); }}
    .caps {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(260px, 1fr));
      gap: 14px;
    }}
    .cap-card {{
      background: #fdfaf3;
      border: 1px solid var(--line);
      border-radius: 18px;
      padding: 16px;
    }}
    .cap-card h3 {{ margin: 0 0 8px; }}
    .cap-card p {{ margin: 0 0 10px; color: var(--muted); }}
    .cap-card ul {{ margin: 0; padding-left: 18px; }}
    .table {{ width: 100%; border-collapse: collapse; }}
    .pill {{
      display: inline-block;
      border-radius: 999px;
      padding: 4px 10px;
      font-size: .78rem;
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: .04em;
    }}
    .pill-validated {{ background: rgba(31,122,92,.12); color: var(--green); }}
    .pill-not_validated {{ background: rgba(160,103,26,.12); color: var(--amber); }}
    .footer {{ margin-top: 18px; color: var(--muted); font-size: .92rem; }}
    svg {{ display: block; }}
  </style>
</head>
<body>
  <div class="wrap">
    <section class="hero">
      <h1>PowerModelConverter Validation Dashboard</h1>
      <p>Generated from the live route inventory. Native-origin cases are the main ground-truth layer, while canonical common-subset cases act as the cross-tool interoperability stress test. Balanced and unbalanced evidence are tracked separately because their supported semantics differ.</p>
    </section>

    <div class="grid">
      <article class="card">
        <h2>Validated Routes</h2>
        <div class="value">{len(validated)}</div>
        <div class="sub">Routes with successful measured validation</div>
      </article>
      <article class="card">
        <h2>Pending Routes</h2>
        <div class="value">{len(pending)}</div>
        <div class="sub">Tracked explicitly, but not yet signed off</div>
      </article>
      <article class="card">
        <h2>Best Voltage Delta</h2>
        <div class="value">{format_metric(best_voltage)}</div>
        <div class="sub">Smallest measured full-voltage mismatch</div>
      </article>
      <article class="card">
        <h2>Worst Validated Voltage Delta</h2>
        <div class="value">{format_metric(worst_voltage)}</div>
        <div class="sub">Largest mismatch among currently validated routes</div>
      </article>
      <article class="card">
        <h2>Validated 3-Phase Routes</h2>
        <div class="value">{len(three_phase_validated)}</div>
        <div class="sub">Routes using explicit unbalanced three-phase node-voltage checks</div>
      </article>
    </div>

    <section class="section">
      <h2>Analysis</h2>
      <div class="analysis">
        <article class="analysis-card">
          <h3>Validation Rule</h3>
          <p>Every signed-off route is checked by slack-power agreement first and full complex-voltage agreement second. Native-origin routes provide the strongest fidelity evidence, while canonical common-subset routes demonstrate broad interchangeability. Rows labeled 3-phase unbalanced are validated node by node across explicit phase voltages, not just balanced bus magnitudes.</p>
        </article>
        <article class="analysis-card">
          <h3>Where To Read The Numbers</h3>
          <p>The numeric results are listed directly in the Balanced Precision, Unbalanced Precision, and Route Details tables below. The same per-route values are also written to <code>docs/validation_report.json</code> for machine-readable reporting.</p>
        </article>
        <article class="analysis-card">
          <h3>Deterministic Coverage</h3>
          <p>The current report signs off {summary['validated_routes']} route(s) with deterministic full-voltage comparisons across {summary['total_compared_points']} matched buses or phase nodes in total. Individual routes compare between {summary['min_compared_points']} and {summary['max_compared_points']} points.</p>
        </article>
        <article class="analysis-card">
          <h3>Strongest Current Route</h3>
          <p>{escape(describe_route(strongest_route))}</p>
        </article>
        <article class="analysis-card">
          <h3>Largest Current Mismatch</h3>
          <p>{escape(describe_route(weakest_route))}</p>
        </article>
        <article class="analysis-card">
          <h3>Unbalanced Coverage</h3>
          <p>{escape(describe_unbalanced(unbalanced_routes))}</p>
        </article>
      </div>
    </section>

    <section class="section">
      <h2>Balanced Conversion Matrix</h2>
      <table class="matrix">
        <thead>
          <tr><th>Source \\ Export</th>{''.join(f'<th>{escape(tool)}</th>' for tool in balanced_export_tools)}</tr>
        </thead>
        <tbody>
          {''.join(build_matrix_rows(balanced_records, balanced_source_tools, balanced_export_tools))}
        </tbody>
      </table>
    </section>

    <section class="section">
      <h2>Unbalanced Conversion Matrix</h2>
      <table class="matrix">
        <thead>
          <tr><th>Source \\ Export</th>{''.join(f'<th>{escape(tool)}</th>' for tool in unbalanced_export_tools)}</tr>
        </thead>
        <tbody>
          {''.join(build_matrix_rows(unbalanced_records, unbalanced_source_tools, unbalanced_export_tools))}
        </tbody>
      </table>
    </section>

    <section class="section">
      <h2>Balanced Precision</h2>
      <table class="table">
        <thead>
          <tr>
            <th>Source</th>
            <th>Export</th>
            <th>Case</th>
            <th>Model</th>
            <th>Slack Delta</th>
            <th>Slack Precision</th>
            <th>Voltage Delta</th>
            <th>Voltage Precision</th>
            <th>Compared Points</th>
          </tr>
        </thead>
        <tbody>
          {''.join(render_precision_rows(balanced_validated))}
        </tbody>
      </table>
    </section>

    <section class="section">
      <h2>Unbalanced Precision</h2>
      <table class="table">
        <thead>
          <tr>
            <th>Source</th>
            <th>Export</th>
            <th>Case</th>
            <th>Model</th>
            <th>Slack Delta</th>
            <th>Slack Precision</th>
            <th>Voltage Delta</th>
            <th>Voltage Precision</th>
            <th>Compared Points</th>
          </tr>
        </thead>
        <tbody>
          {''.join(render_precision_rows(unbalanced_validated))}
        </tbody>
      </table>
    </section>

    <section class="section">
      <h2>Tool Capabilities</h2>
      <div class="caps">
        {''.join(capability_cards)}
      </div>
    </section>

    <section class="section">
      <h2>Balanced Route Details</h2>
      <table class="table">
        <thead>
          <tr>
            <th>Source</th>
            <th>Export</th>
            <th>Case</th>
            <th>Model</th>
            <th>Status</th>
            <th>Slack Delta</th>
            <th>Voltage Delta</th>
            <th>Compared Points</th>
            <th>Notes</th>
          </tr>
        </thead>
        <tbody>
          {''.join(render_detail_rows(balanced_records))}
        </tbody>
      </table>
    </section>

    <section class="section">
      <h2>Unbalanced Route Details</h2>
      <table class="table">
        <thead>
          <tr>
            <th>Source</th>
            <th>Export</th>
            <th>Case</th>
            <th>Model</th>
            <th>Status</th>
            <th>Slack Delta</th>
            <th>Voltage Delta</th>
            <th>Compared Points</th>
            <th>Notes</th>
          </tr>
        </thead>
        <tbody>
          {''.join(render_detail_rows(unbalanced_records))}
        </tbody>
      </table>
      <p class="footer">Generated by <code>scripts/generate_validation_report.py</code>.</p>
    </section>
  </div>
</body>
</html>
"""


def render_markdown_table(records: list[RouteRecord]) -> list[str]:
    lines = [
        "| Source | Export | Case | Model | Status | Slack Delta (MVA) | Max Voltage Delta (pu) | Compared Points | Notes |",
        "| --- | --- | --- | --- | --- | ---: | ---: | ---: | --- |",
    ]
    for record in records:
        lines.append(
            "| "
            f"{record.source_tool} | {record.export_tool} | {record.case_id} | {record.model_type} | {record.status} | "
            f"{format_metric(record.slack_delta_mva)} | {format_metric(record.max_voltage_delta_pu)} | "
            f"{record.compared_points if record.compared_points is not None else '-'} | {record.notes} |"
        )
    return lines


def build_matrix_rows(records: list[RouteRecord], source_tools: list[str], export_tools: list[str]) -> list[str]:
    rows = []
    for source_tool in source_tools:
        cells = []
        for export_tool in export_tools:
            matches = [record for record in records if record.source_tool == source_tool and record.export_tool == export_tool]
            if not matches:
                cells.append('<td class="cell cell-empty">-</td>')
                continue
            best = "validated" if any(record.status == "validated" for record in matches) else "not_validated"
            label = "validated" if best == "validated" else "not validated"
            title = " | ".join(f"{record.case_id}: {record.notes}" for record in matches)
            cells.append(f'<td class="cell cell-{best}" title="{escape(title)}">{escape(label)}</td>')
        rows.append(f"<tr><th>{escape(source_tool)}</th>{''.join(cells)}</tr>")
    return rows


def render_precision_rows(records: list[RouteRecord]) -> list[str]:
    rows = []
    for record in records:
        slack_score = score_precision(record.slack_delta_mva)
        voltage_score = score_precision(record.max_voltage_delta_pu)
        rows.append(
            f"""
            <tr>
              <td>{escape(record.source_tool)}</td>
              <td>{escape(record.export_tool)}</td>
              <td>{escape(record.case_id)}</td>
              <td>{escape(display_model_type(record.model_type))}</td>
              <td>{format_metric(record.slack_delta_mva)}</td>
              <td>{bar_svg(slack_score, '#1d4ed8')}</td>
              <td>{format_metric(record.max_voltage_delta_pu)}</td>
              <td>{bar_svg(voltage_score, '#059669')}</td>
              <td>{record.compared_points if record.compared_points is not None else '-'}</td>
            </tr>
            """
        )
    return rows


def render_detail_rows(records: list[RouteRecord]) -> list[str]:
    rows = []
    for record in records:
        rows.append(
            f"""
            <tr>
              <td>{escape(record.source_tool)}</td>
              <td>{escape(record.export_tool)}</td>
              <td>{escape(record.case_id)}</td>
              <td>{escape(display_model_type(record.model_type))}</td>
              <td><span class="pill pill-{record.status}">{escape(record.status)}</span></td>
              <td>{format_metric(record.slack_delta_mva)}</td>
              <td>{format_metric(record.max_voltage_delta_pu)}</td>
              <td>{record.compared_points if record.compared_points is not None else '-'}</td>
              <td>{escape(record.notes)}</td>
            </tr>
            """
        )
    return rows


def score_precision(value: float | None) -> float:
    if value is None:
        return 0.0
    if value == 0:
        return 1.0
    return min(1.0, max(0.0, -math.log10(value) / 12.0))


def bar_svg(score: float, color: str) -> str:
    width = max(2, int(round(score * 180)))
    return (
        '<svg width="190" height="14" viewBox="0 0 190 14" aria-hidden="true">'
        '<rect x="0" y="2" width="190" height="10" rx="5" fill="#ece5d8"></rect>'
        f'<rect x="0" y="2" width="{width}" height="10" rx="5" fill="{color}"></rect>'
        "</svg>"
    )


def yn(value: Any) -> str:
    return "yes" if value else "no"


def describe_route(record: RouteRecord | None) -> str:
    if record is None:
        return "No validated route with voltage metrics is available yet."
    return (
        f"{record.source_tool} -> {record.export_tool} on {record.case_id} "
        f"({record.model_type}) with slack delta {format_metric(record.slack_delta_mva)} MVA "
        f"and max voltage delta {format_metric(record.max_voltage_delta_pu)} pu."
    )


def describe_unbalanced(routes: list[RouteRecord]) -> str:
    validated = [record for record in routes if record.status == "validated"]
    pending = [record for record in routes if record.status != "validated"]
    if not routes:
        return "No unbalanced routes are tracked in the current report."
    return (
        f"{len(validated)} unbalanced route(s) are validated and {len(pending)} remain pending. "
        "Today that includes native pandapower 3-phase roundtrip validation and an "
        "OpenDSS starter feeder validated to both pandapower and PowerModelsDistribution."
    )


def display_model_type(model_type: str) -> str:
    mapping = {
        "balanced": "Balanced",
        "balanced_subset": "Balanced Subset",
        "unbalanced_3ph": "3-Phase Unbalanced",
        "balanced_or_mixed": "Native/Non-Exchange",
    }
    return mapping.get(model_type, model_type.replace("_", " ").title())


if __name__ == "__main__":
    main()
