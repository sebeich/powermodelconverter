#!/usr/bin/env python3
"""
UC5 research-grade reproducer: unbalanced distribution model authored in a simulation-only
ecosystem, carried to three-way cross-validation and unbalanced AC-OPF.

The IEEE European Low Voltage Test Feeder (asymmetric, 907 buses) is imported through pmc,
cross-validated phase-by-phase across three independent backends (pandapower `runpp_3ph`,
OpenDSS, PowerModelsDistribution), and then dispatched as an unbalanced AC-OPF with
single-phase PV generators and per-phase voltage limits in PowerModelsDistribution — an
optimization capability that does not exist in the ecosystems the model came from.

Citations:
- IEEE PES Distribution Test Feeder Working Group, "IEEE European Low Voltage Test Feeder".
- Fobes, D.; Claeys, S.; Geth, F.; Coffrin, C. "PowerModelsDistribution.jl: An open-source
  framework for exploring distribution power flow formulations." EPSR, 2020.
- Thurner, L. et al. "pandapower." IEEE TPWRS, 2018 (three-phase power flow `runpp_3ph`).
- Dugan, R.; McDermott, T. "An open source platform for collaborating on smart grid
  research." IEEE PES General Meeting, 2011 (OpenDSS).
"""

from __future__ import annotations

import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time

CURRENT_DIR = Path(__file__).resolve().parent
EXAMPLES_ROOT = CURRENT_DIR.parent
if str(EXAMPLES_ROOT) not in sys.path:
    sys.path.insert(0, str(EXAMPLES_ROOT))

from _common import REPO_ROOT
from _common import ensure_dir
from _common import json_ready
from _common import write_csv
from _common import write_json

from powermodelconverter.core.pandapower_backend import PandapowerAdapter
from powermodelconverter.exporters.opendss import OpenDSSExportAdapter
from powermodelconverter.exporters.powermodels_distribution import PowerModelsDistributionAdapter
from powermodelconverter.importers.opendss import OpenDSSImportAdapter
from powermodelconverter.importers.pandapower_json import PandapowerImportAdapter
from powermodelconverter.validation.engine import ValidationService


SOURCE_JSON = REPO_ROOT / "validation_cases" / "native" / "pandapower" / "ieee_european_lv_asymmetric.json"
CASE_ID = "ieee_european_lv_asymmetric"

PV_EVERY_NTH_LOAD = 1
PV_UNIT_KW = 5.0
PV_VM_UB_PU = 1.10


def detect_julia_binary() -> str:
    return os.environ.get("PMC_JULIA_BINARY") or os.environ.get("JULIA_BIN") or "julia"


def julia_depot() -> Path:
    return Path(os.environ.get("JULIA_DEPOT_PATH", str(REPO_ROOT / ".julia_depot")))


def build_pv_scenario_dss(pmd_dss: Path, destination: Path) -> dict[str, object]:
    """Append single-phase PV generators at every n-th load bus of the exported DSS model.

    Reusing the exporter's own load definitions (bus, phase, voltage base) avoids any
    guessing about naming conventions; the PV scenario is therefore a deterministic
    function of the validated export.
    """
    text = pmd_dss.read_text()
    load_pattern = re.compile(
        r"^New Load\.(?P<name>\S+)\s.*?bus1=(?P<bus>\S+)\s.*?kv=(?P<kv>[0-9.]+)",
        re.IGNORECASE | re.MULTILINE,
    )
    loads = sorted(load_pattern.finditer(text), key=lambda match: match.group("name"))
    if not loads:
        raise RuntimeError(f"No load definitions found in {pmd_dss}")

    selected = loads[::PV_EVERY_NTH_LOAD]
    generator_lines = ["", f"! UC5 PV scenario: {PV_UNIT_KW} kW single-phase PV at every {PV_EVERY_NTH_LOAD}. load bus"]
    for position, match in enumerate(selected):
        generator_lines.append(
            f"New Generator.pv_{position} phases=1 bus1={match.group('bus')} kv={match.group('kv')} "
            f"kw={PV_UNIT_KW} kvar=0 model=1"
        )

    solve_anchor = "\nSolve"
    if solve_anchor in text:
        text = text.replace(solve_anchor, "\n" + "\n".join(generator_lines) + "\nSolve", 1)
    else:
        text = text + "\n".join(generator_lines) + "\n"
    destination.write_text(text)
    return {
        "dss_path": destination,
        "pv_count": len(selected),
        "pv_unit_kw": PV_UNIT_KW,
        "pv_total_kw": PV_UNIT_KW * len(selected),
        "load_count": len(loads),
    }


def run_mc_opf(scenario_dss: Path, formulation: str) -> dict[str, object]:
    script = CURRENT_DIR / "mc_opf.jl"
    project = REPO_ROOT / "src" / "powermodelconverter" / "julia_pmd"
    env = {
        **os.environ,
        "JULIA_PROJECT": str(project),
        "JULIA_DEPOT_PATH": str(julia_depot()),
    }
    started = time.time()
    completed = subprocess.run(
        [detect_julia_binary(), str(script), str(scenario_dss), formulation, str(PV_VM_UB_PU)],
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )
    wall_time = time.time() - started
    if completed.returncode != 0:
        raise RuntimeError(completed.stderr.strip() or completed.stdout.strip() or "Julia PMD OPF run failed")
    payload = None
    for line in reversed(completed.stdout.strip().splitlines()):
        candidate = line.strip()
        if candidate.startswith("{"):
            payload = json.loads(candidate)
            break
    if payload is None:
        raise RuntimeError(f"Julia PMD OPF run returned no JSON payload: {completed.stdout.strip()}")
    payload["wall_time_seconds"] = wall_time
    payload["formulation"] = formulation
    return payload


def main() -> None:
    output_dir = ensure_dir(REPO_ROOT / "docs" / "generated" / "uc5")
    adapter = PandapowerAdapter()
    validator = ValidationService()

    case = PandapowerImportAdapter().import_case(SOURCE_JSON).copy_with(case_id=CASE_ID)
    if not case.is_unbalanced:
        raise RuntimeError("Expected the IEEE European LV asymmetric feeder to import as unbalanced")

    reference_net = adapter.run_power_flow_3ph(case)
    slack = validator._extract_3ph_slack(reference_net)
    reference_slack_p = sum(slack[key] for key in ("p_a_mw", "p_b_mw", "p_c_mw"))
    reference_slack_q = sum(slack[key] for key in ("q_a_mvar", "q_b_mvar", "q_c_mvar"))

    pandapower_roundtrip = validator.validate_pandapower_unbalanced_roundtrip(case)

    opendss_export = OpenDSSExportAdapter().export_case(case, output_dir / f"{CASE_ID}.opendss.dss")
    opendss_reference = OpenDSSImportAdapter().solve_source_case(opendss_export)
    opendss_validation = validator.validate_pandapower_unbalanced_against_opendss(case, opendss_reference)

    pmd_export = PowerModelsDistributionAdapter().export_input(case, output_dir / f"{CASE_ID}.pmd.dss")
    pmd_validation = validator.validate_powermodelsdistribution_export(
        case,
        pmd_input_path=pmd_export,
        julia_binary=detect_julia_binary(),
        julia_script=REPO_ROOT / "src" / "powermodelconverter" / "julia_pmd" / "run_powermodels_distribution_pf.jl",
        julia_depot=julia_depot(),
        julia_project=REPO_ROOT / "src" / "powermodelconverter" / "julia_pmd",
        reference_slack_p_mw=reference_slack_p,
        reference_slack_q_mvar=reference_slack_q,
        reference_node_voltages=validator._extract_3ph_node_voltages(reference_net),
    )

    scenario = build_pv_scenario_dss(pmd_export, output_dir / f"{CASE_ID}.pv_scenario.dss")
    opf = None
    attempts = []
    for formulation in ("acp", "ivr"):
        try:
            opf = run_mc_opf(Path(scenario["dss_path"]), formulation)
        except RuntimeError as error:
            attempts.append({"formulation": formulation, "error": str(error)[:2000]})
            continue
        if str(opf.get("termination_status", "")) in {"LOCALLY_SOLVED", "ALMOST_LOCALLY_SOLVED", "SOLVED", "ALMOST_SOLVED"}:
            break
        attempts.append({"formulation": formulation, "termination_status": opf.get("termination_status")})
        opf = None
    if opf is None:
        raise RuntimeError(f"PMD unbalanced OPF did not solve in any formulation: {attempts}")

    curtailment_rows = [
        [
            entry["generator"],
            entry["bus"],
            entry["phase"],
            entry["pg_kw"],
            entry["capacity_kw"],
            entry["curtailment_kw"],
        ]
        for entry in opf.pop("pv_dispatch")
    ]
    curtailment_rows.sort(key=lambda row: -float(row[5]))
    write_csv(
        output_dir / "curtailment_table.csv",
        ["generator", "bus", "phase", "pg_kw", "capacity_kw", "curtailment_kw"],
        curtailment_rows,
    )

    summary = {
        "use_case": "uc5",
        "script": "docs/examples/uc5/run.py",
        "inputs": {"pandapower_json": str(SOURCE_JSON)},
        "model_size": case.element_counts,
        "validations": {
            "pandapower_3ph_roundtrip": pandapower_roundtrip,
            "pandapower_vs_opendss": opendss_validation,
            "pandapower_vs_powermodelsdistribution": pmd_validation,
        },
        "pv_scenario": {key: value for key, value in scenario.items() if key != "dss_path"},
        "unbalanced_opf": {
            **opf,
            "vm_ub_pu": PV_VM_UB_PU,
            "failed_attempts": attempts,
        },
        "artifacts": {
            "opendss_dss": str(opendss_export),
            "pmd_dss": str(pmd_export),
            "pv_scenario_dss": str(scenario["dss_path"]),
            "curtailment_table_csv": str(output_dir / "curtailment_table.csv"),
        },
    }
    write_json(output_dir / "summary.json", summary)
    print(json.dumps(json_ready(summary), indent=2))


if __name__ == "__main__":
    main()
