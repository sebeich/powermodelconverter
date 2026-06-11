#!/usr/bin/env python3
"""
UC1 research-grade reproducer: convex OPF relaxation gaps on pinned PGLib-OPF benchmarks.

Citations:
- Babaeinejadsarookolaee et al. (2019), "The Power Grid Library for Benchmarking AC Optimal Power Flow Algorithms", arXiv:1908.02788.
- Coffrin, Hijazi, Van Hentenryck (2016), "The QC Relaxation: A Theoretical and Computational Study on Optimal Power Flow", IEEE TPWRS.
- Low (2014), "Convex Relaxation of Optimal Power Flow, Parts I and II".
- Molzahn, Hiskens (2019), "A Survey of Relaxations and Approximations of the Power Flow Equations", FnT Electric Energy Systems.
"""

from __future__ import annotations

import csv
from dataclasses import asdict
import json
import math
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import time

CURRENT_DIR = Path(__file__).resolve().parent
EXAMPLES_ROOT = CURRENT_DIR.parent
if str(EXAMPLES_ROOT) not in sys.path:
    sys.path.insert(0, str(EXAMPLES_ROOT))

from _common import REPO_ROOT
from _common import ensure_dir
from _common import json_ready
from _common import write_json

from powermodelconverter.core.pandapower_backend import PandapowerAdapter
from powermodelconverter.importers.matpower import MatpowerImportAdapter
from powermodelconverter.importers.pandapower_json import PandapowerImportAdapter
from powermodelconverter.validation.engine import ValidationService


PGLIB_COMMIT = "dc6be4b2f85ca0e776952ec22cbd4c22396ea5a3"
PGLIB_DIR = REPO_ROOT / "input" / "pglib" / PGLIB_COMMIT
CASE_FILES = [
    "pglib_opf_case5_pjm.m",
    "pglib_opf_case14_ieee.m",
    "pglib_opf_case30_ieee__api.m",
]
PGLIB_BASELINE = {
    "pglib_opf_case5_pjm": {"qc_gap_percent": 14.55, "soc_gap_percent": 14.55},
    "pglib_opf_case14_ieee": {"qc_gap_percent": 0.11, "soc_gap_percent": 0.11},
    "pglib_opf_case30_ieee__api": {"qc_gap_percent": 5.43, "soc_gap_percent": 5.43},
}


def detect_julia_binary() -> str:
    return os.environ.get("PMC_JULIA_BINARY") or os.environ.get("JULIA_BIN") or "julia"


def parse_pglib_baseline(path: Path) -> dict[str, dict[str, float]]:
    content = path.read_text()
    rows: dict[str, dict[str, float]] = {}
    pattern = re.compile(
        r"^\|\s*(pglib_opf_case[^\s|]+)\s*\|\s*\d+\s*\|\s*\d+\s*\|\s*[^|]+\|\s*[^|]+\|\s*([0-9.]+)\s*\|\s*([0-9.]+)\s*\|",
        re.MULTILINE,
    )
    for match in pattern.finditer(content):
        rows[match.group(1)] = {
            "qc_gap_percent": float(match.group(2)),
            "soc_gap_percent": float(match.group(3)),
        }
    return rows


def validate_pandapower_export(case, exported_path: Path) -> object:
    validator = ValidationService()
    pandapower = PandapowerAdapter()
    reference = pandapower.run_power_flow(case)
    slack_p, slack_q = validator._extract_balanced_slack(reference)
    reference_voltages = {
        validator._bus_key(reference, int(idx)): complex(
            float(row.vm_pu) * math.cos(math.radians(float(row.va_degree))),
            float(row.vm_pu) * math.sin(math.radians(float(row.va_degree))),
        )
        for idx, row in reference.res_bus.iterrows()
    }
    exported_case = PandapowerImportAdapter().import_case(exported_path)
    return validator.validate_against_pandapower(
        exported_case,
        reference_slack_p_mw=slack_p,
        reference_slack_q_mvar=slack_q,
        reference_voltages=reference_voltages,
    )


def run_relaxations(powermodels_json: Path) -> dict[str, object]:
    data = json.loads(powermodels_json.read_text())
    data.pop("user_defined_params", None)
    with tempfile.NamedTemporaryFile(mode="w", suffix=".powermodels.json", prefix="pmc_uc1_", delete=False) as handle:
        json.dump(data, handle)
        safe_json_path = Path(handle.name)

    script = REPO_ROOT / "docs" / "examples" / "uc1" / "opf_relaxations.jl"
    project = REPO_ROOT / "src" / "powermodelconverter" / "julia"
    env = {
        **os.environ,
        "JULIA_PROJECT": str(project),
        "JULIA_DEPOT_PATH": os.environ.get("JULIA_DEPOT_PATH", str(REPO_ROOT / ".julia_depot")),
    }
    started = time.time()
    completed = subprocess.run(
        [detect_julia_binary(), str(script), str(safe_json_path)],
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )
    wall_time = time.time() - started
    if completed.returncode != 0:
        raise RuntimeError(completed.stderr.strip() or completed.stdout.strip() or "Julia OPF run failed")
    payload = None
    for line in reversed(completed.stdout.strip().splitlines()):
        candidate = line.strip()
        if candidate.startswith("{"):
            payload = json.loads(candidate)
            break
    if payload is None:
        raise RuntimeError(f"Julia OPF run returned no JSON payload: {completed.stdout.strip()}")
    payload["wall_time_seconds"] = wall_time
    return payload


def build_case_summary(case_path: Path, output_dir: Path, baselines: dict[str, dict[str, float]]) -> dict[str, object]:
    case = MatpowerImportAdapter().import_case(case_path)
    pandapower = PandapowerAdapter()
    validator = ValidationService()

    pandapower_json = pandapower.export_json(case, output_dir / f"{case.case_id}.pandapower.json")
    pandapower_validation = validate_pandapower_export(case, pandapower_json)

    powermodels_json = pandapower.export_powermodels_json(case, output_dir / f"{case.case_id}.powermodels.json")
    powermodels_validation = validator.validate_powermodels_export(
        case,
        powermodels_json=powermodels_json,
        julia_binary=detect_julia_binary(),
        julia_script=REPO_ROOT / "src" / "powermodelconverter" / "julia" / "run_powermodels_pf.jl",
        julia_depot=Path(os.environ.get("JULIA_DEPOT_PATH", str(REPO_ROOT / ".julia_depot"))),
    )

    relaxations = run_relaxations(powermodels_json)
    ac_objective = float(relaxations["ac"]["objective"])
    for key in ("soc", "qc", "sdp"):
        entry = relaxations[key]
        objective = entry.get("objective")
        entry["gap_percent"] = None if objective is None else 100.0 * (ac_objective - float(objective)) / ac_objective

    baseline = baselines.get(case.case_id, PGLIB_BASELINE.get(case.case_id, {}))
    discrepancy: dict[str, object] = {}
    for key, baseline_key in (("qc", "qc_gap_percent"), ("soc", "soc_gap_percent")):
        observed = relaxations[key]["gap_percent"]
        reference = baseline.get(baseline_key)
        if observed is None or reference is None:
            discrepancy[key] = {"within_10_percent_relative": None}
            continue
        relative_error = abs(observed - reference) / max(abs(reference), 1e-9)
        discrepancy[key] = {
            "reference_gap_percent": reference,
            "observed_gap_percent": observed,
            "relative_error": relative_error,
            "within_10_percent_relative": relative_error <= 0.10,
        }

    return {
        "case_id": case.case_id,
        "inputs": {"matpower": str(case_path)},
        "artifacts": {
            "pandapower_json": str(pandapower_json),
            "powermodels_json": str(powermodels_json),
        },
        "validation": {
            "matpower_to_pandapower": asdict(pandapower_validation),
            "pandapower_to_powermodels": asdict(powermodels_validation),
        },
        "opf_results": relaxations,
        "baseline_comparison": discrepancy,
    }


def main() -> None:
    output_dir = ensure_dir(REPO_ROOT / "docs" / "generated" / "uc1")
    baselines = parse_pglib_baseline(PGLIB_DIR / "BASELINE.md")

    cases = [build_case_summary(PGLIB_DIR / name, output_dir, baselines) for name in CASE_FILES]

    with (output_dir / "relaxation_table.csv").open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["case_id", "formulation", "objective", "gap_percent", "termination_status", "solve_time_seconds"])
        for case in cases:
            for formulation in ("ac", "soc", "qc", "sdp"):
                row = case["opf_results"][formulation]
                writer.writerow(
                    [
                        case["case_id"],
                        formulation,
                        row.get("objective"),
                        row.get("gap_percent"),
                        row.get("termination_status"),
                        row.get("solve_time_seconds"),
                    ]
                )

    summary = {
        "use_case": "uc1",
        "script": "docs/examples/uc1/run.py",
        "pinned_versions": {
            "pmc_branch": "refactor/hub-and-spoke",
            "pglib_commit": PGLIB_COMMIT,
        },
        "cases": cases,
        "artifacts": {
            "relaxation_table_csv": str(output_dir / "relaxation_table.csv"),
        },
    }
    write_json(output_dir / "summary.json", summary)
    print(json.dumps(json_ready(summary), indent=2))


if __name__ == "__main__":
    main()
