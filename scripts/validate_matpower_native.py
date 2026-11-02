#!/usr/bin/env python3
# SPDX-License-Identifier: BSD-3-Clause
"""Validate pmc's MATPOWER import against MATPOWER itself, run natively on Octave.

Why this exists
---------------
pmc reads and writes the MATPOWER `.m` format through `matpowercaseframes`, which
parses but does not solve. Without MATPOWER in the loop, the reference operating
point for a MATPOWER-sourced route can only come from pandapower -- so the route
compares pandapower against pandapower and can only ever report a zero deviation.
That is a determinism check, not a cross-tool agreement check.

This script closes that gap the same way the Julia layer does for PowerModels and
PowerModelsDistribution: it runs the real tool, in its own runtime, and compares
the solved operating point node by node.

    reference : MATPOWER `runpf` on Octave      (scripts/matpower_reference.m)
    candidate : pmc import -> pandapower `runpp`

Requires the Octave image:

    docker build -t pmc:latest .
    docker run --rm -v "$PWD":/workspace -w /workspace -e PYTHONPATH=/workspace/src \\
        --entrypoint python pmc:latest scripts/validate_matpower_native.py

Bus correspondence is positional: `matpowercaseframes` preserves the row order of
`mpc.bus`, and the importer creates pandapower buses in that same order, so
pandapower index i corresponds to the i-th row of the MATPOWER bus matrix.
"""
from __future__ import annotations

import argparse
import cmath
import json
import math
import os
from pathlib import Path
import subprocess
import sys
import warnings

warnings.filterwarnings("ignore")

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(REPO_ROOT / "src"))

from powermodelconverter.core.pandapower_backend import PandapowerAdapter  # noqa: E402
from powermodelconverter.importers.matpower import MatpowerImportAdapter  # noqa: E402

REFERENCE_SCRIPT = REPO_ROOT / "scripts" / "matpower_reference.m"

# The balanced signed-off tier, as used everywhere else in the validation report.
VOLTAGE_TOLERANCE_PU = 1e-3
SLACK_TOLERANCE_MVA = 1e-3

DEFAULT_CASES = [
    REPO_ROOT / "validation_cases/native/matpower/case9.m",
    REPO_ROOT / "validation_cases/pglib/source/pglib_opf_case5_pjm.m",
    REPO_ROOT / "validation_cases/pglib/source/pglib_opf_case14_ieee.m",
    REPO_ROOT / "validation_cases/pglib/source/pglib_opf_case30_ieee__api.m",
]


def solve_with_matpower(case_path: Path, octave_binary: str) -> dict:
    """Run MATPOWER's own `runpf` and return the solved operating point."""
    completed = subprocess.run(
        [octave_binary, str(REFERENCE_SCRIPT), str(case_path)],
        capture_output=True,
        text=True,
        check=False,
        env={**os.environ, "MATPOWER_HOME": os.environ.get("MATPOWER_HOME", "/opt/matpower")},
    )
    # Octave 7 can raise on interpreter shutdown after a clean run, so the JSON
    # payload on stdout decides success rather than the exit status.
    for line in reversed(completed.stdout.strip().splitlines()):
        candidate = line.strip()
        if candidate.startswith("{"):
            return json.loads(candidate)
    raise RuntimeError(
        f"MATPOWER returned no payload for {case_path.name}: "
        f"{completed.stderr.strip() or completed.stdout.strip()}"
    )


def solve_with_pmc(case_path: Path) -> tuple[list[complex], float, float]:
    """Import through pmc and solve in the pandapower hub."""
    case = MatpowerImportAdapter().import_case(case_path)
    net = PandapowerAdapter().run_power_flow(case)
    voltages = [
        cmath.rect(float(row.vm_pu), math.radians(float(row.va_degree)))
        for _, row in net.res_bus.iterrows()
    ]
    # Generation at the reference buses, to match what the Octave side reports:
    # the external grid's supply plus any generator pandapower placed at the same
    # bus, and never net of a load that happens to sit there too.
    slack_p = float(net.res_ext_grid.p_mw.sum()) if len(net.res_ext_grid) else 0.0
    slack_q = float(net.res_ext_grid.q_mvar.sum()) if len(net.res_ext_grid) else 0.0
    if len(net.gen):
        at_slack = net.gen.bus.isin(net.ext_grid.bus)
        slack_p += float(net.res_gen.p_mw[at_slack].sum())
        slack_q += float(net.res_gen.q_mvar[at_slack].sum())
    return voltages, slack_p, slack_q


def compare(case_path: Path, octave_binary: str) -> dict:
    reference = solve_with_matpower(case_path, octave_binary)
    if not reference.get("success"):
        return {"case": case_path.stem, "status": "matpower_did_not_converge"}

    candidate_v, candidate_p, candidate_q = solve_with_pmc(case_path)
    reference_v = [
        cmath.rect(float(entry["vm_pu"]), math.radians(float(entry["va_deg"])))
        for entry in reference["bus"]
    ]
    if len(reference_v) != len(candidate_v):
        return {
            "case": case_path.stem,
            "status": "bus_count_mismatch",
            "matpower_buses": len(reference_v),
            "pandapower_buses": len(candidate_v),
        }

    deltas = [abs(c - r) for c, r in zip(candidate_v, reference_v)]
    max_delta = max(deltas, default=0.0)
    worst = deltas.index(max_delta) if deltas else -1
    slack_delta = math.hypot(
        candidate_p - float(reference["slack_p_mw"]),
        candidate_q - float(reference["slack_q_mvar"]),
    )
    return {
        "case": case_path.stem,
        "status": "compared",
        "buses": len(reference_v),
        "max_voltage_delta_pu": max_delta,
        "worst_bus_index": worst,
        "slack_delta_mva": slack_delta,
        "matpower_slack_mw": float(reference["slack_p_mw"]),
        "pandapower_slack_mw": candidate_p,
        "passed": max_delta <= VOLTAGE_TOLERANCE_PU and slack_delta <= SLACK_TOLERANCE_MVA,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("cases", nargs="*", default=[str(p) for p in DEFAULT_CASES])
    parser.add_argument("--octave-binary", default=os.environ.get("OCTAVE_BINARY", "octave-cli"))
    args = parser.parse_args()

    results = []
    for raw in args.cases:
        path = Path(raw)
        if not path.exists():
            print(f"  {path.name:34s} SKIPPED (not found)", flush=True)
            continue
        try:
            result = compare(path, args.octave_binary)
        except Exception as exc:  # noqa: BLE001 - report and continue
            result = {"case": path.stem, "status": "error", "error": f"{type(exc).__name__}: {exc}"}
        results.append(result)
        if result["status"] == "compared":
            print(
                f"  {result['case']:34s} buses={result['buses']:4d} "
                f"max|dV|={result['max_voltage_delta_pu']:.3e} pu  "
                f"dS={result['slack_delta_mva']:.3e} MVA  "
                f"passed={result['passed']}",
                flush=True,
            )
        else:
            print(f"  {result['case']:34s} {result['status']}: {result.get('error', '')}", flush=True)

    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
