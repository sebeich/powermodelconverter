"""Source-side oracle for MATPOWER routes: MATPOWER itself, run on Octave.

pmc reads and writes the MATPOWER `.m` format through `matpowercaseframes`, which
parses but does not solve. Without MATPOWER in the loop the reference operating
point for a MATPOWER-sourced route can only come from pandapower, so the route
compares pandapower against pandapower and can only report a zero deviation --
a determinism check, not a cross-tool comparison.

This module runs the real tool in its own runtime, the same arrangement already
used for PowerModels.jl and PowerModelsDistribution, and compares the solved
operating point node by node.

Bus correspondence is positional: `matpowercaseframes` preserves the row order of
`mpc.bus` and the importer creates pandapower buses in that order, so pandapower
index i is the i-th row of the MATPOWER bus matrix.
"""

from __future__ import annotations

import cmath
import json
import math
import os
from pathlib import Path
import re
import subprocess
import tempfile
from typing import Any

from powermodelconverter.core.model import CanonicalCase
from powermodelconverter.validation.result import ValidationResult
from powermodelconverter.validation.tolerances import BALANCED_TOLERANCES

REPO_ROOT = Path(__file__).resolve().parents[3]
REFERENCE_SCRIPT = REPO_ROOT / "scripts" / "matpower_reference.m"


def detect_octave_binary() -> str:
    return os.environ.get("PMC_OCTAVE_BINARY") or os.environ.get("OCTAVE_BINARY") or "octave-cli"


def _sanitized_case_copy(case_path: Path, directory: Path) -> Path:
    """Copy the case under a name MATPOWER's ``loadcase`` can dispatch on.

    ``loadcase`` resolves a case file by calling it as a function, so the file
    stem has to be a valid identifier. pmc case ids routinely are not --
    ``core_bal_star_3bus.pandapower.m`` has a dot in the stem, and MATPOWER
    fails inside ``loadcase`` on it. Copy to a clean name and rewrite the
    function header to match, leaving the original untouched.
    """
    stem = re.sub(r"\W", "_", case_path.stem)
    if stem and stem[0].isdigit():
        stem = f"c_{stem}"
    destination = directory / f"{stem}.m"
    text = case_path.read_text()
    text = re.sub(r"^(\s*function\s+\w+\s*=\s*).*$", rf"\g<1>{stem}", text, count=1, flags=re.M)
    destination.write_text(text)
    return destination


def solve_matpower_reference(
    case_path: str | Path,
    *,
    octave_binary: str | None = None,
    reference_script: str | Path | None = None,
) -> dict[str, Any]:
    """Run MATPOWER's own ``runpf`` and return its solved operating point."""
    script = Path(reference_script or REFERENCE_SCRIPT)
    with tempfile.TemporaryDirectory(prefix="pmc_matpower_") as staging:
        return _run_reference(Path(case_path), Path(staging), script, octave_binary)


def _run_reference(
    case_path: Path, staging: Path, script: Path, octave_binary: str | None
) -> dict[str, Any]:
    prepared = _sanitized_case_copy(case_path, staging)
    completed = subprocess.run(
        [octave_binary or detect_octave_binary(), str(script), str(prepared)],
        capture_output=True,
        text=True,
        check=False,
        env={**os.environ, "MATPOWER_HOME": os.environ.get("MATPOWER_HOME", "/opt/matpower")},
    )
    # Octave 7 can raise while shutting down after a clean run, so the payload on
    # stdout decides success rather than the exit status.
    for line in reversed(completed.stdout.strip().splitlines()):
        candidate = line.strip()
        if candidate.startswith("{"):
            return json.loads(candidate)
    raise RuntimeError(
        f"MATPOWER returned no payload for {case_path.name}: "
        f"{completed.stderr.strip() or completed.stdout.strip()}"
    )


def _slack_generation(net: Any) -> tuple[float, float]:
    """Generation at the reference buses, matching what the Octave side reports.

    Never net of a load at the same bus: pandapower models such a load as its own
    element while MATPOWER's bus row carries it, and netting one side only would
    compare two different quantities (pglib_opf_case5_pjm has 400 MW there).
    """
    p = float(net.res_ext_grid.p_mw.sum()) if len(net.res_ext_grid) else 0.0
    q = float(net.res_ext_grid.q_mvar.sum()) if len(net.res_ext_grid) else 0.0
    if len(net.gen):
        at_slack = net.gen.bus.isin(net.ext_grid.bus)
        p += float(net.res_gen.p_mw[at_slack].sum())
        q += float(net.res_gen.q_mvar[at_slack].sum())
    return p, q


def validate_against_matpower(
    case: CanonicalCase,
    case_path: str | Path,
    solved_net: Any,
    *,
    slack_tolerance_mva: float = BALANCED_TOLERANCES["slack_mva"],
    voltage_tolerance_pu: float = BALANCED_TOLERANCES["voltage_pu"],
    octave_binary: str | None = None,
) -> ValidationResult:
    """Compare a solved hub model against MATPOWER's own solution of the source."""
    try:
        reference = solve_matpower_reference(case_path, octave_binary=octave_binary)
    except Exception as exc:  # noqa: BLE001 - record the route as pending, do not abort
        return ValidationResult(
            case_id=case.case_id,
            passed=False,
            slack_delta_mva=math.inf,
            max_voltage_delta_pu=math.inf,
            details={"compared_buses": 0, "backend": "matpower", "error": str(exc)[:500]},
            status="pending",
            notes=f"MATPOWER reference unavailable: {type(exc).__name__}.",
        )
    if not reference.get("success"):
        return ValidationResult(
            case_id=case.case_id,
            passed=False,
            slack_delta_mva=math.inf,
            max_voltage_delta_pu=math.inf,
            details={"compared_buses": 0, "backend": "matpower"},
            status="pending",
            notes="MATPOWER runpf did not converge on the source case.",
        )

    reference_v = [
        cmath.rect(float(entry["vm_pu"]), math.radians(float(entry["va_deg"])))
        for entry in reference["bus"]
    ]
    candidate_v = [
        cmath.rect(float(row.vm_pu), math.radians(float(row.va_degree)))
        for _, row in solved_net.res_bus.iterrows()
    ]
    if len(reference_v) != len(candidate_v):
        return ValidationResult(
            case_id=case.case_id,
            passed=False,
            slack_delta_mva=math.inf,
            max_voltage_delta_pu=math.inf,
            details={
                "compared_buses": 0,
                "backend": "matpower",
                "matpower_buses": len(reference_v),
                "pandapower_buses": len(candidate_v),
            },
            status="pending",
            notes="Bus count differs between MATPOWER and the converted model.",
        )

    deltas = [abs(c - r) for c, r in zip(candidate_v, reference_v)]
    max_delta = max(deltas, default=0.0)
    candidate_p, candidate_q = _slack_generation(solved_net)
    slack_delta = math.hypot(
        candidate_p - float(reference["slack_p_mw"]),
        candidate_q - float(reference["slack_q_mvar"]),
    )
    return ValidationResult(
        case_id=case.case_id,
        passed=(max_delta <= voltage_tolerance_pu
                and slack_delta <= slack_tolerance_mva
                and len(deltas) > 0),
        slack_delta_mva=slack_delta,
        max_voltage_delta_pu=max_delta,
        details={
            "compared_buses": len(deltas),
            "backend": "matpower",
            "worst_bus_index": deltas.index(max_delta) if deltas else -1,
            "matpower_slack_mw": float(reference["slack_p_mw"]),
            "pandapower_slack_mw": candidate_p,
        },
    )
