from __future__ import annotations

import json
import os
import subprocess
import tempfile
from pathlib import Path
from typing import Any

from powermodelconverter.core.exceptions import ValidationError
from powermodelconverter.core.model import CanonicalCase
from powermodelconverter.core.pandapower_backend import PandapowerAdapter
from powermodelconverter.importers.pypsa import PypsaAdapter


def run_pandapower_pf(case: CanonicalCase, **kwargs: Any) -> Any:
    return PandapowerAdapter().run_power_flow(case, **kwargs)


def run_pandapower_3ph_pf(case: CanonicalCase, **kwargs: Any) -> Any:
    return PandapowerAdapter().run_power_flow_3ph(case, **kwargs)


def run_pypsa_pf(network_path: str | Path) -> Any:
    return PypsaAdapter().solve_network_file(Path(network_path))


def run_julia_powermodels(
    powermodels_json: str | Path,
    *,
    julia_binary: str,
    julia_script: str | Path,
    julia_depot: str | Path,
) -> dict[str, Any]:
    safe_json_path = _sanitize_powermodels_json(Path(powermodels_json))
    env = {
        **os.environ,
        "JULIA_DEPOT_PATH": str(julia_depot),
        "JULIA_PROJECT": str(Path(julia_script).parent),
    }
    command = [julia_binary, str(julia_script), str(safe_json_path)]
    completed = subprocess.run(command, capture_output=True, text=True, env=env, check=False)
    if completed.returncode != 0:
        raise ValidationError(
            "PowerModels validation failed: "
            f"{completed.stderr.strip() or completed.stdout.strip() or 'unknown Julia error'}"
        )
    return _extract_json_payload(completed.stdout, "PowerModels")


def run_julia_powermodels_opf(
    powermodels_json: str | Path,
    *,
    problem: str,
    julia_binary: str,
    julia_script: str | Path,
    julia_depot: str | Path,
    voltage_bounds_pu: tuple[float, float] | None = None,
    loss_slack_capacity_mw: float | None = None,
) -> dict[str, Any]:
    """Solve a DC or AC OPF on a converted model in PowerModels.jl.

    Unlike the power-flow runner this does not raise when the solver stops
    short: an OPF that fails to converge, or stops at Ipopt's acceptable
    tolerance, is a reportable result about the converted model rather than a
    broken run, and the caller needs to see the status to say so.
    """
    safe_json_path = _sanitize_powermodels_json(Path(powermodels_json))
    env = {
        **os.environ,
        "JULIA_DEPOT_PATH": str(julia_depot),
        "JULIA_PROJECT": str(Path(julia_script).parent),
    }
    command = [julia_binary, str(julia_script), str(safe_json_path), str(problem)]
    if voltage_bounds_pu is not None:
        command += ["--vbounds", repr(float(voltage_bounds_pu[0])),
                    repr(float(voltage_bounds_pu[1]))]
    if loss_slack_capacity_mw is not None:
        command += ["--loss-slack-capacity", repr(float(loss_slack_capacity_mw))]
    completed = subprocess.run(command, capture_output=True, text=True, env=env, check=False)
    if completed.returncode != 0:
        raise ValidationError(
            f"PowerModels {problem} failed: "
            f"{completed.stderr.strip() or completed.stdout.strip() or 'unknown Julia error'}"
        )
    return _extract_json_payload(completed.stdout, f"PowerModels {problem}")


def run_julia_pmd(
    pmd_input_path: str | Path,
    *,
    julia_binary: str,
    julia_script: str | Path,
    julia_depot: str | Path,
    julia_project: str | Path,
) -> dict[str, Any]:
    env = {
        **os.environ,
        "JULIA_DEPOT_PATH": str(julia_depot),
        "JULIA_PROJECT": str(julia_project),
    }
    command = [julia_binary, str(julia_script), str(pmd_input_path)]
    completed = subprocess.run(command, capture_output=True, text=True, env=env, check=False)
    if completed.returncode != 0:
        raise ValidationError(
            "PowerModelsDistribution validation failed: "
            f"{completed.stderr.strip() or completed.stdout.strip() or 'unknown Julia error'}"
        )
    return _extract_json_payload(completed.stdout, "PowerModelsDistribution")


def run_julia_powersystems(
    powersystems_case: str | Path,
    *,
    julia_binary: str,
    julia_script: str | Path,
    julia_depot: str | Path,
    julia_project: str | Path,
) -> dict[str, Any]:
    env = {
        **os.environ,
        "JULIA_DEPOT_PATH": str(julia_depot),
        "JULIA_PROJECT": str(julia_project),
    }
    command = [julia_binary, str(julia_script), str(powersystems_case)]
    completed = subprocess.run(command, capture_output=True, text=True, env=env, check=False)
    if completed.returncode != 0:
        raise ValidationError(
            "PowerSystems/PowerSimulations validation failed: "
            f"{completed.stderr.strip() or completed.stdout.strip() or 'unknown Julia error'}"
        )
    return _extract_json_payload(completed.stdout, "PowerSystems/PowerSimulations")


def _extract_json_payload(stdout: str, backend_name: str) -> dict[str, Any]:
    rendered = stdout.strip()
    for line in reversed(rendered.splitlines()):
        candidate = line.strip()
        if candidate.startswith("{"):
            return json.loads(candidate)
    raise ValidationError(f"{backend_name} validation returned no JSON payload: {rendered}")


def _sanitize_powermodels_json(powermodels_json: Path) -> Path:
    data = json.loads(powermodels_json.read_text())
    # pandapower's converter attaches bookkeeping of its own that is not part of
    # the PowerModels schema; its parser rejects the file outright rather than
    # ignoring the extra keys.
    for extra_key in ("user_defined_params", "gen_and_controllable_sgen"):
        data.pop(extra_key, None)
    with tempfile.NamedTemporaryFile(mode="w", suffix=".powermodels.json", prefix="pmc_", delete=False) as handle:
        json.dump(data, handle)
        return Path(handle.name)
