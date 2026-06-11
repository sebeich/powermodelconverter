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
    data.pop("user_defined_params", None)
    with tempfile.NamedTemporaryFile(mode="w", suffix=".powermodels.json", prefix="pmc_", delete=False) as handle:
        json.dump(data, handle)
        return Path(handle.name)
