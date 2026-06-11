#!/usr/bin/env python3
from __future__ import annotations

import json
import math
import os
from dataclasses import asdict, is_dataclass
from pathlib import Path
import sys
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = REPO_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from powermodelconverter.core.model import CanonicalCase  # noqa: E402
from powermodelconverter.core.pandapower_backend import PandapowerAdapter  # noqa: E402
from powermodelconverter.validation.engine import ValidationService  # noqa: E402


def repo_path(*parts: str) -> Path:
    return REPO_ROOT.joinpath(*parts)


def default_output_dir(name: str) -> Path:
    return REPO_ROOT / "docs" / "generated" / name


def ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def json_ready(value: Any) -> Any:
    if is_dataclass(value):
        return asdict(value)
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, complex):
        return {"real": value.real, "imag": value.imag}
    if isinstance(value, dict):
        return {str(key): json_ready(inner) for key, inner in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_ready(inner) for inner in value]
    return value


def write_json(path: Path, payload: Any) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(json_ready(payload), indent=2, sort_keys=True) + "\n")
    return path


def write_csv(path: Path, header: list[str], rows: list[list[Any]]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    rendered = [",".join(header)]
    for row in rows:
        rendered.append(",".join(_csv_cell(value) for value in row))
    path.write_text("\n".join(rendered) + "\n")
    return path


def _csv_cell(value: Any) -> str:
    text = str(value)
    if any(token in text for token in [",", "\"", "\n"]):
        return "\"" + text.replace("\"", "\"\"") + "\""
    return text


def julia_settings() -> dict[str, Path | str]:
    return {
        "julia_binary": os.environ.get("PMC_JULIA_BINARY", "julia"),
        "julia_depot": Path(os.environ.get("JULIA_DEPOT_PATH", str(REPO_ROOT / ".julia_depot"))),
        "powermodels_script": repo_path("src", "powermodelconverter", "julia", "run_powermodels_pf.jl"),
        "pmd_script": repo_path("src", "powermodelconverter", "julia_pmd", "run_powermodels_distribution_pf.jl"),
        "pmd_project": repo_path("src", "powermodelconverter", "julia_pmd"),
    }


def balanced_reference_validation(case: CanonicalCase) -> dict[str, Any]:
    pandapower = PandapowerAdapter()
    validator = ValidationService()
    reference_net = pandapower.run_power_flow(case)
    slack_p, slack_q = validator._extract_balanced_slack(reference_net)
    reference_voltages = {
        validator._bus_key(reference_net, int(idx)): complex(
            float(row.vm_pu) * math.cos(math.radians(float(row.va_degree))),
            float(row.vm_pu) * math.sin(math.radians(float(row.va_degree))),
        )
        for idx, row in reference_net.res_bus.iterrows()
    }
    result = validator.validate_against_pandapower(
        case,
        reference_slack_p_mw=slack_p,
        reference_slack_q_mvar=slack_q,
        reference_voltages=reference_voltages,
    )
    return {
        "reference_net": reference_net,
        "validation": result,
    }

