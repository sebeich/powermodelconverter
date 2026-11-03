from __future__ import annotations

import os
from pathlib import Path
import shutil


def resolve_julia_binary() -> str:
    explicit = os.environ.get("PMC_JULIA_BINARY")
    if explicit:
        return explicit

    env_bin = os.environ.get("JULIA_BIN")
    if env_bin:
        return env_bin

    discovered = shutil.which("julia")
    if discovered:
        return discovered

    return "/opt/julia/bin/julia"


def resolve_julia_depot() -> Path:
    explicit = os.environ.get("JULIA_DEPOT_PATH")
    if explicit:
        return Path(explicit.split(os.pathsep)[0])

    return Path(__file__).resolve().parents[2] / ".julia_depot"


__all__ = ["resolve_julia_binary", "resolve_julia_depot"]
