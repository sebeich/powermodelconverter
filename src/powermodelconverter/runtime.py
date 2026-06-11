from __future__ import annotations

import os
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


__all__ = ["resolve_julia_binary"]
