from __future__ import annotations

from pathlib import Path

from powermodelconverter.core.model import CanonicalCase


def export_pypower(case: CanonicalCase, output_path: str | Path, **_: object) -> Path:
    raise NotImplementedError("A dedicated PYPOWER exporter is not implemented in this refactor step.")
