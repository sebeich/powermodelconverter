from __future__ import annotations

from pathlib import Path

from powermodelconverter.core.model import CanonicalCase


def import_powermodels(path: str | Path, **_: object) -> CanonicalCase:
    raise NotImplementedError("A dedicated PowerModels importer is not implemented in this refactor step.")
