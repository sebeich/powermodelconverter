from __future__ import annotations

from pathlib import Path
from typing import Any

import pandapower as pp

from powermodelconverter.core.contracts import ImportAdapter
from powermodelconverter.core.pandapower_backend import PandapowerAdapter
from powermodelconverter.core.model import CanonicalCase


class PandapowerImportAdapter(ImportAdapter):
    source_format = "pandapower"

    def __init__(self) -> None:
        self._pandapower = PandapowerAdapter()

    def import_case(self, source: str | Path, **kwargs: Any) -> CanonicalCase:
        path = Path(source)
        net = pp.from_json(str(path), **kwargs)
        return self._pandapower.to_canonical(
            net,
            case_id=path.stem,
            source_format=self.source_format,
            metadata={"import_backend": "pandapower.from_json"},
            source_path=path,
        )


def import_pandapower(path: str | Path, **kwargs: Any) -> CanonicalCase:
    return PandapowerImportAdapter().import_case(path, **kwargs)


__all__ = ["PandapowerImportAdapter", "import_pandapower"]
