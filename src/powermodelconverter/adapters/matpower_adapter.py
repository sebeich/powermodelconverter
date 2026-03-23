from __future__ import annotations

from pathlib import Path
from typing import Any

from pandapower.converter.matpower.from_mpc import from_mpc
from pandapower.converter.matpower.to_mpc import to_mpc

from powermodelconverter.adapters.base import ExportAdapter, ImportAdapter
from powermodelconverter.adapters.pandapower_adapter import PandapowerAdapter
from powermodelconverter.core.model import CanonicalCase


class MatpowerImportAdapter(ImportAdapter):
    source_format = "matpower"

    def __init__(self) -> None:
        self._pandapower = PandapowerAdapter()

    def import_case(self, source: str | Path, **kwargs: Any) -> CanonicalCase:
        path = Path(source)
        net = from_mpc(str(path), **kwargs)
        return self._pandapower.to_canonical(
            net,
            case_id=path.stem,
            source_format=self.source_format,
            metadata={"import_backend": "pandapower.from_mpc"},
            source_path=path,
        )


class MatpowerExportAdapter(ExportAdapter):
    target_format = "matpower"

    def __init__(self) -> None:
        self._pandapower = PandapowerAdapter()

    def export_case(self, case: CanonicalCase, destination: str | Path, **kwargs: Any) -> Path:
        path = Path(destination)
        path.parent.mkdir(parents=True, exist_ok=True)
        net = self._pandapower.to_net(case)
        to_mpc(net, filename=str(path), **kwargs)
        return path
