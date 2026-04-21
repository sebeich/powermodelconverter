from __future__ import annotations

from pathlib import Path
from typing import Any

import pandapower.toolbox as tb
import simbench as sb

from powermodelconverter.core.contracts import ImportAdapter
from powermodelconverter.core.model import CanonicalCase
from powermodelconverter.core.pandapower_backend import PandapowerAdapter


class SimbenchImportAdapter(ImportAdapter):
    source_format = "simbench"

    def __init__(self) -> None:
        self._pandapower = PandapowerAdapter()

    def import_case(self, source: str | Path, **kwargs: Any) -> CanonicalCase:
        code = str(source)
        net = sb.get_simbench_net(code, **kwargs)
        tb.create_continuous_bus_index(net)
        tb.create_continuous_elements_index(net)
        return self._pandapower.to_canonical(
            net,
            case_id=code.replace("/", "_"),
            source_format=self.source_format,
            metadata={"simbench_code": code, "normalized_indices": True},
        )


def import_simbench(source: str | Path, **kwargs: Any) -> CanonicalCase:
    return SimbenchImportAdapter().import_case(source, **kwargs)


__all__ = ["SimbenchImportAdapter", "import_simbench"]
