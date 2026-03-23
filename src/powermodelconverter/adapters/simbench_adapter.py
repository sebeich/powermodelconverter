from __future__ import annotations

from pathlib import Path
from typing import Any

import pandapower.toolbox as tb
import simbench as sb

from powermodelconverter.adapters.base import ImportAdapter
from powermodelconverter.adapters.pandapower_adapter import PandapowerAdapter
from powermodelconverter.core.model import CanonicalCase


class SimbenchImportAdapter(ImportAdapter):
    source_format = "simbench"

    def __init__(self) -> None:
        self._pandapower = PandapowerAdapter()

    def import_case(self, source: str | Path, **kwargs: Any) -> CanonicalCase:
        code = str(source)
        net = sb.get_simbench_net(code, **kwargs)
        # SimBench networks often ship with sparse element indices. Normalizing them
        # early makes later converter and validation steps more predictable.
        tb.create_continuous_bus_index(net)
        tb.create_continuous_elements_index(net)
        return self._pandapower.to_canonical(
            net,
            case_id=code.replace("/", "_"),
            source_format=self.source_format,
            metadata={"simbench_code": code, "normalized_indices": True},
        )
