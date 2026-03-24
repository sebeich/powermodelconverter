from __future__ import annotations

from pathlib import Path
from typing import Any

from powermodelconverter.adapters.base import ImportAdapter
from powermodelconverter.adapters.pypsa_adapter import PypsaAdapter
from powermodelconverter.core.model import CanonicalCase


class PypsaImportAdapter(ImportAdapter):
    source_format = "pypsa"

    def __init__(self) -> None:
        self._pypsa = PypsaAdapter()

    def import_case(self, source: str | Path, **kwargs: Any) -> CanonicalCase:
        return self._pypsa.import_case(source)
