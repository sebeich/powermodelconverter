from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

from powermodelconverter.core.model import CanonicalCase


class ImportAdapter(ABC):
    source_format: str

    @abstractmethod
    def import_case(self, source: str | Path, **kwargs: Any) -> CanonicalCase:
        raise NotImplementedError


class ExportAdapter(ABC):
    target_format: str

    @abstractmethod
    def export_case(self, case: CanonicalCase, destination: str | Path, **kwargs: Any) -> Path:
        raise NotImplementedError
