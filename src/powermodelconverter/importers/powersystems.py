from __future__ import annotations

from pathlib import Path
from typing import Any

from powermodelconverter.core.contracts import ImportAdapter
from powermodelconverter.importers.matpower import import_matpower
from powermodelconverter.core.model import CanonicalCase


class PowerSystemsImportAdapter(ImportAdapter):
    source_format = "powersystems"

    def import_case(self, source: str | Path, **kwargs: Any) -> CanonicalCase:
        imported = import_matpower(source, **kwargs)
        metadata = dict(imported.metadata)
        metadata["import_backend"] = "powersystems:matpower_parser_compat"
        return imported.copy_with(
            source_tool=self.source_format,
            metadata=metadata,
        )


def import_powersystems(path: str | Path, **kwargs: Any) -> CanonicalCase:
    return PowerSystemsImportAdapter().import_case(path, **kwargs)


__all__ = ["PowerSystemsImportAdapter", "import_powersystems"]
