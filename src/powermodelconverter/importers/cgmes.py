from __future__ import annotations

from pathlib import Path
from typing import Any

from pandapower.converter.cim import from_cim

from powermodelconverter.core.contracts import ImportAdapter
from powermodelconverter.core.pandapower_backend import PandapowerAdapter
from powermodelconverter.core.model import CanonicalCase


class CGMESImportAdapter(ImportAdapter):
    source_format = "cgmes"

    def __init__(self) -> None:
        self._pandapower = PandapowerAdapter()

    def import_case(self, source: str | Path, **kwargs: Any) -> CanonicalCase:
        source_path = Path(source)
        file_list = self._resolve_file_list(source_path)
        net = from_cim.from_cim(file_list=[str(path) for path in file_list], **kwargs)
        case_id = source_path.stem if source_path.is_file() else source_path.name
        return self._pandapower.to_canonical(
            net,
            case_id=case_id,
            source_format=self.source_format,
            metadata={
                "import_backend": "pandapower.converter.cim.from_cim",
                "cgmes_files": [str(path) for path in file_list],
            },
            source_path=source_path,
        )

    def _resolve_file_list(self, source_path: Path) -> list[Path]:
        if source_path.is_file():
            return [source_path]
        if source_path.is_dir():
            files = sorted(
                path
                for path in source_path.iterdir()
                if path.suffix.lower() in {".zip", ".xml"}
            )
            if files:
                return files
        raise ValueError(f"No CGMES .zip or .xml files found at {source_path}")


def import_cgmes(path: str | Path, **kwargs: Any) -> CanonicalCase:
    return CGMESImportAdapter().import_case(path, **kwargs)


__all__ = ["CGMESImportAdapter", "import_cgmes"]
