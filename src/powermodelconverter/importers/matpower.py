from __future__ import annotations

from pathlib import Path
import re
import tempfile
from typing import Any

import pandapower as pp
from pandapower.converter.matpower.from_mpc import from_mpc

from powermodelconverter.core.contracts import ImportAdapter
from powermodelconverter.core.pandapower_backend import PandapowerAdapter
from powermodelconverter.core.model import CanonicalCase


class MatpowerImportAdapter(ImportAdapter):
    source_format = "matpower"

    def import_case(self, source: str | Path, **kwargs: Any) -> CanonicalCase:
        return import_matpower(source, **kwargs)


def import_matpower(path: str | Path, **kwargs: Any) -> CanonicalCase:
    source_path = Path(path)
    source_text = source_path.read_text()
    try:
        net = from_mpc(str(source_path), **kwargs)
    except ValueError as exc:
        if "max() iterable argument is empty" not in str(exc):
            raise
        normalized = _normalize_matpower_matrix_rows(source_text)
        with tempfile.NamedTemporaryFile("w", suffix=".m", delete=False) as handle:
            handle.write(normalized)
            normalized_path = Path(handle.name)
        try:
            net = from_mpc(str(normalized_path), **kwargs)
        finally:
            normalized_path.unlink(missing_ok=True)
    _ensure_reference_bus_from_matpower_source(net, source_text)
    return PandapowerAdapter().to_canonical(
        net,
        case_id=source_path.stem,
        source_format="matpower",
        metadata={"import_backend": "pandapower.from_mpc"},
        source_path=source_path,
    )


def _normalize_matpower_matrix_rows(contents: str) -> str:
    normalized_lines: list[str] = []
    inside_matrix = False

    for line in contents.splitlines():
        stripped = line.strip()
        if stripped.startswith("%"):
            continue
        if not inside_matrix and "= [" in line:
            inside_matrix = True
            normalized_lines.append(line)
            continue

        if inside_matrix:
            if stripped == "];":
                inside_matrix = False
                normalized_lines.append(line)
                continue
            if not stripped:
                normalized_lines.append(line)
                continue
            if not stripped.endswith(";"):
                line = f"{line.rstrip()};"

        normalized_lines.append(line)

    return "\n".join(normalized_lines) + "\n"


def _ensure_reference_bus_from_matpower_source(net: Any, contents: str) -> None:
    if not net.ext_grid.empty:
        return
    if "slack" in net.gen.columns and bool(net.gen["slack"].fillna(False).any()):
        return

    for row in _parse_matpower_matrix(contents, "bus"):
        if len(row) < 8:
            continue
        bus_id = int(float(row[0]))
        bus_type = int(float(row[1]))
        vm_pu = float(row[7])
        if bus_type != 3:
            continue

        candidate_indices = [bus_id - 1]
        if bus_id in net.bus.index:
            candidate_indices.insert(0, bus_id)

        chosen_index = next((idx for idx in candidate_indices if idx in net.bus.index), None)
        if chosen_index is None:
            continue

        pp.create_ext_grid(net, bus=chosen_index, vm_pu=vm_pu)
        return


def _parse_matpower_matrix(contents: str, matrix_name: str) -> list[list[str]]:
    normalized = _normalize_matpower_matrix_rows(contents)
    match = re.search(rf"mpc\.{re.escape(matrix_name)}\s*=\s*\[(.*?)\];", normalized, re.DOTALL)
    if not match:
        return []

    rows: list[list[str]] = []
    for raw_line in match.group(1).splitlines():
        stripped = raw_line.strip().rstrip(";")
        if not stripped:
            continue
        rows.append(stripped.split())
    return rows


__all__ = ["MatpowerImportAdapter", "import_matpower"]
