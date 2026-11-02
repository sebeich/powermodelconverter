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

    # Kept separate from import_case so the power-flow-only normalisation is an
    # explicit choice at the call site rather than a default nobody notices.
    def import_case_merged_generators(self, source: str | Path, **kwargs: Any) -> CanonicalCase:
        return import_matpower(source, merge_colocated_generators=True, **kwargs)


def import_matpower(
    path: str | Path, *, merge_colocated_generators: bool = False, **kwargs: Any
) -> CanonicalCase:
    """Import a MATPOWER case into the pandapower hub.

    `merge_colocated_generators` folds several units on one bus into a single
    voltage-controlling machine, restoring MATPOWER's bus-level reactive control
    (see `_merge_colocated_generators`). It is OFF by default because it is only
    correct for power flow: merging discards the units' individual cost curves,
    which changes the feasible set of any OPF built on the converted model. On
    pglib_opf_case5_pjm it moves the QC/SOC relaxation gap from 14.5407 % to
    14.6830 %, away from the published PGLib baseline of 14.55 %.
    """
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
    if merge_colocated_generators:
        _merge_colocated_generators(net)
    return PandapowerAdapter().to_canonical(
        net,
        case_id=source_path.stem,
        source_format="matpower",
        metadata={"import_backend": "pandapower.from_mpc"},
        source_path=source_path,
    )


def _merge_colocated_generators(net: Any) -> None:
    """Merge several MATPOWER units on one bus into a single controlling machine.

    MATPOWER has no per-machine voltage control: a bus is PV or slack, and the
    reactive output of every unit on it is free, jointly holding one setpoint.
    pandapower's converter keeps the first unit as the voltage-controlling
    element and turns the rest into `sgen`, whose reactive power is *fixed* at
    whatever Qg the case file happened to carry. The active powers survive that
    translation, but the bus then holds its voltage with only part of its
    reactive capability free, so the reactive dispatch -- and with it the voltage
    at surrounding PQ buses -- drifts from what MATPOWER computes. On the IEEE
    RTS-24 case, where six buses carry three to six units each, that is worth
    almost 0.04 pu.

    Folding the co-located units back into the controlling element restores
    MATPOWER's semantics: one voltage setpoint per bus, the full active power of
    the units behind it, and their reactive capability free. Buses with a single
    unit are untouched, and `sgen` rows the converter created for anything other
    than a generator (negative bus loads, for instance) are left alone -- only
    rows sharing a bus with a gen or the external grid are folded in.
    """
    if net.sgen.empty:
        return
    controlled = set(net.gen.bus) | set(net.ext_grid.bus)
    absorbed: list[int] = []
    for sgen_idx, sgen in net.sgen.iterrows():
        bus = int(sgen.bus)
        if bus not in controlled:
            continue
        target = net.gen.index[net.gen.bus == bus]
        if len(target):
            net.gen.loc[target[0], "p_mw"] += float(sgen.p_mw)
            for limit, contribution in (("max_p_mw", "p_mw"), ("min_p_mw", "p_mw"),
                                        ("max_q_mvar", "q_mvar"), ("min_q_mvar", "q_mvar")):
                if limit in net.gen.columns and limit in net.sgen.columns:
                    base = net.gen.loc[target[0], limit]
                    extra = sgen.get(limit)
                    if base == base and extra == extra:  # both non-NaN
                        net.gen.loc[target[0], limit] = float(base) + float(extra)
            if "sn_mva" in net.gen.columns and "sn_mva" in net.sgen.columns:
                base, extra = net.gen.loc[target[0], "sn_mva"], sgen.get("sn_mva")
                if base == base and extra == extra:
                    net.gen.loc[target[0], "sn_mva"] = float(base) + float(extra)
        # At the reference bus the external grid balances the system, so a unit
        # there needs no active-power transfer -- dropping the fixed injection is
        # what frees the slack to pick up the same total MATPOWER assigns it.
        absorbed.append(sgen_idx)
    if absorbed:
        net.sgen.drop(index=absorbed, inplace=True)


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
