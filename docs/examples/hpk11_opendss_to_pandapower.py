#!/usr/bin/env python3
from __future__ import annotations

"""Build the HPK11 OpenDSS benchmark and import it into pandapower.

The source data comes from Team-Nando/MV-LV-Networks, network 3
(`Network_3_Urban_HPK11.xlsx`). The OpenDSS model generated here mirrors the
repository's `DSSDriver` static network construction and uses OpenDSS as the
validation reference for pmc's pandapower import route.
"""

import argparse
from dataclasses import asdict
import json
import math
from pathlib import Path
import shutil
from typing import Any

import pandas as pd
import pandapower as pp
from pandapower.toolbox import drop_buses

from powermodelconverter.core.pandapower_backend import PandapowerAdapter
from powermodelconverter.importers.opendss import OpenDSSImportAdapter
from powermodelconverter.validation.powerflow import ValidationService


REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_WORKBOOK = REPO_ROOT / "validation_cases" / "hpk11" / "source" / "Network_3_Urban_HPK11.xlsx"
DEFAULT_OUTPUT_DIR = REPO_ROOT / "artifacts" / "examples" / "hpk11"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build and validate the HPK11 OpenDSS pandapower example.")
    parser.add_argument("--workbook", default=str(DEFAULT_WORKBOOK), help="Path to Network_3_Urban_HPK11.xlsx")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR), help="Output directory")
    parser.add_argument(
        "--load-kw",
        type=float,
        default=1.0,
        help="Static kW assigned to each HPK11 customer load for the validation snapshot.",
    )
    parser.add_argument(
        "--backend-copy",
        default="",
        help="Optional path to copy the pandapower JSON for the powerflowserver backend.",
    )
    parser.add_argument("--allow-validation-failure", action="store_true", help="Write artifacts even if validation fails.")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    workbook = Path(args.workbook).resolve()
    output_dir = Path(args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    if not workbook.exists():
        raise FileNotFoundError(
            f"HPK11 workbook not found at {workbook}. "
            "Place Network_3_Urban_HPK11.xlsx under validation_cases/hpk11/source/."
        )

    data = read_hpk11_workbook(workbook)
    dss_path = output_dir / "hpk11_master.dss"
    dss_path.write_text("\n".join(build_opendss_commands(data, load_kw=args.load_kw)) + "\n")

    adapter = OpenDSSImportAdapter()
    reference = adapter.solve_source_case(dss_path)
    case = adapter.import_case(dss_path)
    case.net.name = "HPK11"
    attach_original_geodata(case.net, data)
    propagated_bus_count = assign_lv_feeder_pseudo_coords(case.net, data)
    numerical_equivalents = apply_hpk11_pandapower_numerical_equivalents(case.net)
    case.metadata.update(
        {
            "source": "Team-Nando/MV-LV-Networks",
            "network": "Network_3_Urban_HPK11",
            "validation_reference": "OpenDSS",
            "geodata_source": "original workbook sheets buscoords/connections",
            "geodata_crs": "original HPK11 local coordinates as published",
            "github": "https://github.com/Team-Nando/MV-LV-Networks",
            "snapshot_load_kw_per_customer": float(args.load_kw),
            "pandapower_numerical_equivalents": numerical_equivalents,
            "validation_tolerances": {"slack_mva": 0.13, "voltage_pu": 0.01},
        }
    )

    pandapower_json = PandapowerAdapter().export_json(case, output_dir / "hpk11.pandapower.json")

    validation_error = None
    try:
        validation = ValidationService().validate_opendss_unbalanced_roundtrip(
            case,
            reference,
            slack_tolerance_mva=0.13,
            voltage_tolerance_pu=0.01,
        )
    except Exception as exc:
        validation = None
        validation_error = f"{type(exc).__name__}: {exc}"

    if args.backend_copy:
        backend_copy = Path(args.backend_copy).expanduser().resolve()
        backend_copy.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(pandapower_json, backend_copy)

    summary = {
        "source_workbook": str(workbook),
        "opendss_master": str(dss_path),
        "pandapower_json": str(pandapower_json),
        "backend_copy": str(Path(args.backend_copy).expanduser().resolve()) if args.backend_copy else None,
        "validation": asdict(validation) if validation else None,
        "validation_error": validation_error,
        "counts": {
            "bus": len(case.net.bus),
            "line": len(case.net.line),
            "trafo": len(case.net.trafo),
            "load": len(case.net.load),
            "asymmetric_load": len(case.net.asymmetric_load),
            "ext_grid": len(case.net.ext_grid),
            "bus_geodata": len(case.net.bus_geodata),
            "bus_geodata_propagated": propagated_bus_count,
            "line_geodata": len(case.net.line_geodata),
        },
        "geodata_bounds": geodata_bounds(case.net),
    }
    summary_path = output_dir / "hpk11.summary.json"
    summary_path.write_text(json.dumps(summary, indent=2))
    print(json.dumps(summary, indent=2))
    if validation is None or not validation.passed:
        if args.allow_validation_failure:
            return
        raise SystemExit("HPK11 OpenDSS -> pandapower validation failed; see summary JSON.")


def read_hpk11_workbook(path: Path) -> dict[str, pd.DataFrame]:
    sheets = pd.ExcelFile(path).sheet_names
    return {sheet: pd.read_excel(path, sheet_name=sheet) for sheet in sheets}


def build_opendss_commands(data: dict[str, pd.DataFrame], *, load_kw: float) -> list[str]:
    commands: list[str] = [
        "clear",
        "Set DefaultBaseFrequency = 50",
        "New circuit.circuit basekv=66 pu=1 angle=0 phases=3 R1=0.52824 X1=2.113 R0=0.59157 X0=1.7747",
        "edit vsource.source bus1=sourcebus basekv=66 pu=1 angle=0 phases=3 R1=0.52824 X1=2.113 R0=0.59157 X0=1.7",
    ]

    for _, row in data["mv_net_txs"].iterrows():
        commands.append(
            f"New Transformer.{row['Substation_ID']} "
            "phases=3 windings=2 "
            f"buses=[{row['Bus1']}, mv_f0_n{row['Bus2']}] "
            f"conns=[{row['Connection_Primary']}, {row['Connection_Secondary']}] "
            f"kVs=[{row['kvs_primary']}, {row['kvs_secondary']}] "
            f"kVAs=[{row['kvas_primary']}, {row['kvas_secondary']}] "
            f"%loadloss={row['loadloss']} %noloadloss={row['noloadloss']} "
            f"xhl={row['xhl']} enabled=true"
        )

    for _, row in data["linecodes"].iterrows():
        commands.append(
            f"new linecode.lc_{row['Linecode_ID']} "
            f"nphases={int(row['Phases'])} r1={row['r1']} x1={row['x1']} b1={row['b1']} "
            f"r0={row['r0']} x0={row['x0']} b0={row['b0']} units={row['Units']} "
            f"normamp={min(float(row['Ampacity1']), float(row['Ampacity2']))}"
        )

    for _, row in data["lines"].iterrows():
        if str(row["Element_Name"]).lower() == "delete":
            continue
        commands.append(
            f"new line.mv_f0_l{row['Line_Number']} "
            f"bus1=mv_f0_n{row['Start_Node']}.{row['Start_Node_Phase']} "
            f"bus2=mv_f0_n{row['End_Node']}.{row['End_Node_Phase']} "
            f"phases={int(row['Phases'])} length={row['Length']} units={row['Units']} "
            f"linecode=lc_{row['Linecode']}-{int(row['Phases'])}ph enabled=true"
        )

    for index, row in data["lvtx"].iterrows():
        commands.append(
            f"new transformer.mv_f0_lv_{row['Substation_ID']} "
            "phases=3 windings=2 "
            f"buses=[mv_f0_n{row['Bus1']} mv_f0_lv{index}_busbar] "
            f"conns=[{row['Connection_Primary']} {row['Connection_Secondary']}] "
            f"kVs=[{row['kvs_primary']} {row['kvs_secondary']}] "
            f"kVAs=[{row['kvas_primary']} {row['kvas_secondary']}] "
            f"xhl={row['xhl']} %noloadloss={row['noloadloss']} %loadloss={row['loadloss']} "
            "wdg=1 numtaps=4 "
            f"tap={row['wdg1_tap']} maxtap=1.137 mintap=1.028"
        )

    for _, row in data["lv_lines"].iterrows():
        bus_conn = ".1.2.3" if int(row["phases"]) == 3 else ".1"
        commands.append(
            f"new line.{row['line_name']} bus1={row['bus1']}{bus_conn} bus2={row['bus2']}{bus_conn} "
            f"phases={int(row['phases'])} length={row['length']} units={row['units']} linecode={row['linecode']}"
        )

    for _, row in data["lv_loads"].iterrows():
        commands.append(
            f"new load.{row['load_name']} phases={int(row['phases'])} bus1={row['bus1']} "
            f"kw={float(load_kw)} conn=wye kv={row['kv']} pf={row['pf']} model=1 "
            f"vminpu=0.0 vmaxpu=2 status={row['model']} enabled=True"
        )

    # Build VoltageBases from actual transformer kV ratings so OpenDSS and
    # pandapower agree on the per-unit base at every voltage level.
    # Including both the L-L and L-N equivalent so calcv can match either.
    vbase_ll: set[float] = {66.0, 22.0}
    for _, row in data["mv_net_txs"].iterrows():
        vbase_ll.add(float(row["kvs_primary"]))
        vbase_ll.add(float(row["kvs_secondary"]))
    for _, row in data["lvtx"].iterrows():
        vbase_ll.add(float(row["kvs_primary"]))
        vbase_ll.add(float(row["kvs_secondary"]))
    vbase_all = vbase_ll | {round(v / math.sqrt(3), 6) for v in vbase_ll}
    vbases_str = " ".join(str(v) for v in sorted(vbase_all, reverse=True))
    commands.extend(
        [
            f"Set VoltageBases=[{vbases_str}]",
            "calcv",
            "set mode=snapshot",
            "solve",
        ]
    )
    return commands


def attach_original_geodata(net: pp.pandapowerNet, data: dict[str, pd.DataFrame]) -> None:
    buscoords = {
        int(row.Node_ID): (float(row.NodeStartX), float(row.NodeStartY))
        for row in data["buscoords"].itertuples(index=False)
    }
    if "bus_geodata" not in net or net.bus_geodata is None:
        net["bus_geodata"] = pd.DataFrame(columns=["x", "y"])
    if "line_geodata" not in net or net.line_geodata is None:
        net["line_geodata"] = pd.DataFrame(columns=["coords"])

    for bus_idx, row in net.bus.iterrows():
        name = str(row["name"]).lower()
        coord = None
        if name == "sourcebus":
            mvtx = data["mv_net_txs"].iloc[0]
            coord = buscoords.get(int(mvtx["Bus2"]))
        elif name.startswith("mv_f0_n"):
            try:
                coord = buscoords.get(int(name.removeprefix("mv_f0_n")))
            except ValueError:
                coord = None
        elif "_busbar" in name and name.startswith("mv_f0_lv"):
            try:
                lv_index = int(name.split("_lv", 1)[1].split("_", 1)[0])
                lvtx = data["lvtx"].iloc[lv_index]
                coord = buscoords.get(int(lvtx["Bus1"]))
            except (ValueError, IndexError):
                coord = None
        if coord is not None:
            net.bus_geodata.loc[int(bus_idx), ["x", "y"]] = coord

    connection_by_element = {
        int(row.Element_ID): row
        for row in data["connections"].itertuples(index=False)
        if str(row.Type).lower() == "line"
    }
    for line_idx, row in net.line.iterrows():
        name = str(row["name"]).lower()
        if not name.startswith("mv_f0_l"):
            continue
        try:
            line_number = int(name.removeprefix("mv_f0_l"))
        except ValueError:
            continue
        source_rows = data["lines"].loc[data["lines"]["Line_Number"] == line_number]
        if source_rows.empty:
            continue
        source = source_rows.iloc[0]
        connection = connection_by_element.get(int(source["Element_ID"]))
        if connection is None:
            continue
        net.line_geodata.at[int(line_idx), "coords"] = [
            (float(connection.Xcor1), float(connection.Ycor1)),
                (float(connection.Xcor2), float(connection.Ycor2)),
        ]


def apply_hpk11_pandapower_numerical_equivalents(net: pp.pandapowerNet) -> dict[str, Any]:
    """Replace OpenDSS near-ideal elements with pandapower-stable equivalents.

    The OpenDSS importer already converts near-zero impedance connector lines to
    tiny physical lines (1e-6 ohm total) for runpp_3ph stability — do not replace
    those with bus-bus switches here, since runpp_3ph cannot robustly solve through
    zero-impedance switches. Only the near-ideal 66/22 kV source transformer is
    handled here (moved to a 22 kV slack) when the transformer can be identified.
    """

    removed_source_buses: list[int] = []
    if len(net.trafo):
        source_candidates = net.trafo.index[net.trafo["name"].astype(str).str.lower() == "hpk11"].tolist()
        if source_candidates:
            idx = source_candidates[0]
            source_trafo = net.trafo.loc[idx]
            old_source_bus = int(source_trafo.hv_bus)
            net.ext_grid.at[net.ext_grid.index[0], "bus"] = int(source_trafo.lv_bus)
            net.ext_grid.at[net.ext_grid.index[0], "va_degree"] = -30.0
            net.trafo.drop(index=[idx], inplace=True)
            drop_buses(net, [old_source_bus], drop_elements=True)
            removed_source_buses.append(old_source_bus)

    # Replace only the synthetic tiny connector lines the importer created for
    # near-zero impedance elements (r_ohm_per_km == 1e-3, x_ohm_per_km == 1e-3).
    # Real LV linecodes always have r_ohm_per_km >> 0.002 (even heavy 400mm² cable
    # is ~0.05 ohm/km), so the 0.002 cutoff on both r AND x safely isolates only
    # the synthetic substitutes. These need to be switches (not tiny lines) because
    # runpp_3ph's Newton-Raphson ill-conditions on many near-zero-impedance lines.
    remove_lines: list[int] = []
    for idx, row in net.line.iterrows():
        if abs(float(row.r_ohm_per_km)) <= 0.002 and abs(float(row.x_ohm_per_km)) <= 0.002:
            pp.create_switch(
                net,
                bus=int(row.from_bus),
                element=int(row.to_bus),
                et="b",
                closed=True,
                type="LBS",
                name=f"hpk11_connector_{row['name']}",
            )
            remove_lines.append(int(idx))
    if remove_lines:
        net.line.drop(index=remove_lines, inplace=True)

    return {
        "source_transformer_represented_as_22kv_slack": bool(removed_source_buses),
        "removed_source_buses": removed_source_buses,
        "slack_angle_degree": -30.0 if removed_source_buses else 0.0,
        "synthetic_connector_lines_to_switches": len(remove_lines),
    }


def assign_lv_feeder_pseudo_coords(net: pp.pandapowerNet, data: dict[str, pd.DataFrame]) -> int:
    """Assign pseudo-geographic coordinates to LV customer buses.

    Lays out each transformer's LV network as a rectangular block of parallel
    feeder lines placed in the free space away from the nearest transformer:

    * Each feeder is a straight row; rows are stacked perpendicularly.
    * Bus positions along each row use actual cumulative LV line lengths scaled
      to fill the available along-feeder budget, preserving relative distances.
    * Row spacing fills the available perpendicular space (inter-transformer gap)
      so feeders are visually well separated with no label overlap.
    * The block is offset from the transformer toward the nearest-neighbour void.

    Coordinate scale: MV nodes use ~2.37 units/m.
    Along-feeder budget: 45% of nearest-transformer gap, max 280 units (~118 m).
    Perpendicular budget: 40% of nearest-transformer gap, max 260 units (~110 m).
    ROW_STEP = perp_budget / (n_feeders - 1), clamped to [20, 80] units.
    """
    import re
    from collections import deque

    name_to_idx: dict[str, int] = {
        str(net.bus.at[i, "name"]).lower(): int(i) for i in net.bus.index
    }

    # Build adjacency from lv_lines
    adj: dict[str, list[tuple[str, float]]] = {}
    for _, row in data["lv_lines"].iterrows():
        b1 = str(row.bus1).lower()
        b2 = str(row.bus2).lower()
        adj.setdefault(b1, []).append((b2, float(row.length)))
        adj.setdefault(b2, []).append((b1, float(row.length)))

    # Pre-collect each transformer's busbar position for offset-direction lookup
    busbar_pos: dict[int, tuple[float, float]] = {}
    for tx_idx, _ in data["lvtx"].iterrows():
        bname = f"mv_f0_lv{tx_idx}_busbar".lower()
        pp_idx = name_to_idx.get(bname)
        if pp_idx is not None and pp_idx in net.bus_geodata.index:
            x = net.bus_geodata.at[pp_idx, "x"]
            y = net.bus_geodata.at[pp_idx, "y"]
            if not (pd.isna(x) or pd.isna(y)):
                busbar_pos[int(tx_idx)] = (float(x), float(y))

    assigned_count = 0

    for tx_idx, _ in data["lvtx"].iterrows():
        tx_idx = int(tx_idx)
        busbar_name = f"mv_f0_lv{tx_idx}_busbar"
        busbar_pp = name_to_idx.get(busbar_name.lower())
        if busbar_pp is None or tx_idx not in busbar_pos:
            continue

        bx, by = busbar_pos[tx_idx]

        # Direction away from nearest other transformer busbar
        nearest_dist = float("inf")
        away_dx, away_dy = 0.0, 1.0
        for other_idx, (ox, oy) in busbar_pos.items():
            if other_idx == tx_idx:
                continue
            dx, dy = bx - ox, by - oy
            d = math.sqrt(dx * dx + dy * dy)
            if d < nearest_dist:
                nearest_dist = d
                away_dx, away_dy = dx, dy

        norm = math.sqrt(away_dx ** 2 + away_dy ** 2)
        if norm > 0:
            away_dx /= norm
            away_dy /= norm

        # Perpendicular direction (90 deg CCW rotation)
        perp_dx, perp_dy = -away_dy, away_dx

        # Discover feeder roots from bus name pattern mv_f0_lv{N}_f{FEEDER}_*
        feeder_roots: dict[int, str] = {}
        for neighbor, _ in adj.get(busbar_name.lower(), []):
            m = re.search(r"_lv\d+_f(\d+)_", neighbor)
            fnum = int(m.group(1)) if m else 0
            if fnum not in feeder_roots:
                feeder_roots[fnum] = neighbor

        if not feeder_roots:
            continue

        # BFS from each feeder root tracking cumulative line-length (metres).
        # Preserves relative spacing: buses on longer lines are further apart.
        feeder_buses: dict[int, list[tuple[str, float]]] = {}
        all_visited: set[str] = {busbar_name.lower()}
        for fnum, root_name in sorted(feeder_roots.items()):
            all_visited.add(root_name.lower())
            queue: deque[tuple[str, float]] = deque([(root_name, 0.0)])
            ordered: list[tuple[str, float]] = [(root_name, 0.0)]
            while queue:
                cur, cum_dist = queue.popleft()
                for child, length_m in adj.get(cur.lower(), []):
                    if child.lower() not in all_visited:
                        all_visited.add(child.lower())
                        child_dist = cum_dist + length_m
                        queue.append((child, child_dist))
                        ordered.append((child, child_dist))
            feeder_buses[fnum] = ordered

        # Maximum cumulative distance across all feeders (metres)
        max_cum_dist_m = max(
            (d for buses in feeder_buses.values() for _, d in buses),
            default=1.0,
        )

        # Along-feeder: scale actual distances to fill the available budget
        along_budget = min(nearest_dist * 0.45, 280.0)
        dist_scale = along_budget / max_cum_dist_m   # coord-units per metre

        n_feeders = len(feeder_buses)

        # Perpendicular: divide available width evenly across feeders
        perp_budget = min(nearest_dist * 0.40, 260.0)
        row_step = perp_budget / max(n_feeders - 1, 1)
        row_step = max(20.0, min(row_step, 80.0))

        total_perp = row_step * (n_feeders - 1)

        # Small initial gap between transformer and block start
        offset_along = min(nearest_dist * 0.05, 30.0)
        origin_x = bx + away_dx * offset_along - perp_dx * total_perp / 2.0
        origin_y = by + away_dy * offset_along - perp_dy * total_perp / 2.0

        for row_i, (fnum, buses) in enumerate(sorted(feeder_buses.items())):
            for bus_name, cum_dist_m in buses:
                pp_idx = name_to_idx.get(bus_name.lower())
                if pp_idx is not None:
                    d = cum_dist_m * dist_scale
                    x = origin_x + away_dx * d + perp_dx * row_i * row_step
                    y = origin_y + away_dy * d + perp_dy * row_i * row_step
                    net.bus_geodata.loc[pp_idx, ["x", "y"]] = [x, y]
                    assigned_count += 1

    return assigned_count

def geodata_bounds(net: pp.pandapowerNet) -> dict[str, float | int]:
    geodata = net.bus_geodata.dropna()
    if geodata.empty:
        return {"count": 0}
    return {
        "count": int(len(geodata)),
        "x_min": float(geodata.x.min()),
        "x_max": float(geodata.x.max()),
        "y_min": float(geodata.y.min()),
        "y_max": float(geodata.y.max()),
    }


if __name__ == "__main__":
    main()
