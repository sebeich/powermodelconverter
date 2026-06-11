#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pypsa


MV_VOLTAGE_CUTOFF_KV = 220.0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Build a PyPSA source network with Europe-wide PyPSA-Eur HV AC network "
            "and a German MV overlay from the raw OSM base network."
        )
    )
    parser.add_argument("--hv-source", required=True, help="PyPSA-Eur electricity network for all countries")
    parser.add_argument("--de-mv-source", required=True, help="Germany raw OSM base.nc with MV/HV topology")
    parser.add_argument("--output", required=True, help="Output hybrid PyPSA NetCDF")
    parser.add_argument("--summary-output", default=None, help="Optional JSON summary path")
    return parser


def finite_float(value: Any, default: float = 0.0) -> float:
    try:
        result = float(value)
    except Exception:
        return default
    return result if math.isfinite(result) else default


def line_parameter_defaults(v_nom: float) -> tuple[float, float, float]:
    """Return r_ohm_per_km, x_ohm_per_km, c_nf_per_km for German MV/HV OSM lines."""
    if v_nom < 80.0:
        return 0.32, 0.38, 9.0
    if v_nom < 170.0:
        return 0.12, 0.36, 10.0
    return 0.06, 0.30, 12.0


def prefixed(name: Any) -> str:
    return f"de_mv__{name}"


def remove_many(network: pypsa.Network, component: str, names: Any) -> None:
    for name in list(names):
        network.remove(component, name)


def remove_non_ac_projection(network: pypsa.Network) -> pypsa.Network:
    n = network.copy()
    for component, attr in (("Link", "links"), ("Store", "stores"), ("StorageUnit", "storage_units")):
        table = getattr(n, attr)
        if len(table):
            remove_many(n, component, table.index)

    non_hv_buses = n.buses.index[n.buses.v_nom.astype(float) < MV_VOLTAGE_CUTOFF_KV]
    for component, attr, bus_columns in (
        ("Load", "loads", ("bus",)),
        ("Generator", "generators", ("bus",)),
        ("ShuntImpedance", "shunt_impedances", ("bus",)),
        ("Line", "lines", ("bus0", "bus1")),
        ("Transformer", "transformers", ("bus0", "bus1")),
    ):
        table = getattr(n, attr)
        if table.empty:
            continue
        mask = pd.Series(False, index=table.index)
        for bus_col in bus_columns:
            mask |= table[bus_col].isin(non_hv_buses)
        if bool(mask.any()):
            remove_many(n, component, table.index[mask])

    if len(non_hv_buses):
        remove_many(n, "Bus", non_hv_buses)
    return n


def build_hv_lookup(hv: pypsa.Network) -> tuple[np.ndarray, np.ndarray, list[str]]:
    candidates = hv.buses[
        (hv.buses.v_nom.astype(float) >= MV_VOLTAGE_CUTOFF_KV)
        & (hv.buses.country.astype(str) == "DE")
        & hv.buses.x.notna()
        & hv.buses.y.notna()
    ]
    if candidates.empty:
        candidates = hv.buses[
            (hv.buses.v_nom.astype(float) >= MV_VOLTAGE_CUTOFF_KV)
            & hv.buses.x.notna()
            & hv.buses.y.notna()
        ]
    if candidates.empty:
        raise ValueError("No geocoded HV buses available for MV transformer mapping.")

    coords = candidates[["x", "y"]].astype(float).to_numpy()
    voltages = candidates.v_nom.astype(float).to_numpy()
    names = [str(idx) for idx in candidates.index]
    return coords, voltages, names


def nearest_hv_bus(
    *,
    raw_bus: str,
    raw: pypsa.Network,
    hv_coords: np.ndarray,
    hv_voltages: np.ndarray,
    hv_names: list[str],
) -> tuple[str, float]:
    x = finite_float(raw.buses.at[raw_bus, "x"], float("nan"))
    y = finite_float(raw.buses.at[raw_bus, "y"], float("nan"))
    if not math.isfinite(x) or not math.isfinite(y):
        return hv_names[0], float("nan")

    raw_v = finite_float(raw.buses.at[raw_bus, "v_nom"], 380.0)
    voltage_penalty = np.abs(hv_voltages - raw_v) / 1000.0
    distances = np.hypot(hv_coords[:, 0] - x, hv_coords[:, 1] - y) + voltage_penalty
    pos = int(np.argmin(distances))
    return hv_names[pos], float(np.hypot(hv_coords[pos, 0] - x, hv_coords[pos, 1] - y))


def add_bus_from_raw(target: pypsa.Network, raw: pypsa.Network, raw_bus: str) -> str:
    name = prefixed(raw_bus)
    if name in target.buses.index:
        return name
    row = raw.buses.loc[raw_bus]
    target.add(
        "Bus",
        name,
        v_nom=finite_float(row.get("v_nom"), 110.0),
        x=finite_float(row.get("x"), 0.0),
        y=finite_float(row.get("y"), 0.0),
        country="DE",
        carrier="AC",
    )
    return name


def add_mv_overlay(hv: pypsa.Network, raw: pypsa.Network) -> dict[str, Any]:
    hv_coords, hv_voltages, hv_names = build_hv_lookup(hv)
    raw_bus_v = raw.buses.v_nom.astype(float)
    mv_raw_buses = set(str(idx) for idx in raw.buses.index[raw_bus_v < MV_VOLTAGE_CUTOFF_KV])

    added_buses: set[str] = set()
    added_lines = 0
    added_transformers = 0
    mapped_transformers: list[dict[str, Any]] = []

    for raw_bus in sorted(mv_raw_buses):
        added_buses.add(add_bus_from_raw(hv, raw, raw_bus))

    for line_name, row in raw.lines.iterrows():
        bus0 = str(row.bus0)
        bus1 = str(row.bus1)
        if bus0 not in mv_raw_buses or bus1 not in mv_raw_buses:
            continue

        target_bus0 = add_bus_from_raw(hv, raw, bus0)
        target_bus1 = add_bus_from_raw(hv, raw, bus1)
        added_buses.update((target_bus0, target_bus1))

        length = max(finite_float(row.get("length"), 1.0), 0.001)
        parallel = max(int(round(finite_float(row.get("num_parallel"), 1.0))), 1)
        v_nom = min(finite_float(raw.buses.at[bus0, "v_nom"], 110.0), finite_float(raw.buses.at[bus1, "v_nom"], 110.0))
        r_per_km, x_per_km, c_nf_per_km = line_parameter_defaults(v_nom)
        b = 2.0 * math.pi * finite_float(getattr(hv, "f_hz", 50.0), 50.0) * c_nf_per_km * 1e-9 * length * parallel
        s_nom = finite_float(row.get("s_nom"), 0.0)
        if s_nom <= 0.0:
            s_nom = math.sqrt(3.0) * v_nom * 0.6 * parallel

        hv.add(
            "Line",
            prefixed(line_name),
            bus0=target_bus0,
            bus1=target_bus1,
            r=r_per_km * length / parallel,
            x=x_per_km * length / parallel,
            b=b,
            g=0.0,
            s_nom=s_nom,
            length=length,
            num_parallel=parallel,
        )
        added_lines += 1

    for trafo_name, row in raw.transformers.iterrows():
        bus0 = str(row.bus0)
        bus1 = str(row.bus1)
        is_mv0 = bus0 in mv_raw_buses
        is_mv1 = bus1 in mv_raw_buses
        if not (is_mv0 or is_mv1):
            continue

        if is_mv0 and is_mv1:
            target_bus0 = add_bus_from_raw(hv, raw, bus0)
            target_bus1 = add_bus_from_raw(hv, raw, bus1)
            mapped_distance = None
        elif is_mv0:
            target_bus0 = add_bus_from_raw(hv, raw, bus0)
            target_bus1, mapped_distance = nearest_hv_bus(
                raw_bus=bus1,
                raw=raw,
                hv_coords=hv_coords,
                hv_voltages=hv_voltages,
                hv_names=hv_names,
            )
        else:
            target_bus0, mapped_distance = nearest_hv_bus(
                raw_bus=bus0,
                raw=raw,
                hv_coords=hv_coords,
                hv_voltages=hv_voltages,
                hv_names=hv_names,
            )
            target_bus1 = add_bus_from_raw(hv, raw, bus1)

        added_buses.update(bus for bus in (target_bus0, target_bus1) if bus.startswith("de_mv__"))
        r = max(finite_float(row.get("r"), 0.0), 0.002)
        x = max(finite_float(row.get("x"), 0.1), 0.02)
        s_nom = max(finite_float(row.get("s_nom"), 100.0), 1.0)
        hv.add(
            "Transformer",
            prefixed(trafo_name),
            bus0=target_bus0,
            bus1=target_bus1,
            model="t",
            s_nom=s_nom,
            r=r,
            x=x,
            g=max(finite_float(row.get("g"), 0.0), 0.0),
            b=finite_float(row.get("b"), 0.0),
            tap_ratio=1.0,
            tap_side=0,
            phase_shift=finite_float(row.get("phase_shift"), 0.0),
        )
        added_transformers += 1
        if mapped_distance is not None:
            mapped_transformers.append(
                {
                    "raw_transformer": str(trafo_name),
                    "raw_bus0": bus0,
                    "raw_bus1": bus1,
                    "target_bus0": str(target_bus0),
                    "target_bus1": str(target_bus1),
                    "distance_deg": mapped_distance,
                }
            )

    return {
        "added_mv_buses": len(added_buses),
        "added_mv_lines": added_lines,
        "added_mv_transformers": added_transformers,
        "hv_mapping_count": len(mapped_transformers),
        "hv_mapping_distance_deg": {
            "max": None if not mapped_transformers else max(item["distance_deg"] for item in mapped_transformers),
            "mean": None if not mapped_transformers else sum(item["distance_deg"] for item in mapped_transformers) / len(mapped_transformers),
        },
        "sample_hv_mappings": mapped_transformers[:20],
    }


def run(args: argparse.Namespace) -> int:
    hv_source = Path(args.hv_source)
    raw_source = Path(args.de_mv_source)
    output = Path(args.output)
    summary_output = Path(args.summary_output) if args.summary_output else output.with_suffix(".summary.json")

    hv = remove_non_ac_projection(pypsa.Network(hv_source))
    raw = pypsa.Network(raw_source)
    summary = add_mv_overlay(hv, raw)
    hv.name = "pypsa_eur_hv_all_de_mv_overlay"
    hv.determine_network_topology()

    output.parent.mkdir(parents=True, exist_ok=True)
    hv.export_to_netcdf(output)

    payload = {
        "hv_source": str(hv_source),
        "de_mv_source": str(raw_source),
        "output": str(output),
        **summary,
        "final": {
            "buses": int(len(hv.buses)),
            "lines": int(len(hv.lines)),
            "transformers": int(len(hv.transformers)),
            "links": int(len(hv.links)),
            "loads": int(len(hv.loads)),
            "generators": int(len(hv.generators)),
            "stores": int(len(hv.stores)),
            "storage_units": int(len(hv.storage_units)),
            "sub_networks": int(len(hv.sub_networks)),
            "v_nom_counts": {str(k): int(v) for k, v in hv.buses.v_nom.value_counts().sort_index().items()},
        },
    }
    summary_output.parent.mkdir(parents=True, exist_ok=True)
    summary_output.write_text(json.dumps(payload, indent=2))
    print(json.dumps(payload, indent=2))
    return 0


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    raise SystemExit(run(args))


if __name__ == "__main__":
    main()
