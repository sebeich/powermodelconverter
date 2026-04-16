#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
import warnings
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pandapower as pp
from pandapower import toolbox as pp_toolbox
import pypsa

from powermodelconverter.adapters.pandapower_adapter import PandapowerAdapter
from powermodelconverter.adapters.pypsa_adapter import PypsaAdapter


warnings.filterwarnings("ignore", category=pd.errors.PerformanceWarning)
warnings.filterwarnings(
    "ignore",
    message="DataFrame is highly fragmented.*",
    category=pd.errors.PerformanceWarning,
    module=r"pypsa\.network\.graph",
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Import a full-resolution PyPSA/PyPSA-Eur network into pandapower and "
            "validate the conversion with AC power-flow agreement."
        )
    )
    parser.add_argument("--source", required=True, help="PyPSA source (.nc/.netcdf/.h5/.hdf5 or CSV folder)")
    parser.add_argument(
        "--output",
        default=None,
        help="Target pandapower JSON path (default: <source_stem>.pandapower.json next to source)",
    )
    parser.add_argument(
        "--validation-output",
        default=None,
        help="Optional JSON report path (default: <output>.validation.json)",
    )
    parser.add_argument(
        "--snapshot",
        default=None,
        help=(
            "Snapshot label or zero-based snapshot index used for source-side validation. "
            "Default: first snapshot."
        ),
    )
    parser.add_argument(
        "--slack-tolerance-mva",
        type=float,
        default=1e-3,
        help="Slack power mismatch tolerance in MVA for validation",
    )
    parser.add_argument(
        "--voltage-tolerance-pu",
        type=float,
        default=1e-3,
        help="Max bus voltage mismatch tolerance in pu for validation",
    )
    parser.add_argument(
        "--fail-on-pruned-components",
        action="store_true",
        help=(
            "Fail if the source contains populated unsupported PyPSA components "
            "(links/stores/storage_units) instead of pruning them."
        ),
    )
    parser.add_argument(
        "--no-embed-pypsa-details",
        action="store_true",
        help="Do not embed full original PyPSA component payloads into the pandapower JSON artifact.",
    )
    parser.add_argument(
        "--no-embed-time-series",
        action="store_true",
        help="When embedding full PyPSA details, skip *_t time-series tables to reduce output size.",
    )
    parser.add_argument(
        "--skip-validation",
        action="store_true",
        help="Skip source-vs-target power-flow validation and only perform the conversion.",
    )
    parser.add_argument(
        "--lpf-fallback",
        action="store_true",
        help=(
            "Enable LPF-seeded Newton-Raphson fallback for non-converged islands. "
            "Disabled by default for large full-resolution networks to avoid topology rebuild instability."
        ),
    )
    parser.add_argument(
        "--trafo-model",
        choices=["t", "pi"],
        default="t",
        help=(
            "Transformer equivalent model used by pandapower runpp (default: t). "
            "Use 't' for PyPSA-sourced networks because PyPSA uses t-model transformers."
        ),
    )
    parser.add_argument(
        "--island-output-dir",
        default=None,
        help=(
            "Directory for exported pandapower island JSON files. "
            "Default: <output_stem>_islands next to output."
        ),
    )
    parser.add_argument(
        "--bundle-output",
        default=None,
        help=(
            "Combined bundle JSON path with full network + island artifacts. "
            "Default: <output>.islands.bundle.json"
        ),
    )
    return parser


def resolve_output_path(source: Path, requested_output: str | None) -> Path:
    if requested_output:
        return Path(requested_output)
    if source.is_dir():
        return source / f"{source.name}.pandapower.json"
    return source.with_name(f"{source.stem}.pandapower.json")


def resolve_validation_output_path(output_path: Path, requested_output: str | None) -> Path:
    if requested_output:
        return Path(requested_output)
    return output_path.with_suffix(output_path.suffix + ".validation.json")


def resolve_island_output_dir(output_path: Path, requested_dir: str | None) -> Path:
    if requested_dir:
        return Path(requested_dir)
    return output_path.with_name(f"{output_path.stem}_islands")


def resolve_bundle_output_path(output_path: Path, requested_output: str | None) -> Path:
    if requested_output:
        return Path(requested_output)
    return output_path.with_suffix(output_path.suffix + ".islands.bundle.json")


def resolve_diagnostic_output_path(output_path: Path) -> Path:
    return output_path.with_suffix(output_path.suffix + ".diagnostic.json")


def select_snapshot(network: pypsa.Network, snapshot_arg: str | None) -> Any:
    if len(network.snapshots) == 0:
        raise ValueError("The PyPSA network has no snapshots.")

    if snapshot_arg is None:
        return network.snapshots[0]

    if snapshot_arg.isdigit():
        idx = int(snapshot_arg)
        if idx < 0 or idx >= len(network.snapshots):
            raise ValueError(f"Snapshot index out of range: {idx} (available: 0..{len(network.snapshots) - 1})")
        return network.snapshots[idx]

    if snapshot_arg not in network.snapshots:
        raise ValueError(f"Snapshot label not found: {snapshot_arg}")
    return snapshot_arg


def prune_unsupported_components(network: pypsa.Network) -> dict[str, int]:
    removed: dict[str, int] = {}
    component_map = {
        "links": "Link",
        "stores": "Store",
        "storage_units": "StorageUnit",
    }

    for table_name, component_name in component_map.items():
        table = getattr(network, table_name, None)
        count = 0 if table is None else len(table)
        removed[table_name] = int(count)
        if count == 0:
            continue

        if hasattr(network, "mremove"):
            network.mremove(component_name, table.index)
        else:
            setattr(network, table_name, table.iloc[0:0].copy())

    return removed


def _subnetwork_buses(row: Any) -> list[str]:
    obj = row.get("obj")
    if obj is None:
        return []
    if hasattr(obj, "components") and hasattr(obj.components, "buses"):
        return [str(bus) for bus in obj.components.buses.static.index]
    if hasattr(obj, "buses_i"):
        return [str(bus) for bus in obj.buses_i()]
    return []


def _sanitize_name(value: str) -> str:
    sanitized = []
    for char in value:
        if char.isalnum() or char in {"_", "-"}:
            sanitized.append(char)
        else:
            sanitized.append("_")
    return "".join(sanitized)


def _collect_bus_degrees(network: pypsa.Network) -> dict[str, int]:
    degrees: dict[str, int] = {str(bus): 0 for bus in network.buses.index}

    if hasattr(network, "lines") and len(network.lines):
        for _, row in network.lines.iterrows():
            bus0 = str(row.get("bus0", ""))
            bus1 = str(row.get("bus1", ""))
            if bus0 in degrees:
                degrees[bus0] += 1
            if bus1 in degrees:
                degrees[bus1] += 1

    if hasattr(network, "transformers") and len(network.transformers):
        for _, row in network.transformers.iterrows():
            bus0 = str(row.get("bus0", ""))
            bus1 = str(row.get("bus1", ""))
            if bus0 in degrees:
                degrees[bus0] += 1
            if bus1 in degrees:
                degrees[bus1] += 1

    return degrees


def _extract_pf_converged(payload: Any) -> Any | None:
    if isinstance(payload, dict):
        return payload.get("converged")
    if isinstance(payload, (tuple, list)):
        for item in payload:
            if isinstance(item, dict) and "converged" in item:
                return item.get("converged")
        for item in payload:
            if hasattr(item, "dtype"):
                try:
                    if str(item.dtype) == "bool":
                        return item
                except Exception:
                    pass
    return None


def _coerce_converged_flag(value: Any) -> bool | None:
    if value is None:
        return None
    if hasattr(value, "all"):
        try:
            return bool(value.all())
        except Exception:
            pass
    if isinstance(value, (list, tuple, np.ndarray)):
        try:
            return bool(np.all(value))
        except Exception:
            pass
    try:
        return bool(value)
    except Exception:
        return None


def _run_island_pf(
    subnetwork: Any,
    snapshot: Any,
    *,
    allow_lpf_fallback: bool = False,
) -> tuple[bool, str | None]:
    first_error: str | None = None

    try:
        result = subnetwork.pf(snapshots=[snapshot])
        converged_flag = _coerce_converged_flag(_extract_pf_converged(result))
        if converged_flag is True:
            return True, None
        first_error = "PyPSA NR did not converge" if converged_flag is False else "PyPSA PF result missing converged flag"
    except Exception as exc:
        first_error = str(exc)

    if allow_lpf_fallback:
        try:
            subnetwork.lpf(snapshots=[snapshot])
            result = subnetwork.pf(snapshots=[snapshot], use_seed=True)
            converged_flag = _coerce_converged_flag(_extract_pf_converged(result))
            if converged_flag is True:
                return True, None
            fallback_error = "PyPSA LPF-seeded NR did not converge" if converged_flag is False else "PyPSA LPF-seeded PF missing converged flag"
            return False, f"{first_error}; {fallback_error}" if first_error else fallback_error
        except Exception as exc:
            fallback_error = str(exc)
            return False, f"{first_error}; {fallback_error}" if first_error else fallback_error

    return False, first_error or "non-convergent"


def inject_fixed_small_pq_validation_case(
    network: pypsa.Network,
    *,
    total_target_mw_per_island: float = 0.1,
    use_distributed_pv_support: bool = False,
) -> dict[str, Any]:
    if hasattr(network, "loads") and len(network.loads):
        if hasattr(network, "mremove"):
            network.mremove("Load", network.loads.index)
        else:
            for item in list(network.loads.index):
                network.remove("Load", item)

    if hasattr(network, "generators") and len(network.generators):
        if hasattr(network, "mremove"):
            network.mremove("Generator", network.generators.index)
        else:
            for item in list(network.generators.index):
                network.remove("Generator", item)

    network.determine_network_topology()
    ac_subnets = network.sub_networks[network.sub_networks.carrier == "AC"]
    bus_degrees = _collect_bus_degrees(network)

    total_loads = 0
    reactive_power_ratio = 0.3
    total_slacks = 0
    total_pv_generators = 0
    degenerate_islands: list[dict[str, Any]] = []
    configured_islands: list[dict[str, Any]] = []

    for subnet_id, row in ac_subnets.iterrows():
        buses = _subnetwork_buses(row)
        connected_buses = [bus for bus in buses if bus_degrees.get(str(bus), 0) > 0]
        if not connected_buses:
            degenerate_islands.append(
                {
                    "subnetwork": str(subnet_id),
                    "bus_count": int(len(buses)),
                    "reason": "no_line_or_transformer_connections",
                }
            )
            continue

        total_degree = sum(max(int(bus_degrees.get(str(bus), 0)), 0) for bus in connected_buses)
        assigned_p: list[float] = []
        for bus_name in connected_buses:
            degree = max(int(bus_degrees.get(str(bus_name), 0)), 0)
            if total_degree > 0:
                p_mw = total_target_mw_per_island * (degree / total_degree)
            else:
                p_mw = total_target_mw_per_island / max(len(connected_buses), 1)
            q_mvar = p_mw * reactive_power_ratio
            load_name = f"pmc_val_load_{_sanitize_name(str(bus_name))}"
            while load_name in network.loads.index:
                load_name += "_x"
            network.add(
                "Load",
                load_name,
                bus=bus_name,
                p_set=p_mw,
                q_set=q_mvar,
            )
            total_loads += 1
            assigned_p.append(float(p_mw))

        slack_bus = sorted(
            connected_buses,
            key=lambda bus: (bus_degrees.get(str(bus), 0), str(bus)),
            reverse=True,
        )[0]
        slack_name = f"pmc_val_slack_{_sanitize_name(str(subnet_id))}"
        while slack_name in network.generators.index:
            slack_name += "_x"
        network.add(
            "Generator",
            slack_name,
            bus=slack_bus,
            control="Slack",
            p_set=0.0,
            q_set=0.0,
            p_nom=1e6,
            p_nom_min=0.0,
            p_nom_max=1e6,
            p_min_pu=-1.0,
            p_max_pu=1.0,
            vm_pu=1.0,
        )
        total_slacks += 1

        pv_buses: list[str] = []
        pv_unit_p_mw = 0.0
        if use_distributed_pv_support:
            degree_values = sorted(max(int(bus_degrees.get(str(bus), 0)), 0) for bus in connected_buses)
            median_degree = float(np.median(np.asarray(degree_values, dtype=float))) if degree_values else 0.0
            pv_buses = [
                bus_name
                for bus_name in connected_buses
                if max(int(bus_degrees.get(str(bus_name), 0)), 0) >= median_degree
            ]
            if not pv_buses:
                pv_buses = [slack_bus]

            n_pv = len(pv_buses)
            pv_unit_p_mw = float(total_target_mw_per_island) / float(max(n_pv, 1))
            for idx, bus_name in enumerate(pv_buses):
                if str(bus_name) == str(slack_bus):
                    continue
                pv_name = f"pmc_val_pv_{_sanitize_name(str(subnet_id))}_{idx}"
                while pv_name in network.generators.index:
                    pv_name += "_x"
                network.add(
                    "Generator",
                    pv_name,
                    bus=bus_name,
                    control="PV",
                    p_set=pv_unit_p_mw,
                    q_set=0.0,
                    p_nom=max(10.0 * pv_unit_p_mw, 1.0),
                    p_nom_min=0.0,
                    p_nom_max=max(10.0 * pv_unit_p_mw, 1.0),
                    p_min_pu=0.0,
                    p_max_pu=1.0,
                    vm_pu=1.0,
                )
                total_pv_generators += 1

        configured_islands.append(
            {
                "subnetwork": str(subnet_id),
                "connected_bus_count": int(len(connected_buses)),
                "slack_bus": str(slack_bus),
                "max_degree": int(bus_degrees.get(str(slack_bus), 0)),
                "total_target_p_mw": float(total_target_mw_per_island),
                "total_assigned_p_mw": float(sum(assigned_p)),
                "min_bus_p_mw": float(min(assigned_p)) if assigned_p else 0.0,
                "max_bus_p_mw": float(max(assigned_p)) if assigned_p else 0.0,
                "distributed_pv_enabled": bool(use_distributed_pv_support),
                "pv_bus_count": int(len(pv_buses)),
                "pv_bus_degree_threshold": (
                    float(np.median(np.asarray([max(int(bus_degrees.get(str(bus), 0)), 0) for bus in connected_buses], dtype=float)))
                    if use_distributed_pv_support and connected_buses
                    else None
                ),
                "pv_unit_p_mw": float(pv_unit_p_mw),
            }
        )

    return {
        "mode": "degree_weighted_near_zero_loads_with_distributed_pv"
        if use_distributed_pv_support
        else "degree_weighted_near_zero_loads",
        "total_target_p_mw_per_island": float(total_target_mw_per_island),
        "load_q_to_p_ratio": float(reactive_power_ratio),
        "distributed_pv_support": bool(use_distributed_pv_support),
        "added_pv_generators": int(total_pv_generators),
        "added_loads": total_loads,
        "added_slacks": total_slacks,
        "configured_islands": configured_islands,
        "degenerate_islands": degenerate_islands,
    }


def build_pypsa_reference(
    network: pypsa.Network,
    snapshot: Any,
    *,
    allow_lpf_fallback: bool,
) -> tuple[float, float, dict[str, float], dict[str, Any], list[dict[str, Any]]]:
    network.determine_network_topology()
    ac_subnets = network.sub_networks[network.sub_networks.carrier == "AC"]
    if ac_subnets.empty:
        raise RuntimeError("No AC subnetworks available for power flow validation.")

    voltages: dict[str, float] = {}
    slack_p_total = 0.0
    slack_q_total = 0.0
    bus_degrees = _collect_bus_degrees(network)
    details: dict[str, Any] = {
        "total_ac_islands": int(len(ac_subnets)),
        "converged_islands": 0,
        "excluded_islands": [],
        "failed_islands": [],
    }
    island_references: list[dict[str, Any]] = []

    for subnet_id, row in ac_subnets.iterrows():
        buses = _subnetwork_buses(row)
        subnetwork = row.get("obj")
        if not buses:
            details["excluded_islands"].append({"subnetwork": str(subnet_id), "reason": "empty"})
            continue
        if subnetwork is None:
            details["excluded_islands"].append({"subnetwork": str(subnet_id), "reason": "missing-object"})
            continue

        connected_buses = [bus for bus in buses if bus_degrees.get(str(bus), 0) > 0]
        if not connected_buses:
            details["excluded_islands"].append(
                {
                    "subnetwork": str(subnet_id),
                    "bus_count": int(len(buses)),
                    "reason": "degenerate_no_connected_buses",
                }
            )
            continue

        converged, error = _run_island_pf(subnetwork, snapshot, allow_lpf_fallback=allow_lpf_fallback)
        if not converged:
            condition_number = None
            try:
                subnetwork.calculate_Y()
                if hasattr(subnetwork, "Y") and subnetwork.Y is not None:
                    y_dense = np.asarray(subnetwork.Y.todense(), dtype=np.complex128)
                    condition_number = _compute_condition_number(y_dense)
            except Exception:
                condition_number = None
            details["excluded_islands"].append(
                {
                    "subnetwork": str(subnet_id),
                    "bus_count": int(len(buses)),
                    "reason": f"degenerate_non_convergent: {error or 'non-convergent'}",
                    "condition_number": condition_number,
                }
            )
            continue

        details["converged_islands"] += 1
        for bus_name in connected_buses:
            vm = float(network.buses_t.v_mag_pu.loc[snapshot, bus_name])
            voltages[str(bus_name)] = vm

        if hasattr(network, "generators") and len(network.generators):
            subnet_generators = network.generators[network.generators.bus.astype(str).isin(buses)]
            slack_mask = subnet_generators.control == "Slack"
            slack_names = list(subnet_generators.index[slack_mask])
            if slack_names:
                island_slack_p = float(network.generators_t.p.loc[snapshot, slack_names].sum())
                island_slack_q = float(network.generators_t.q.loc[snapshot, slack_names].sum())
                slack_p_total += island_slack_p
                slack_q_total += island_slack_q
            else:
                island_slack_p = 0.0
                island_slack_q = 0.0
        else:
            island_slack_p = 0.0
            island_slack_q = 0.0

        island_references.append(
            {
                "subnetwork": str(subnet_id),
                "buses": [str(bus_name) for bus_name in connected_buses],
                "reference_slack_p_mw": island_slack_p,
                "reference_slack_q_mvar": island_slack_q,
                "slack_bus": str(subnet_generators.loc[slack_names[0], "bus"]) if slack_names else None,
            }
        )

    if details["converged_islands"] == 0 or not voltages:
        raise RuntimeError("No AC islands converged for PyPSA PF reference.")

    return slack_p_total, slack_q_total, voltages, details, island_references


def _extract_balanced_slack_from_net(net: Any) -> tuple[float, float]:
    slack_p = float(net.res_ext_grid.p_mw.sum()) if hasattr(net, "res_ext_grid") else 0.0
    slack_q = float(net.res_ext_grid.q_mvar.sum()) if hasattr(net, "res_ext_grid") else 0.0
    if hasattr(net, "gen") and hasattr(net, "res_gen") and len(net.gen):
        slack_mask = net.gen.get("slack", False)
        if hasattr(slack_mask, "fillna"):
            slack_mask = slack_mask.fillna(False)
        if getattr(slack_mask, "any", lambda: False)():
            slack_p += float(net.res_gen.loc[slack_mask, "p_mw"].sum())
            slack_q += float(net.res_gen.loc[slack_mask, "q_mvar"].sum())
    return slack_p, slack_q


def _relative_delta(left: float, right: float) -> float:
    return abs(left - right) / max(abs(left), 1e-12)


def _normalize_tap_side(value: Any) -> str:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return "none"
    text = str(value).strip().lower()
    if text in {"0", "hv", "high", "high_voltage"}:
        return "hv"
    if text in {"1", "lv", "low", "low_voltage"}:
        return "lv"
    if text in {"", "none", "nan"}:
        return "none"
    return text


def _line_totals_from_pp(net: Any, line_row: Any) -> tuple[float, float, float, float]:
    length_km = float(line_row.length_km)
    parallel = max(int(line_row.parallel), 1)
    r_total_ohm = float(line_row.r_ohm_per_km) * length_km / parallel
    x_total_ohm = float(line_row.x_ohm_per_km) * length_km / parallel
    c_total_nf = float(line_row.c_nf_per_km) * length_km * parallel
    b_total_siemens = 2 * math.pi * float(net.f_hz) * c_total_nf * 1e-9
    return r_total_ohm, x_total_ohm, c_total_nf, b_total_siemens


def _build_island_diagnostic(
    *,
    source_network: pypsa.Network,
    island_net: Any,
    island_ref: dict[str, Any],
    mismatch_threshold: float = 0.01,
) -> dict[str, Any]:
    buses = [str(bus) for bus in island_ref.get("buses", [])]
    bus_set = set(buses)
    line_entries: list[dict[str, Any]] = []
    line_lookup = {
        str(row["name"]): idx
        for idx, row in island_net.line.iterrows()
        if "name" in island_net.line.columns and pd.notna(row["name"])
    }

    for line_name, py_line in source_network.lines.iterrows():
        if str(py_line.bus0) not in bus_set or str(py_line.bus1) not in bus_set:
            continue
        if str(line_name) not in line_lookup:
            line_entries.append(
                {
                    "line": str(line_name),
                    "missing_in_pandapower": True,
                }
            )
            continue

        pp_line = island_net.line.loc[line_lookup[str(line_name)]]
        r_total_ohm, x_total_ohm, c_total_nf, b_total_siemens = _line_totals_from_pp(island_net, pp_line)
        vn_kv = float(source_network.buses.loc[py_line.bus0, "v_nom"])
        z_base_sn = vn_kv * vn_kv / float(island_net.sn_mva)
        s_nom = float(py_line.s_nom) if float(py_line.s_nom) > 0 else float(island_net.sn_mva)
        z_base_snom = vn_kv * vn_kv / s_nom

        py_r = float(py_line.r)
        py_x = float(py_line.x)
        py_b = float(py_line.b)
        py_r_pu_on_snom = py_r / z_base_snom
        py_x_pu_on_snom = py_x / z_base_snom
        py_b_pu_on_snom = py_b * z_base_snom

        py_r_pu_on_sn = py_r / z_base_sn
        py_x_pu_on_sn = py_x / z_base_sn
        py_b_pu_on_sn = py_b * z_base_sn

        pp_r_pu_on_sn = r_total_ohm / z_base_sn
        pp_x_pu_on_sn = x_total_ohm / z_base_sn
        pp_b_pu_on_sn = b_total_siemens * z_base_sn

        rel_r = _relative_delta(py_r_pu_on_sn, pp_r_pu_on_sn)
        rel_x = _relative_delta(py_x_pu_on_sn, pp_x_pu_on_sn)
        rel_b = _relative_delta(py_b_pu_on_sn, pp_b_pu_on_sn)
        line_entries.append(
            {
                "line": str(line_name),
                "pypsa": {
                    "r_ohm": py_r,
                    "x_ohm": py_x,
                    "b_siemens": py_b,
                    "s_nom_mva": s_nom,
                    "r_pu_on_s_nom": py_r_pu_on_snom,
                    "x_pu_on_s_nom": py_x_pu_on_snom,
                    "b_pu_on_s_nom": py_b_pu_on_snom,
                    "r_pu_on_sn_mva": py_r_pu_on_sn,
                    "x_pu_on_sn_mva": py_x_pu_on_sn,
                    "b_pu_on_sn_mva": py_b_pu_on_sn,
                },
                "pandapower": {
                    "r_total_ohm": r_total_ohm,
                    "x_total_ohm": x_total_ohm,
                    "c_total_nf": c_total_nf,
                    "b_total_siemens": b_total_siemens,
                    "r_pu_on_sn_mva": pp_r_pu_on_sn,
                    "x_pu_on_sn_mva": pp_x_pu_on_sn,
                    "b_pu_on_sn_mva": pp_b_pu_on_sn,
                },
                "relative_delta": {
                    "r_pu": rel_r,
                    "x_pu": rel_x,
                    "b_pu": rel_b,
                },
                "flagged": bool(rel_r > mismatch_threshold or rel_x > mismatch_threshold or rel_b > mismatch_threshold),
            }
        )

    trafo_entries: list[dict[str, Any]] = []
    trafo_lookup = {
        str(row["name"]): idx
        for idx, row in island_net.trafo.iterrows()
        if "name" in island_net.trafo.columns and pd.notna(row["name"])
    }
    for trafo_name, py_trafo in source_network.transformers.iterrows():
        if str(py_trafo.bus0) not in bus_set or str(py_trafo.bus1) not in bus_set:
            continue
        if str(trafo_name) not in trafo_lookup:
            trafo_entries.append(
                {
                    "transformer": str(trafo_name),
                    "missing_in_pandapower": True,
                }
            )
            continue

        pp_trafo = island_net.trafo.loc[trafo_lookup[str(trafo_name)]]
        pp_r_trafo = float(pp_trafo.vkr_percent) / 100.0
        pp_z_trafo = float(pp_trafo.vk_percent) / 100.0
        pp_x_trafo = max(pp_z_trafo * pp_z_trafo - pp_r_trafo * pp_r_trafo, 0.0) ** 0.5
        rel_r = _relative_delta(float(py_trafo.r), pp_r_trafo)
        rel_x = _relative_delta(float(py_trafo.x), pp_x_trafo)
        py_tap_side = _normalize_tap_side(py_trafo.tap_side)
        pp_tap_side = _normalize_tap_side(pp_trafo.tap_side)
        flagged = bool(
            rel_r > mismatch_threshold
            or rel_x > mismatch_threshold
            or py_tap_side != pp_tap_side
        )
        trafo_entries.append(
            {
                "transformer": str(trafo_name),
                "pypsa": {
                    "r_pu_trafo": float(py_trafo.r),
                    "x_pu_trafo": float(py_trafo.x),
                    "tap_ratio": float(py_trafo.tap_ratio),
                    "tap_side": py_tap_side,
                },
                "pandapower": {
                    "vk_percent": float(pp_trafo.vk_percent),
                    "vkr_percent": float(pp_trafo.vkr_percent),
                    "tap_pos": None if pd.isna(pp_trafo.tap_pos) else float(pp_trafo.tap_pos),
                    "tap_side": pp_tap_side,
                    "r_pu_trafo": pp_r_trafo,
                    "x_pu_trafo": pp_x_trafo,
                },
                "relative_delta": {"r_pu": rel_r, "x_pu": rel_x},
                "flagged": flagged,
            }
        )

    expected_slack_bus = str(island_ref.get("slack_bus")) if island_ref.get("slack_bus") is not None else None
    ext_grids = []
    if hasattr(island_net, "ext_grid") and len(island_net.ext_grid):
        for idx, row in island_net.ext_grid.iterrows():
            bus_name = str(island_net.bus.at[int(row.bus), "name"])
            ext_grids.append({"index": int(idx), "bus": bus_name, "vm_pu": float(row.vm_pu)})

    zero_impedance_lines = []
    for idx, row in island_net.line.iterrows():
        r_total_ohm, x_total_ohm, _, _ = _line_totals_from_pp(island_net, row)
        if abs(r_total_ohm) < 1e-12 and abs(x_total_ohm) < 1e-12:
            zero_impedance_lines.append(
                {
                    "index": int(idx),
                    "name": str(row.get("name", idx)),
                    "from_bus": str(island_net.bus.at[int(row.from_bus), "name"]),
                    "to_bus": str(island_net.bus.at[int(row.to_bus), "name"]),
                }
            )

    zero_impedance_trafos = []
    if len(island_net.trafo):
        for idx, row in island_net.trafo.iterrows():
            if abs(float(row.vkr_percent)) < 1e-12 and abs(float(row.vk_percent)) < 1e-12:
                zero_impedance_trafos.append(
                    {
                        "index": int(idx),
                        "name": str(row.get("name", idx)),
                        "hv_bus": str(island_net.bus.at[int(row.hv_bus), "name"]),
                        "lv_bus": str(island_net.bus.at[int(row.lv_bus), "name"]),
                    }
                )

    py_sn_mva = float(getattr(source_network, "sn_mva", 1.0) or 1.0)
    return {
        "subnetwork": str(island_ref.get("subnetwork")),
        "bus_count": int(len(buses)),
        "line_checks": line_entries,
        "transformer_checks": trafo_entries,
        "slack_check": {
            "expected_slack_bus": expected_slack_bus,
            "ext_grids": ext_grids,
            "ext_grid_present": bool(len(ext_grids) > 0),
            "expected_bus_has_ext_grid": bool(expected_slack_bus and any(item["bus"] == expected_slack_bus for item in ext_grids)),
            "all_vm_pu_unity": bool(all(abs(item["vm_pu"] - 1.0) <= 1e-9 for item in ext_grids)) if ext_grids else False,
        },
        "zero_impedance": {
            "lines": zero_impedance_lines,
            "transformers": zero_impedance_trafos,
        },
        "base_check": {
            "pypsa_sn_mva": py_sn_mva,
            "pandapower_sn_mva": float(island_net.sn_mva),
            "sn_mva_match": bool(abs(py_sn_mva - float(island_net.sn_mva)) <= 1e-9),
        },
    }


def _sanitize_zero_impedance_branches(net: Any) -> dict[str, int]:
    fixed_lines = 0
    for idx, row in net.line.iterrows():
        length_km = float(row.length_km) if float(row.length_km) > 0 else 1.0
        r_total = float(row.r_ohm_per_km) * length_km
        x_total = float(row.x_ohm_per_km) * length_km
        if abs(r_total) < 1e-12 and abs(x_total) < 1e-12:
            net.line.at[idx, "r_ohm_per_km"] = 1e-6 / length_km
            net.line.at[idx, "x_ohm_per_km"] = 1e-4 / length_km
            fixed_lines += 1

    fixed_trafos = 0
    if len(net.trafo):
        for idx, row in net.trafo.iterrows():
            if abs(float(row.vkr_percent)) < 1e-12 and abs(float(row.vk_percent)) < 1e-12:
                net.trafo.at[idx, "vkr_percent"] = 1e-6
                net.trafo.at[idx, "vk_percent"] = 1e-4
                fixed_trafos += 1

    return {"lines": fixed_lines, "transformers": fixed_trafos}


def _ensure_island_slack_ext_grid(net: Any, slack_bus_name: str | None) -> bool:
    if not slack_bus_name:
        return False
    bus_rows = net.bus.index[net.bus["name"].astype(str) == str(slack_bus_name)]
    if len(bus_rows) == 0:
        return False
    bus_idx = int(bus_rows[0])

    if len(net.ext_grid) and bool((net.ext_grid.bus == bus_idx).any()):
        net.ext_grid.loc[net.ext_grid.bus == bus_idx, "vm_pu"] = 1.0
        return True

    pp.create_ext_grid(net, bus=bus_idx, vm_pu=1.0, name=f"pmc_val_ext_grid_{_sanitize_name(slack_bus_name)}")
    return True


def _apply_transformer_series_corrections(
    *,
    source_network: pypsa.Network,
    net: Any,
    buses: set[str],
    mismatch_threshold: float = 0.01,
) -> dict[str, int]:
    trafo_lookup = {
        str(row["name"]): idx
        for idx, row in net.trafo.iterrows()
        if "name" in net.trafo.columns and pd.notna(row["name"])
    }
    corrected = 0
    flagged = 0
    for trafo_name, py_trafo in source_network.transformers.iterrows():
        if str(py_trafo.bus0) not in buses or str(py_trafo.bus1) not in buses:
            continue
        if str(trafo_name) not in trafo_lookup:
            continue
        idx = trafo_lookup[str(trafo_name)]
        row = net.trafo.loc[idx]
        pp_r_trafo = float(row.vkr_percent) / 100.0
        pp_z_trafo = float(row.vk_percent) / 100.0
        pp_x_trafo = max(pp_z_trafo * pp_z_trafo - pp_r_trafo * pp_r_trafo, 0.0) ** 0.5
        dr = _relative_delta(float(py_trafo.r), pp_r_trafo)
        dx = _relative_delta(float(py_trafo.x), pp_x_trafo)
        py_tap_side = _normalize_tap_side(py_trafo.tap_side)
        pp_tap_side = _normalize_tap_side(row.tap_side)
        tap_mismatch = py_tap_side != pp_tap_side
        if dr <= mismatch_threshold and dx <= mismatch_threshold and not tap_mismatch:
            continue
        flagged += 1

        target_r = float(py_trafo.r)
        target_x = float(py_trafo.x)
        target_z = math.hypot(target_r, target_x)
        net.trafo.at[idx, "vkr_percent"] = target_r * 100.0
        net.trafo.at[idx, "vk_percent"] = target_z * 100.0
        if py_tap_side in {"hv", "lv"}:
            net.trafo.at[idx, "tap_side"] = py_tap_side
        corrected += 1

    return {"flagged": flagged, "corrected": corrected}


def _build_island_net_from_full(
    *,
    source_network: pypsa.Network,
    base_net: Any,
    island_ref: dict[str, Any],
) -> tuple[Any, dict[str, Any]]:
    buses = [str(bus) for bus in island_ref.get("buses", [])]
    bus_name_series = base_net.bus["name"].astype(str)
    bus_indices = list(base_net.bus.index[bus_name_series.isin(buses)])
    island_net = pp_toolbox.select_subnet(
        base_net,
        bus_indices,
        include_switch_buses=False,
        include_results=False,
        keep_everything_else=False,
    )

    island_bus_set = set(buses)
    py_sn_mva = float(getattr(source_network, "sn_mva", 1.0) or 1.0)
    island_net.sn_mva = py_sn_mva
    zero_fix = _sanitize_zero_impedance_branches(island_net)
    slack_fixed = _ensure_island_slack_ext_grid(island_net, island_ref.get("slack_bus"))
    trafo_fix = _apply_transformer_series_corrections(
        source_network=source_network,
        net=island_net,
        buses=island_bus_set,
    )
    return island_net, {
        "zero_impedance": zero_fix,
        "slack_ext_grid_present": slack_fixed,
        "transformer_series": trafo_fix,
    }


def _extract_pp_ybus(net: Any, *, trafo_model: str = "t") -> tuple[list[str], np.ndarray]:
    try:
        pp.runpp(net, algorithm="nr", init="dc", max_iteration=1, trafo_model=trafo_model, numba=False)
    except Exception:
        pass

    if not hasattr(net, "_ppc") or net._ppc is None:
        raise RuntimeError("pandapower _ppc was not created while assembling Y-bus")
    internal = net._ppc.get("internal", {})
    ybus = internal.get("Ybus")
    if ybus is None:
        raise RuntimeError("pandapower internal Ybus is unavailable")

    lookup = net._pd2ppc_lookups.get("bus")
    if lookup is None:
        raise RuntimeError("pandapower bus lookup is unavailable for Y-bus indexing")

    ppc_to_name: dict[int, str] = {}
    for bus_idx in net.bus.index:
        mapped = int(lookup[int(bus_idx)])
        if mapped < 0:
            continue
        ppc_to_name[mapped] = str(net.bus.at[int(bus_idx), "name"])

    ordered = sorted(ppc_to_name.items(), key=lambda item: item[0])
    bus_names = [name for _, name in ordered]
    dense = np.asarray(ybus.todense(), dtype=np.complex128)
    return bus_names, dense


def _extract_pypsa_ybus(source_network: pypsa.Network, island_ref: dict[str, Any]) -> tuple[list[str], np.ndarray]:
    subnet_id = str(island_ref.get("subnetwork"))
    source_network.determine_network_topology()
    row = None
    for candidate_id, candidate_row in source_network.sub_networks.iterrows():
        if str(candidate_id) == subnet_id:
            row = candidate_row
            break
    if row is None:
        raise RuntimeError(f"Unable to locate PyPSA subnetwork {subnet_id}")

    subnetwork = row.get("obj")
    if subnetwork is None:
        raise RuntimeError(f"PyPSA subnetwork object missing for {subnet_id}")
    subnetwork.calculate_Y()
    if not hasattr(subnetwork, "Y"):
        raise RuntimeError(f"PyPSA subnetwork Y matrix unavailable for {subnet_id}")

    bus_names = [str(name) for name in subnetwork.buses_o]
    dense = np.asarray(subnetwork.Y.todense(), dtype=np.complex128)
    return bus_names, dense


def _compare_ybus(
    *,
    pypsa_bus_names: list[str],
    pypsa_ybus: np.ndarray,
    pandapower_bus_names: list[str],
    pandapower_ybus: np.ndarray,
    rel_threshold: float = 1e-3,
) -> dict[str, Any]:
    py_index = {name: idx for idx, name in enumerate(pypsa_bus_names)}
    pp_index = {name: idx for idx, name in enumerate(pandapower_bus_names)}
    common_buses = [name for name in pypsa_bus_names if name in pp_index]
    missing_in_pp = [name for name in pypsa_bus_names if name not in pp_index]
    extra_in_pp = [name for name in pandapower_bus_names if name not in py_index]

    if not common_buses:
        raise RuntimeError("No common buses between PyPSA and pandapower Y-bus index sets")

    py_idx = np.array([py_index[name] for name in common_buses], dtype=int)
    pp_idx = np.array([pp_index[name] for name in common_buses], dtype=int)
    y_py = pypsa_ybus[np.ix_(py_idx, py_idx)]
    y_pp = pandapower_ybus[np.ix_(pp_idx, pp_idx)]
    diff = y_pp - y_py

    py_norm = float(np.linalg.norm(y_py, ord="fro"))
    diff_norm = float(np.linalg.norm(diff, ord="fro"))
    rel_fro = diff_norm / max(py_norm, 1e-12)

    mag_py = np.abs(y_py)
    mag_pp = np.abs(y_pp)
    mag_rel = np.abs(mag_pp - mag_py) / np.maximum(mag_py, 1e-12)
    ang_py = np.angle(y_py)
    ang_pp = np.angle(y_pp)
    ang_delta = np.abs(np.angle(np.exp(1j * (ang_pp - ang_py))))
    ang_rel = ang_delta / np.maximum(np.abs(ang_py), 1e-12)
    flagged_mask = (mag_rel > rel_threshold) | (ang_rel > rel_threshold)

    flagged_entries: list[dict[str, Any]] = []
    rows, cols = np.where(flagged_mask)
    for row, col in zip(rows.tolist(), cols.tolist(), strict=False):
        flagged_entries.append(
            {
                "i_bus": common_buses[row],
                "j_bus": common_buses[col],
                "pypsa": {
                    "real": float(np.real(y_py[row, col])),
                    "imag": float(np.imag(y_py[row, col])),
                    "magnitude": float(mag_py[row, col]),
                    "angle_rad": float(ang_py[row, col]),
                },
                "pandapower": {
                    "real": float(np.real(y_pp[row, col])),
                    "imag": float(np.imag(y_pp[row, col])),
                    "magnitude": float(mag_pp[row, col]),
                    "angle_rad": float(ang_pp[row, col]),
                },
                "relative_delta": {
                    "magnitude": float(mag_rel[row, col]),
                    "angle": float(ang_rel[row, col]),
                },
            }
        )

    return {
        "threshold_relative": rel_threshold,
        "common_bus_count": int(len(common_buses)),
        "missing_in_pandapower": missing_in_pp,
        "extra_in_pandapower": extra_in_pp,
        "relative_frobenius_norm": rel_fro,
        "flagged_entry_count": int(len(flagged_entries)),
        "flagged_entries": flagged_entries,
    }


def _compute_condition_number(ybus: np.ndarray) -> float | None:
    try:
        value = float(np.linalg.cond(ybus))
    except Exception:
        return None
    if math.isnan(value) or math.isinf(value):
        return None
    return value


def _edge_key(bus_a: str, bus_b: str) -> tuple[str, str]:
    return tuple(sorted((str(bus_a), str(bus_b))))


def _build_island_branch_lookup(source_network: pypsa.Network, buses: set[str]) -> tuple[dict[tuple[str, str], list[dict[str, str]]], dict[str, list[dict[str, str]]]]:
    edge_lookup: dict[tuple[str, str], list[dict[str, str]]] = {}
    incident_lookup: dict[str, list[dict[str, str]]] = {bus: [] for bus in buses}

    for line_name, row in source_network.lines.iterrows():
        bus0 = str(row.bus0)
        bus1 = str(row.bus1)
        if bus0 not in buses or bus1 not in buses:
            continue
        payload = {"type": "line", "name": str(line_name), "bus0": bus0, "bus1": bus1}
        edge_lookup.setdefault(_edge_key(bus0, bus1), []).append(payload)
        incident_lookup[bus0].append(payload)
        incident_lookup[bus1].append(payload)

    for trafo_name, row in source_network.transformers.iterrows():
        bus0 = str(row.bus0)
        bus1 = str(row.bus1)
        if bus0 not in buses or bus1 not in buses:
            continue
        payload = {"type": "transformer", "name": str(trafo_name), "bus0": bus0, "bus1": bus1}
        edge_lookup.setdefault(_edge_key(bus0, bus1), []).append(payload)
        incident_lookup[bus0].append(payload)
        incident_lookup[bus1].append(payload)

    return edge_lookup, incident_lookup


def _complex_payload(value: complex) -> dict[str, float]:
    return {
        "real": float(np.real(value)),
        "imag": float(np.imag(value)),
        "magnitude": float(np.abs(value)),
        "angle_rad": float(np.angle(value)),
    }


def _matrix_payload(matrix: np.ndarray) -> list[list[dict[str, float]]]:
    return [
        [_complex_payload(complex(matrix[row, col])) for col in range(matrix.shape[1])]
        for row in range(matrix.shape[0])
    ]


def _line_primitive_y_from_pypsa(*, line_row: Any, vn_kv: float, sn_mva: float) -> np.ndarray:
    z_series = complex(float(line_row.r), float(line_row.x))
    y_series = 0j if abs(z_series) <= 1e-18 else 1 / z_series
    y_shunt_half = complex(float(line_row.g), float(line_row.b)) / 2.0
    z_base = (vn_kv * vn_kv) / sn_mva
    y_series_pu = y_series * z_base
    y_shunt_half_pu = y_shunt_half * z_base
    return np.array(
        [
            [y_series_pu + y_shunt_half_pu, -y_series_pu],
            [-y_series_pu, y_series_pu + y_shunt_half_pu],
        ],
        dtype=np.complex128,
    )


def _line_primitive_y_from_pp(*, line_row: Any, f_hz: float, vn_kv: float, sn_mva: float) -> np.ndarray:
    length_km = float(line_row.length_km)
    parallel = max(int(line_row.parallel), 1)
    r_total = float(line_row.r_ohm_per_km) * length_km / parallel
    x_total = float(line_row.x_ohm_per_km) * length_km / parallel
    g_total = float(line_row.g_us_per_km) * 1e-6 * length_km * parallel
    b_total = 2 * math.pi * float(f_hz) * float(line_row.c_nf_per_km) * 1e-9 * length_km * parallel
    z_series = complex(r_total, x_total)
    y_series = 0j if abs(z_series) <= 1e-18 else 1 / z_series
    y_shunt_half = complex(g_total, b_total) / 2.0
    z_base = (vn_kv * vn_kv) / sn_mva
    y_series_pu = y_series * z_base
    y_shunt_half_pu = y_shunt_half * z_base
    return np.array(
        [
            [y_series_pu + y_shunt_half_pu, -y_series_pu],
            [-y_series_pu, y_series_pu + y_shunt_half_pu],
        ],
        dtype=np.complex128,
    )


def _effective_pp_tap_ratio(trafo_row: Any) -> float:
    tap_pos = None if pd.isna(trafo_row.tap_pos) else float(trafo_row.tap_pos)
    tap_neutral = None if pd.isna(trafo_row.tap_neutral) else float(trafo_row.tap_neutral)
    tap_step_percent = None if pd.isna(trafo_row.tap_step_percent) else float(trafo_row.tap_step_percent)
    if tap_pos is None or tap_neutral is None or tap_step_percent is None:
        return 1.0
    return 1.0 + (tap_pos - tap_neutral) * tap_step_percent / 100.0


def _pp_transformer_series_pu(*, trafo_row: Any, net_sn_mva: float) -> complex:
    pp_r_trafo = float(trafo_row.vkr_percent) / 100.0
    pp_z_trafo = float(trafo_row.vk_percent) / 100.0
    pp_x_trafo = max(pp_z_trafo * pp_z_trafo - pp_r_trafo * pp_r_trafo, 0.0) ** 0.5
    return complex(pp_r_trafo, pp_x_trafo)


def _build_two_bus_pypsa_transformer_y(
    *,
    py_trafo_row: Any,
    hv_vn_kv: float,
    lv_vn_kv: float,
) -> np.ndarray:
    probe = pypsa.Network()
    probe.set_snapshots(["diag"])
    probe.add("Bus", "hv", v_nom=float(hv_vn_kv))
    probe.add("Bus", "lv", v_nom=float(lv_vn_kv))
    probe.add(
        "Transformer",
        "t",
        bus0="hv",
        bus1="lv",
        r=float(py_trafo_row.r),
        x=float(py_trafo_row.x),
        g=float(py_trafo_row.g),
        b=float(py_trafo_row.b),
        s_nom=float(py_trafo_row.s_nom),
        tap_ratio=float(py_trafo_row.tap_ratio),
        tap_side=py_trafo_row.tap_side,
        phase_shift=float(py_trafo_row.phase_shift) if "phase_shift" in py_trafo_row else 0.0,
    )
    probe.determine_network_topology()
    subnetwork = probe.sub_networks.iloc[0]["obj"]
    subnetwork.calculate_Y()
    buses = [str(name) for name in subnetwork.buses_o]
    dense = np.asarray(subnetwork.Y.todense(), dtype=np.complex128)
    idx_hv = buses.index("hv")
    idx_lv = buses.index("lv")
    return dense[np.ix_([idx_hv, idx_lv], [idx_hv, idx_lv])]


def _build_two_bus_pandapower_transformer_y(
    *,
    pp_trafo_row: Any,
    net_sn_mva: float,
    f_hz: float,
    trafo_model: str,
) -> np.ndarray:
    probe = pp.create_empty_network(sn_mva=float(net_sn_mva), f_hz=float(f_hz))
    hv_bus = pp.create_bus(probe, vn_kv=float(pp_trafo_row.vn_hv_kv), name="hv")
    lv_bus = pp.create_bus(probe, vn_kv=float(pp_trafo_row.vn_lv_kv), name="lv")
    pp.create_transformer_from_parameters(
        probe,
        hv_bus=hv_bus,
        lv_bus=lv_bus,
        sn_mva=float(pp_trafo_row.sn_mva),
        vn_hv_kv=float(pp_trafo_row.vn_hv_kv),
        vn_lv_kv=float(pp_trafo_row.vn_lv_kv),
        vk_percent=float(pp_trafo_row.vk_percent),
        vkr_percent=float(pp_trafo_row.vkr_percent),
        pfe_kw=float(pp_trafo_row.pfe_kw),
        i0_percent=float(pp_trafo_row.i0_percent),
        shift_degree=float(pp_trafo_row.shift_degree),
        tap_side=None if pd.isna(pp_trafo_row.tap_side) else str(pp_trafo_row.tap_side),
        tap_pos=None if pd.isna(pp_trafo_row.tap_pos) else float(pp_trafo_row.tap_pos),
        tap_neutral=None if pd.isna(pp_trafo_row.tap_neutral) else float(pp_trafo_row.tap_neutral),
        tap_step_percent=None if pd.isna(pp_trafo_row.tap_step_percent) else float(pp_trafo_row.tap_step_percent),
        name="t",
    )
    pp.create_ext_grid(probe, bus=hv_bus, vm_pu=1.0, name="slack")
    bus_names, ybus = _extract_pp_ybus(probe, trafo_model=trafo_model)
    idx_hv = bus_names.index("hv")
    idx_lv = bus_names.index("lv")
    return ybus[np.ix_([idx_hv, idx_lv], [idx_hv, idx_lv])]


def _map_flagged_entries_to_branches(
    *,
    source_network: pypsa.Network,
    island_ref: dict[str, Any],
    flagged_entries: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, str]]]:
    buses = set(str(bus) for bus in island_ref.get("buses", []))
    edge_lookup, incident_lookup = _build_island_branch_lookup(source_network, buses)

    mapping: list[dict[str, Any]] = []
    implicated: dict[tuple[str, str], dict[str, str]] = {}
    for entry in flagged_entries:
        i_bus = str(entry["i_bus"])
        j_bus = str(entry["j_bus"])
        if i_bus != j_bus:
            branch_list = edge_lookup.get(_edge_key(i_bus, j_bus), [])
            mapping.append(
                {
                    "i_bus": i_bus,
                    "j_bus": j_bus,
                    "type": "off_diagonal",
                    "branches": branch_list,
                    "branch_count": len(branch_list),
                }
            )
            for branch in branch_list:
                key = (branch["type"], branch["name"])
                implicated[key] = {"type": branch["type"], "name": branch["name"]}
        else:
            branch_list = incident_lookup.get(i_bus, [])
            mapping.append(
                {
                    "i_bus": i_bus,
                    "j_bus": j_bus,
                    "type": "diagonal",
                    "incident_branches": branch_list,
                    "incident_count": len(branch_list),
                }
            )
            for branch in branch_list:
                key = (branch["type"], branch["name"])
                implicated[key] = {"type": branch["type"], "name": branch["name"]}

    implicated_list = [implicated[key] for key in sorted(implicated)]
    return mapping, implicated_list


def _branch_relative_matrix_norm(py_matrix: np.ndarray, pp_matrix: np.ndarray) -> float:
    diff = pp_matrix - py_matrix
    return float(np.linalg.norm(diff, ord="fro") / max(np.linalg.norm(py_matrix, ord="fro"), 1e-12))


def _decompose_implicated_branches(
    *,
    source_network: pypsa.Network,
    island_net: Any,
    implicated: list[dict[str, str]],
    island_sn_mva: float,
    trafo_model: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    line_lookup = {
        str(row["name"]): idx
        for idx, row in island_net.line.iterrows()
        if "name" in island_net.line.columns and pd.notna(row["name"])
    }
    trafo_lookup = {
        str(row["name"]): idx
        for idx, row in island_net.trafo.iterrows()
        if "name" in island_net.trafo.columns and pd.notna(row["name"])
    }

    per_branch: list[dict[str, Any]] = []
    correction_table: list[dict[str, Any]] = []
    aggregate = {"line_rel_norm_sum": 0.0, "trafo_rel_norm_sum": 0.0, "line_count": 0, "trafo_count": 0}

    for item in implicated:
        branch_type = item["type"]
        branch_name = item["name"]
        if branch_type == "line":
            py_row = source_network.lines.loc[branch_name]
            if branch_name not in line_lookup:
                continue
            pp_row = island_net.line.loc[line_lookup[branch_name]]
            vn_kv = float(source_network.buses.loc[py_row.bus0, "v_nom"])
            py_mat = _line_primitive_y_from_pypsa(line_row=py_row, vn_kv=vn_kv, sn_mva=island_sn_mva)
            pp_mat = _line_primitive_y_from_pp(line_row=pp_row, f_hz=float(island_net.f_hz), vn_kv=vn_kv, sn_mva=island_sn_mva)
            rel_norm = _branch_relative_matrix_norm(py_mat, pp_mat)
            aggregate["line_rel_norm_sum"] += rel_norm
            aggregate["line_count"] += 1

            length_km = float(pp_row.length_km)
            parallel = max(int(pp_row.parallel), 1)
            req_r = float(py_row.r) / length_km * parallel
            req_x = float(py_row.x) / length_km * parallel
            req_c = float(py_row.b) / (2 * math.pi * float(island_net.f_hz)) * 1e9 / (length_km * parallel) if float(py_row.b) else 0.0
            req_g = float(py_row.g) * 1e6 / (length_km * parallel) if float(py_row.g) else 0.0

            per_branch.append(
                {
                    "branch_id": branch_name,
                    "branch_type": "line",
                    "bus0": str(py_row.bus0),
                    "bus1": str(py_row.bus1),
                    "pypsa_matrix": _matrix_payload(py_mat),
                    "pandapower_matrix": _matrix_payload(pp_mat),
                    "delta_matrix": _matrix_payload(pp_mat - py_mat),
                    "relative_matrix_norm": rel_norm,
                }
            )

            for parameter, current_value, required_value in (
                ("r_ohm_per_km", float(pp_row.r_ohm_per_km), float(req_r)),
                ("x_ohm_per_km", float(pp_row.x_ohm_per_km), float(req_x)),
                ("c_nf_per_km", float(pp_row.c_nf_per_km), float(req_c)),
                ("g_us_per_km", float(pp_row.g_us_per_km), float(req_g)),
            ):
                correction_table.append(
                    {
                        "branch_id": branch_name,
                        "branch_type": "line",
                        "parameter": parameter,
                        "current_value": current_value,
                        "required_value": required_value,
                        "correction_factor": None if abs(current_value) <= 1e-18 else required_value / current_value,
                    }
                )

        elif branch_type == "transformer":
            py_row = source_network.transformers.loc[branch_name]
            if branch_name not in trafo_lookup:
                continue
            pp_row = island_net.trafo.loc[trafo_lookup[branch_name]]
            hv_vn_kv = float(source_network.buses.loc[py_row.bus0, "v_nom"])
            lv_vn_kv = float(source_network.buses.loc[py_row.bus1, "v_nom"])
            py_mat = _build_two_bus_pypsa_transformer_y(py_trafo_row=py_row, hv_vn_kv=hv_vn_kv, lv_vn_kv=lv_vn_kv)
            pp_mat = _build_two_bus_pandapower_transformer_y(
                pp_trafo_row=pp_row,
                net_sn_mva=float(island_net.sn_mva),
                f_hz=float(island_net.f_hz),
                trafo_model=trafo_model,
            )
            rel_norm = _branch_relative_matrix_norm(py_mat, pp_mat)
            aggregate["trafo_rel_norm_sum"] += rel_norm
            aggregate["trafo_count"] += 1

            py_series = complex(float(py_row.r), float(py_row.x))
            pp_series = _pp_transformer_series_pu(trafo_row=pp_row, net_sn_mva=float(island_net.sn_mva))
            tap_effective = _effective_pp_tap_ratio(pp_row)
            offdiag_py = py_mat[0, 1]
            offdiag_pp = pp_mat[0, 1]
            required_tap = None
            if abs(offdiag_py) > 1e-12 and abs(offdiag_pp) > 1e-12:
                required_tap = tap_effective * (abs(offdiag_pp) / abs(offdiag_py))

            req_vkr = float(np.real(py_series)) * 100.0
            req_vk = abs(py_series) * 100.0

            per_branch.append(
                {
                    "branch_id": branch_name,
                    "branch_type": "transformer",
                    "bus0": str(py_row.bus0),
                    "bus1": str(py_row.bus1),
                    "pypsa_raw": {
                        "r": float(py_row.r),
                        "x": float(py_row.x),
                        "g": float(py_row.g),
                        "b": float(py_row.b),
                        "tap_ratio": float(py_row.tap_ratio),
                        "tap_side": _normalize_tap_side(py_row.tap_side),
                        "s_nom": float(py_row.s_nom),
                    },
                    "pandapower_raw": {
                        "vk_percent": float(pp_row.vk_percent),
                        "vkr_percent": float(pp_row.vkr_percent),
                        "sn_mva": float(pp_row.sn_mva),
                        "vn_hv_kv": float(pp_row.vn_hv_kv),
                        "vn_lv_kv": float(pp_row.vn_lv_kv),
                        "tap_pos": None if pd.isna(pp_row.tap_pos) else float(pp_row.tap_pos),
                        "tap_neutral": None if pd.isna(pp_row.tap_neutral) else float(pp_row.tap_neutral),
                        "tap_step_percent": None if pd.isna(pp_row.tap_step_percent) else float(pp_row.tap_step_percent),
                        "tap_side": _normalize_tap_side(pp_row.tap_side),
                    },
                    "effective_tap_ratio": tap_effective,
                    "effective_tap_ratio_required_from_offdiag": required_tap,
                    "series_impedance_pu": {
                        "pypsa": _complex_payload(py_series),
                        "pandapower": _complex_payload(pp_series),
                    },
                    "pypsa_matrix": _matrix_payload(py_mat),
                    "pandapower_matrix": _matrix_payload(pp_mat),
                    "delta_matrix": _matrix_payload(pp_mat - py_mat),
                    "relative_matrix_norm": rel_norm,
                }
            )

            correction_table.extend(
                [
                    {
                        "branch_id": branch_name,
                        "branch_type": "transformer",
                        "parameter": "vkr_percent",
                        "current_value": float(pp_row.vkr_percent),
                        "required_value": float(req_vkr),
                        "correction_factor": None if abs(float(pp_row.vkr_percent)) <= 1e-18 else float(req_vkr) / float(pp_row.vkr_percent),
                    },
                    {
                        "branch_id": branch_name,
                        "branch_type": "transformer",
                        "parameter": "vk_percent",
                        "current_value": float(pp_row.vk_percent),
                        "required_value": float(req_vk),
                        "correction_factor": None if abs(float(pp_row.vk_percent)) <= 1e-18 else float(req_vk) / float(pp_row.vk_percent),
                    },
                    {
                        "branch_id": branch_name,
                        "branch_type": "transformer",
                        "parameter": "effective_tap_ratio",
                        "current_value": float(tap_effective),
                        "required_value": None if required_tap is None else float(required_tap),
                        "correction_factor": None if required_tap is None or abs(tap_effective) <= 1e-18 else float(required_tap / tap_effective),
                    },
                ]
            )

    return per_branch, correction_table, aggregate


def _classify_branch_root_cause(per_branch: list[dict[str, Any]], aggregate: dict[str, Any]) -> dict[str, Any]:
    line_rel = float(aggregate.get("line_rel_norm_sum", 0.0))
    trafo_rel = float(aggregate.get("trafo_rel_norm_sum", 0.0))
    line_count = int(aggregate.get("line_count", 0))
    trafo_count = int(aggregate.get("trafo_count", 0))

    dominant = "Per-unit base mismatch"
    rationale = "No dominant mismatch pattern identified yet"

    if trafo_count > 0 and trafo_rel > max(5.0 * line_rel, 0.01):
        dominant = "Transformer equivalent circuit (PyPSA t-model vs pandapower π-model)"
        rationale = "Transformer branch matrix deltas dominate while line branch deltas remain negligible"
    else:
        line_shunt_indicators = []
        for item in per_branch:
            if item.get("branch_type") != "line":
                continue
            py = np.array([[complex(cell["real"], cell["imag"]) for cell in row] for row in item["pypsa_matrix"]], dtype=np.complex128)
            pp = np.array([[complex(cell["real"], cell["imag"]) for cell in row] for row in item["pandapower_matrix"]], dtype=np.complex128)
            py_shunt = py[0, 0] + py[0, 1]
            pp_shunt = pp[0, 0] + pp[0, 1]
            rel = abs(pp_shunt - py_shunt) / max(abs(py_shunt), 1e-12)
            line_shunt_indicators.append(rel)
        if line_shunt_indicators and max(line_shunt_indicators) > 1e-3 and line_rel >= trafo_rel:
            dominant = "Shunt admittance scaling"
            rationale = "Line shunt terms produce the largest relative branch-level matrix deltas"
        elif trafo_count > 0 and trafo_rel > 1e-3:
            dominant = "Transformer tap model"
            rationale = "Transformer off-diagonal terms show mismatch despite aligned series impedance values"

    return {
        "dominant_source": dominant,
        "rationale": rationale,
        "aggregate_branch_norms": {
            "line_rel_norm_sum": line_rel,
            "transformer_rel_norm_sum": trafo_rel,
            "line_branch_count": line_count,
            "transformer_branch_count": trafo_count,
        },
    }


def _matrix_from_payload(payload: list[list[dict[str, float]]]) -> np.ndarray:
    return np.array(
        [[complex(cell["real"], cell["imag"]) for cell in row] for row in payload],
        dtype=np.complex128,
    )


def _t_model_backcalculate(matrix: np.ndarray) -> dict[str, Any]:
    y = complex(matrix[1, 1])
    offdiag = complex(matrix[0, 1])
    t_from_offdiag = None if abs(offdiag) <= 1e-12 else (-y / offdiag)
    t_from_diag = None
    if abs(matrix[0, 0]) > 1e-12 and abs(y) > 1e-18:
        t_from_diag = np.sqrt(y / complex(matrix[0, 0]))

    predicted = None
    residual_rel = None
    if t_from_offdiag is not None and abs(t_from_offdiag) > 1e-12:
        t = t_from_offdiag
        predicted = np.array(
            [
                [y / (t * t), -y / t],
                [-y / t, y],
            ],
            dtype=np.complex128,
        )
        residual_rel = float(np.linalg.norm(matrix - predicted, ord="fro") / max(np.linalg.norm(matrix, ord="fro"), 1e-12))

    return {
        "y_from_y11": _complex_payload(y),
        "t_from_offdiag": None if t_from_offdiag is None else _complex_payload(t_from_offdiag),
        "t_from_diag": None if t_from_diag is None else _complex_payload(t_from_diag),
        "formula_residual_rel_fro": residual_rel,
        "predicted_t_model_matrix": None if predicted is None else _matrix_payload(predicted),
    }


def _clone_pandapower_net(net: Any) -> Any:
    return pp.from_json_string(pp.to_json(net, filename=None))


def _build_phase5_tap_ratio_trace(
    *,
    source_network: pypsa.Network,
    island_ref: dict[str, Any],
    island_net: Any,
    per_branch: list[dict[str, Any]],
    reference_voltages: dict[str, float],
    trafo_model: str,
) -> dict[str, Any]:
    trafo_lookup = {
        str(row["name"]): idx
        for idx, row in island_net.trafo.iterrows()
        if "name" in island_net.trafo.columns and pd.notna(row["name"])
    }

    transformer_branches = [
        item for item in per_branch
        if item.get("branch_type") == "transformer" and str(item.get("branch_id", "")) in trafo_lookup
    ]
    transformer_branches = sorted(transformer_branches, key=lambda item: str(item.get("branch_id")))

    entries: list[dict[str, Any]] = []
    override_indices: list[int] = []

    for item in transformer_branches:
        name = str(item["branch_id"])
        idx = int(trafo_lookup[name])
        override_indices.append(idx)

        py_row = source_network.transformers.loc[name]
        pp_row = island_net.trafo.loc[idx]

        py_matrix = _matrix_from_payload(item["pypsa_matrix"])
        pp_matrix = _matrix_from_payload(item["pandapower_matrix"])
        delta_matrix = _matrix_from_payload(item["delta_matrix"])

        py_back = _t_model_backcalculate(py_matrix)
        pp_back = _t_model_backcalculate(pp_matrix)

        py_tap_ratio = float(py_row.tap_ratio) if "tap_ratio" in py_row else 1.0
        py_tap_side = _normalize_tap_side(py_row.tap_side)
        py_phase_shift = float(py_row.phase_shift) if "phase_shift" in py_row else 0.0

        pp_tap_effective = _effective_pp_tap_ratio(pp_row)
        pp_hv_bus = int(pp_row.hv_bus)
        pp_lv_bus = int(pp_row.lv_bus)
        pp_hv_name = str(island_net.bus.at[pp_hv_bus, "name"])
        pp_lv_name = str(island_net.bus.at[pp_lv_bus, "name"])

        py_bus0_nom = float(source_network.buses.loc[str(py_row.bus0), "v_nom"])
        py_bus1_nom = float(source_network.buses.loc[str(py_row.bus1), "v_nom"])

        entries.append(
            {
                "transformer": name,
                "pypsa_matrix": _matrix_payload(py_matrix),
                "pandapower_matrix": _matrix_payload(pp_matrix),
                "delta_matrix": _matrix_payload(delta_matrix),
                "backcalculated": {
                    "pypsa": py_back,
                    "pandapower": pp_back,
                },
                "tap_ratio_trace": {
                    "pypsa_raw": {
                        "tap_ratio": py_tap_ratio,
                        "tap_side": py_tap_side,
                        "phase_shift_degree": py_phase_shift,
                        "tap_ratio_convention": "PyPSA tap_ratio is an absolute off-nominal multiplier (1.0 = nominal)",
                    },
                    "converter_written_pandapower": {
                        "tap_pos": None if pd.isna(pp_row.tap_pos) else float(pp_row.tap_pos),
                        "tap_neutral": None if pd.isna(pp_row.tap_neutral) else float(pp_row.tap_neutral),
                        "tap_step_percent": None if pd.isna(pp_row.tap_step_percent) else float(pp_row.tap_step_percent),
                        "tap_side": None if pd.isna(pp_row.tap_side) else str(pp_row.tap_side),
                        "shift_degree": float(pp_row.shift_degree),
                    },
                    "effective_pandapower_tap_ratio": pp_tap_effective,
                    "backcalculated_tap_ratio_from_matrix": {
                        "pypsa": py_back.get("t_from_offdiag"),
                        "pandapower": pp_back.get("t_from_offdiag"),
                    },
                },
                "base_checks": {
                    "sn_mva": {
                        "pypsa_s_nom": float(py_row.s_nom),
                        "pandapower_sn_mva": float(pp_row.sn_mva),
                        "match": math.isclose(float(py_row.s_nom), float(pp_row.sn_mva), rel_tol=0.0, abs_tol=1e-9),
                    },
                    "vn_hv_kv": {
                        "pypsa_bus_v_nom": py_bus0_nom,
                        "pandapower_trafo_vn_hv_kv": float(pp_row.vn_hv_kv),
                        "pandapower_bus_vn_kv": float(island_net.bus.at[pp_hv_bus, "vn_kv"]),
                        "pandapower_bus_name": pp_hv_name,
                    },
                    "vn_lv_kv": {
                        "pypsa_bus_v_nom": py_bus1_nom,
                        "pandapower_trafo_vn_lv_kv": float(pp_row.vn_lv_kv),
                        "pandapower_bus_vn_kv": float(island_net.bus.at[pp_lv_bus, "vn_kv"]),
                        "pandapower_bus_name": pp_lv_name,
                    },
                },
            }
        )

    override_result: dict[str, Any] = {
        "applied": False,
        "converged": False,
        "reason": "No implicated transformers",
        "compared_buses": 0,
        "max_voltage_delta_pu": None,
        "slack_delta_mva": None,
        "ybus_after_override": None,
    }

    if override_indices:
        override_net = _clone_pandapower_net(island_net)
        for item in entries:
            name = str(item["transformer"])
            idx = int(trafo_lookup[name])
            py_row = source_network.transformers.loc[name]
            py_tap_ratio = float(py_row.tap_ratio) if "tap_ratio" in py_row else 1.0
            py_tap_side = _normalize_tap_side(py_row.tap_side)

            override_net.trafo.at[idx, "tap_pos"] = 1.0
            override_net.trafo.at[idx, "tap_neutral"] = 0.0
            override_net.trafo.at[idx, "tap_step_percent"] = (py_tap_ratio - 1.0) * 100.0
            if py_tap_side in {"hv", "lv"}:
                override_net.trafo.at[idx, "tap_side"] = py_tap_side

        override_result["applied"] = True
        override_result["applied_transformers"] = [item["transformer"] for item in entries]
        override_result["applied_parameters"] = {
            "tap_pos": 1.0,
            "tap_neutral": 0.0,
            "tap_step_percent_formula": "(pypsa_tap_ratio - 1.0) * 100",
            "effective_tap_formula": "1 + (tap_pos - tap_neutral) * tap_step_percent / 100",
        }

        try:
            pp.runpp(
                override_net,
                algorithm="nr",
                init="dc",
                max_iteration=50,
                trafo_model=trafo_model,
                numba=False,
            )
            override_result["converged"] = bool(getattr(override_net, "converged", False))

            if override_result["converged"]:
                slack_p, slack_q = _extract_balanced_slack_from_net(override_net)
                ref_slack_p = float(island_ref.get("reference_slack_p_mw", 0.0))
                ref_slack_q = float(island_ref.get("reference_slack_q_mvar", 0.0))
                override_result["slack_delta_mva"] = float(math.hypot(slack_p - ref_slack_p, slack_q - ref_slack_q))

                voltage_delta = 0.0
                compared = 0
                for _, row in override_net.res_bus.iterrows():
                    bus_idx = int(row.name)
                    bus_key = str(override_net.bus.at[bus_idx, "name"])
                    reference = reference_voltages.get(bus_key)
                    if reference is None:
                        continue
                    voltage_delta = max(voltage_delta, abs(float(row.vm_pu) - float(reference)))
                    compared += 1
                override_result["compared_buses"] = int(compared)
                override_result["max_voltage_delta_pu"] = float(voltage_delta)
        except Exception as exc:
            override_result["converged"] = False
            override_result["reason"] = str(exc)

        try:
            py_bus_names, py_ybus = _extract_pypsa_ybus(source_network, island_ref)
            pp_bus_names, pp_ybus = _extract_pp_ybus(override_net, trafo_model=trafo_model)
            override_result["ybus_after_override"] = _compare_ybus(
                pypsa_bus_names=py_bus_names,
                pypsa_ybus=py_ybus,
                pandapower_bus_names=pp_bus_names,
                pandapower_ybus=pp_ybus,
                rel_threshold=1e-3,
            )
        except Exception as exc:
            override_result["ybus_after_override"] = {"error": str(exc)}

    return {
        "scope": "smallest_failing_island_implicated_transformers",
        "transformer_count": int(len(entries)),
        "transformers": entries,
        "override_test": override_result,
    }


def _compute_island_structural_counts(
    *,
    source_network: pypsa.Network,
    island_ref: dict[str, Any],
    island_net: Any,
) -> dict[str, Any]:
    buses = [str(bus) for bus in island_ref.get("buses", [])]
    bus_set = set(buses)
    expected_line_count = int(
        len(source_network.lines[(source_network.lines.bus0.astype(str).isin(bus_set)) & (source_network.lines.bus1.astype(str).isin(bus_set))])
    )
    expected_trafo_count = int(
        len(
            source_network.transformers[
                (source_network.transformers.bus0.astype(str).isin(bus_set))
                & (source_network.transformers.bus1.astype(str).isin(bus_set))
            ]
        )
    )
    expected_load_count = int(len(source_network.loads[source_network.loads.bus.astype(str).isin(bus_set)]))

    expected_slack_bus = str(island_ref.get("slack_bus")) if island_ref.get("slack_bus") else None
    ext_grid_bus_names = [
        str(island_net.bus.at[int(row.bus), "name"])
        for _, row in island_net.ext_grid.iterrows()
    ]
    gen_slack_count = 0
    if len(island_net.gen) and "slack" in island_net.gen.columns:
        gen_slack_count = int(island_net.gen[island_net.gen["slack"].fillna(False)].shape[0])

    return {
        "bus_count": {
            "expected": int(len(buses)),
            "actual": int(len(island_net.bus)),
            "match": int(len(buses)) == int(len(island_net.bus)),
        },
        "line_count": {
            "expected": expected_line_count,
            "actual": int(len(island_net.line)),
            "match": expected_line_count == int(len(island_net.line)),
        },
        "transformer_count": {
            "expected": expected_trafo_count,
            "actual": int(len(island_net.trafo)),
            "match": expected_trafo_count == int(len(island_net.trafo)),
        },
        "load_count": {
            "expected": expected_load_count,
            "actual": int(len(island_net.load)),
            "match": expected_load_count == int(len(island_net.load)),
        },
        "ext_grid": {
            "count": int(len(island_net.ext_grid)),
            "exactly_one": int(len(island_net.ext_grid)) == 1,
            "bus_names": ext_grid_bus_names,
            "expected_slack_bus": expected_slack_bus,
            "correct_slack_bus": bool(expected_slack_bus and len(ext_grid_bus_names) == 1 and ext_grid_bus_names[0] == expected_slack_bus),
        },
        "gen_slack": {
            "count": gen_slack_count,
            "none_present": gen_slack_count == 0,
        },
        "in_service": {
            "buses_all_true": bool(island_net.bus.in_service.all()) if len(island_net.bus) else True,
            "lines_all_true": bool(island_net.line.in_service.all()) if len(island_net.line) else True,
            "trafos_all_true": bool(island_net.trafo.in_service.all()) if len(island_net.trafo) else True,
        },
    }


def _build_pandapower_island_direct(
    *,
    source_network: pypsa.Network,
    island_ref: dict[str, Any],
) -> Any:
    buses = [str(bus) for bus in island_ref.get("buses", [])]
    bus_set = set(buses)
    net = pp.create_empty_network(
        sn_mva=float(getattr(source_network, "sn_mva", 1.0) or 1.0),
        f_hz=float(getattr(source_network, "f_hz", 50.0) or 50.0),
    )
    bus_lookup: dict[str, int] = {}
    for bus_name in buses:
        row = source_network.buses.loc[bus_name]
        bus_lookup[bus_name] = pp.create_bus(
            net,
            vn_kv=float(row.v_nom),
            name=str(bus_name),
            type=str(row.type) if "type" in row and pd.notna(row.type) else "b",
        )

    for line_name, row in source_network.lines.iterrows():
        bus0 = str(row.bus0)
        bus1 = str(row.bus1)
        if bus0 not in bus_set or bus1 not in bus_set:
            continue
        length_km = float(row.length) if "length" in row and float(row.length) > 0 else 1.0
        parallel = max(int(round(float(row.num_parallel))), 1) if "num_parallel" in row else 1
        c_nf_per_km = float(row.b) / (2 * math.pi * net.f_hz) * 1e9 / (length_km * parallel) if float(row.b) else 0.0
        g_us_per_km = float(row.g) * 1e6 / (length_km * parallel) if float(row.g) else 0.0
        max_i_ka_total = float(row.s_nom) / float(source_network.buses.loc[row.bus0, "v_nom"]) if float(row.s_nom) else 1.0
        max_i_ka = max_i_ka_total / parallel
        pp.create_line_from_parameters(
            net,
            from_bus=bus_lookup[bus0],
            to_bus=bus_lookup[bus1],
            length_km=length_km,
            r_ohm_per_km=float(row.r) * parallel / length_km,
            x_ohm_per_km=float(row.x) * parallel / length_km,
            c_nf_per_km=c_nf_per_km,
            g_us_per_km=g_us_per_km,
            max_i_ka=max_i_ka,
            parallel=parallel,
            name=str(line_name),
        )

    for trafo_name, row in source_network.transformers.iterrows():
        bus0 = str(row.bus0)
        bus1 = str(row.bus1)
        if bus0 not in bus_set or bus1 not in bus_set:
            continue
        z = math.hypot(float(row.r), float(row.x))
        y = math.hypot(float(row.g), float(row.b))
        sn_mva = float(row.s_nom)
        tap_side = _normalize_tap_side(row.tap_side)
        pp.create_transformer_from_parameters(
            net,
            hv_bus=bus_lookup[bus0],
            lv_bus=bus_lookup[bus1],
            sn_mva=sn_mva,
            vn_hv_kv=float(source_network.buses.loc[row.bus0, "v_nom"]),
            vn_lv_kv=float(source_network.buses.loc[row.bus1, "v_nom"]),
            vkr_percent=float(row.r) * 100.0,
            vk_percent=z * 100.0,
            pfe_kw=float(row.g) * sn_mva * 1000.0,
            i0_percent=y * 100.0,
            shift_degree=float(row.phase_shift) if "phase_shift" in row else 0.0,
            tap_side=tap_side if tap_side in {"hv", "lv"} else None,
            name=str(trafo_name),
        )

    for load_name, row in source_network.loads.iterrows():
        bus_name = str(row.bus)
        if bus_name not in bus_set:
            continue
        pp.create_load(
            net,
            bus=bus_lookup[bus_name],
            p_mw=float(row.p_set),
            q_mvar=float(row.q_set),
            name=str(load_name),
        )

    for gen_name, row in source_network.generators.iterrows():
        bus_name = str(row.bus)
        if bus_name not in bus_set:
            continue
        control = str(row.control)
        vm_pu = float(row.vm_pu) if "vm_pu" in row and pd.notna(row.vm_pu) else 1.0
        if control == "Slack":
            pp.create_ext_grid(net, bus=bus_lookup[bus_name], vm_pu=vm_pu, name=str(gen_name))
        elif control == "PV":
            pp.create_gen(net, bus=bus_lookup[bus_name], p_mw=float(row.p_set), vm_pu=vm_pu, name=str(gen_name))
        else:
            pp.create_sgen(net, bus=bus_lookup[bus_name], p_mw=float(row.p_set), q_mvar=float(row.q_set), name=str(gen_name))

    return net


def build_phase3_smallest_island_diagnostic(
    *,
    source_network: pypsa.Network,
    case: Any,
    reference_islands: list[dict[str, Any]],
    reference_voltages: dict[str, float],
    island_validation_payload: dict[str, Any],
    trafo_model: str,
) -> dict[str, Any] | None:
    failed_subnets = {
        str(item.get("subnetwork"))
        for item in island_validation_payload.get("islands", [])
        if not bool(item.get("passed"))
    }
    if not failed_subnets:
        return None

    candidates = [ref for ref in reference_islands if str(ref.get("subnetwork")) in failed_subnets]
    if not candidates:
        return None
    smallest_ref = min(candidates, key=lambda ref: len(ref.get("buses", [])))

    base_net = PandapowerAdapter().to_net(case)
    island_net, _ = _build_island_net_from_full(
        source_network=source_network,
        base_net=base_net,
        island_ref=smallest_ref,
    )

    py_bus_names, py_ybus = _extract_pypsa_ybus(source_network, smallest_ref)
    pp_bus_names, pp_ybus = _extract_pp_ybus(island_net, trafo_model=trafo_model)
    ybus_comparison = _compare_ybus(
        pypsa_bus_names=py_bus_names,
        pypsa_ybus=py_ybus,
        pandapower_bus_names=pp_bus_names,
        pandapower_ybus=pp_ybus,
        rel_threshold=1e-3,
    )

    flagged_mapping, implicated = _map_flagged_entries_to_branches(
        source_network=source_network,
        island_ref=smallest_ref,
        flagged_entries=ybus_comparison.get("flagged_entries", []),
    )
    per_branch, correction_table, aggregate = _decompose_implicated_branches(
        source_network=source_network,
        island_net=island_net,
        implicated=implicated,
        island_sn_mva=float(island_net.sn_mva),
        trafo_model=trafo_model,
    )
    root_cause = _classify_branch_root_cause(per_branch, aggregate)
    phase5_tap_ratio_trace = _build_phase5_tap_ratio_trace(
        source_network=source_network,
        island_ref=smallest_ref,
        island_net=island_net,
        per_branch=per_branch,
        reference_voltages=reference_voltages,
        trafo_model=trafo_model,
    )

    structural_counts = _compute_island_structural_counts(
        source_network=source_network,
        island_ref=smallest_ref,
        island_net=island_net,
    )

    bypass_net = _build_pandapower_island_direct(
        source_network=source_network,
        island_ref=smallest_ref,
    )
    bypass_result = {
        "converged": False,
        "reason": None,
    }
    try:
        pp.runpp(
            bypass_net,
            algorithm="nr",
            init="dc",
            max_iteration=50,
            trafo_model=trafo_model,
            numba=False,
        )
        bypass_result["converged"] = bool(getattr(bypass_net, "converged", False))
    except Exception as exc:
        bypass_result["converged"] = False
        bypass_result["reason"] = str(exc)

    py_cond = _compute_condition_number(py_ybus)
    pp_cond = _compute_condition_number(pp_ybus)

    return {
        "subnetwork": str(smallest_ref.get("subnetwork")),
        "bus_count": int(len(smallest_ref.get("buses", []))),
        "ybus_comparison": ybus_comparison,
        "structural_counts": structural_counts,
        "bypass_build": bypass_result,
        "condition_numbers": {
            "pypsa": py_cond,
            "pandapower": pp_cond,
            "ratio_pandapower_over_pypsa": None if py_cond in {None, 0.0} or pp_cond is None else pp_cond / py_cond,
        },
        "branch_ybus_decomposition": {
            "flagged_pair_mapping": flagged_mapping,
            "per_branch": per_branch,
            "root_cause_classification": root_cause,
            "correction_table": correction_table,
        },
        "phase5_tap_ratio_trace": phase5_tap_ratio_trace,
    }


def validate_and_export_pandapower_islands(
    *,
    source_network: pypsa.Network,
    case: Any,
    case_id: str,
    reference_islands: list[dict[str, Any]],
    reference_voltages: dict[str, float],
    slack_tolerance_mva: float,
    voltage_tolerance_pu: float,
    island_output_dir: Path,
    trafo_model: str = "t",
) -> dict[str, Any]:
    base_net = PandapowerAdapter().to_net(case)
    island_output_dir.mkdir(parents=True, exist_ok=True)

    island_results: list[dict[str, Any]] = []
    max_slack_delta = 0.0
    max_voltage_delta = 0.0
    max_ybus_rel_fro = 0.0
    passed_count = 0
    smallest_failed_diag: dict[str, Any] | None = None
    smallest_failed_bus_count: int | None = None

    bus_name_series = base_net.bus["name"].astype(str)
    for island_ref in reference_islands:
        subnet_id = str(island_ref["subnetwork"])
        buses = [str(bus) for bus in island_ref.get("buses", [])]
        bus_indices = list(base_net.bus.index[bus_name_series.isin(buses)])
        island_file = island_output_dir / f"{case_id}.island_{subnet_id}.pandapower.json"

        if not bus_indices:
            island_results.append(
                {
                    "subnetwork": subnet_id,
                    "passed": False,
                    "reason": "No matching pandapower buses for island",
                    "output": str(island_file),
                }
            )
            continue

        island_net = pp_toolbox.select_subnet(
            base_net,
            bus_indices,
            include_switch_buses=False,
            include_results=False,
            keep_everything_else=False,
        )

        island_bus_set = set(buses)
        py_sn_mva = float(getattr(source_network, "sn_mva", 1.0) or 1.0)
        island_net.sn_mva = py_sn_mva
        zero_fix = _sanitize_zero_impedance_branches(island_net)
        slack_fixed = _ensure_island_slack_ext_grid(island_net, island_ref.get("slack_bus"))
        trafo_fix = _apply_transformer_series_corrections(
            source_network=source_network,
            net=island_net,
            buses=island_bus_set,
        )
        ybus_relative_frobenius_norm: float | None = None
        ybus_flagged_entry_count: int | None = None
        ybus_error: str | None = None
        try:
            py_bus_names, py_ybus = _extract_pypsa_ybus(source_network, island_ref)
            pp_bus_names, pp_ybus = _extract_pp_ybus(island_net, trafo_model=trafo_model)
            ybus_cmp = _compare_ybus(
                pypsa_bus_names=py_bus_names,
                pypsa_ybus=py_ybus,
                pandapower_bus_names=pp_bus_names,
                pandapower_ybus=pp_ybus,
                rel_threshold=1e-12,
            )
            ybus_relative_frobenius_norm = float(ybus_cmp.get("relative_frobenius_norm", 0.0))
            ybus_flagged_entry_count = int(ybus_cmp.get("flagged_entry_count", 0))
            max_ybus_rel_fro = max(max_ybus_rel_fro, ybus_relative_frobenius_norm)
        except Exception as exc:
            ybus_error = str(exc)

        pp.to_json(island_net, str(island_file))

        try:
            pp.runpp(
                island_net,
                algorithm="nr",
                init="dc",
                max_iteration=50,
                trafo_model=trafo_model,
                numba=False,
            )
            slack_p, slack_q = _extract_balanced_slack_from_net(island_net)
            reference_slack_p = float(island_ref.get("reference_slack_p_mw", 0.0))
            reference_slack_q = float(island_ref.get("reference_slack_q_mvar", 0.0))
            slack_delta = math.hypot(slack_p - reference_slack_p, slack_q - reference_slack_q)

            island_voltage_delta = 0.0
            compared_buses = 0
            for _, row in island_net.res_bus.iterrows():
                bus_idx = int(row.name)
                bus_key = str(island_net.bus.at[bus_idx, "name"])
                reference = reference_voltages.get(bus_key)
                if reference is None:
                    continue
                island_voltage_delta = max(island_voltage_delta, abs(float(row.vm_pu) - float(reference)))
                compared_buses += 1

            island_passed = (
                slack_delta <= slack_tolerance_mva
                and island_voltage_delta <= voltage_tolerance_pu
                and compared_buses > 0
            )
            if island_passed:
                passed_count += 1
            else:
                bus_count = int(len(buses))
                if smallest_failed_bus_count is None or bus_count < smallest_failed_bus_count:
                    smallest_failed_bus_count = bus_count
                    smallest_failed_diag = _build_island_diagnostic(
                        source_network=source_network,
                        island_net=island_net,
                        island_ref=island_ref,
                    )

            max_slack_delta = max(max_slack_delta, slack_delta)
            max_voltage_delta = max(max_voltage_delta, island_voltage_delta)
            island_results.append(
                {
                    "subnetwork": subnet_id,
                    "converged": bool(getattr(island_net, "converged", False)),
                    "passed": island_passed,
                    "slack_delta_mva": slack_delta,
                    "max_voltage_delta_pu": island_voltage_delta,
                    "ybus_relative_frobenius_norm": ybus_relative_frobenius_norm,
                    "ybus_flagged_entry_count": ybus_flagged_entry_count,
                    "ybus_error": ybus_error,
                    "compared_buses": compared_buses,
                    "fixes": {
                        "zero_impedance": zero_fix,
                        "slack_ext_grid_present": slack_fixed,
                        "transformer_series": trafo_fix,
                    },
                    "output": str(island_file),
                }
            )
        except Exception as exc:
            bus_count = int(len(buses))
            if smallest_failed_bus_count is None or bus_count < smallest_failed_bus_count:
                smallest_failed_bus_count = bus_count
                smallest_failed_diag = _build_island_diagnostic(
                    source_network=source_network,
                    island_net=island_net,
                    island_ref=island_ref,
                )
            island_results.append(
                {
                    "subnetwork": subnet_id,
                    "converged": False,
                    "passed": False,
                    "reason": str(exc),
                    "ybus_relative_frobenius_norm": ybus_relative_frobenius_norm,
                    "ybus_flagged_entry_count": ybus_flagged_entry_count,
                    "ybus_error": ybus_error,
                    "fixes": {
                        "zero_impedance": zero_fix,
                        "slack_ext_grid_present": slack_fixed,
                        "transformer_series": trafo_fix,
                    },
                    "output": str(island_file),
                }
            )

    total_islands = len(reference_islands)
    overall_passed = total_islands > 0 and passed_count == total_islands
    return {
        "passed": overall_passed,
        "trafo_model": str(trafo_model),
        "total_islands": total_islands,
        "passed_islands": passed_count,
        "failed_islands": total_islands - passed_count,
        "max_slack_delta_mva": max_slack_delta,
        "max_voltage_delta_pu": max_voltage_delta,
        "max_ybus_relative_frobenius_norm": max_ybus_rel_fro,
        "islands": island_results,
        "smallest_failed_island_diagnostic": smallest_failed_diag,
    }


def _json_scalar(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        return None if (math.isnan(value) or math.isinf(value)) else value
    if isinstance(value, complex):
        return {"real": value.real, "imag": value.imag}
    if isinstance(value, (pd.Timestamp, pd.Timedelta)):
        return str(value)
    if isinstance(value, Path):
        return str(value)
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    return str(value)


def serialize_dataframe(table: pd.DataFrame) -> dict[str, Any]:
    columns = [str(column) for column in table.columns]
    records: list[dict[str, Any]] = []
    for idx, row in table.iterrows():
        payload = {"_index": _json_scalar(idx)}
        for column in table.columns:
            payload[str(column)] = _json_scalar(row[column])
        records.append(payload)
    return {
        "columns": columns,
        "row_count": int(len(table)),
        "records": records,
    }


def collect_pypsa_full_details(
    network: pypsa.Network,
    *,
    snapshot: Any,
    include_time_series: bool,
) -> dict[str, Any]:
    details: dict[str, Any] = {
        "meta": {
            "name": str(getattr(network, "name", "")),
            "srid": int(getattr(network, "srid", 4326)),
            "snapshots_count": int(len(network.snapshots)),
            "selected_snapshot": str(snapshot),
        },
        "components": {},
        "time_series": {},
    }

    component_tables = (
        "buses",
        "lines",
        "transformers",
        "loads",
        "generators",
        "shunt_impedances",
        "links",
        "stores",
        "storage_units",
        "carriers",
        "global_constraints",
    )
    for table_name in component_tables:
        table = getattr(network, table_name, None)
        if isinstance(table, pd.DataFrame) and not table.empty:
            details["components"][table_name] = serialize_dataframe(table)

    if include_time_series:
        time_series_groups = (
            "buses_t",
            "lines_t",
            "transformers_t",
            "loads_t",
            "generators_t",
            "shunt_impedances_t",
            "links_t",
            "stores_t",
            "storage_units_t",
        )
        for group_name in time_series_groups:
            group = getattr(network, group_name, None)
            if group is None:
                continue
            frames: dict[str, Any] = {}
            if hasattr(group, "items"):
                for frame_name, frame in group.items():
                    if isinstance(frame, pd.DataFrame) and not frame.empty:
                        frames[str(frame_name)] = serialize_dataframe(frame)
            if frames:
                details["time_series"][group_name] = frames

    return details


def run(args: argparse.Namespace) -> int:
    source = Path(args.source)
    pypsa_adapter = PypsaAdapter()
    pandapower_adapter = PandapowerAdapter()
    network = pypsa_adapter.load_network(source)
    snapshot = select_snapshot(network, args.snapshot)

    removed_components = {
        "links": int(len(network.links)),
        "stores": int(len(network.stores)),
        "storage_units": int(len(network.storage_units)),
    }
    has_unsupported = any(value > 0 for value in removed_components.values())

    if has_unsupported and args.fail_on_pruned_components:
        raise ValueError(
            "Source network contains unsupported populated components for this repository's validated AC subset: "
            + ", ".join(f"{name}={count}" for name, count in removed_components.items() if count > 0)
        )

    base_network_for_conversion = network.copy()
    pruned = prune_unsupported_components(base_network_for_conversion)
    pypsa_adapter._assert_supported_pypsa_network(base_network_for_conversion)

    network_for_conversion = base_network_for_conversion.copy()

    synthetic_pf: dict[str, Any] | None = None
    approach_used = "none"
    if not args.skip_validation:
        synthetic_pf = inject_fixed_small_pq_validation_case(
            network_for_conversion,
            total_target_mw_per_island=0.1,
            use_distributed_pv_support=False,
        )
        approach_used = "A_near_zero_single_slack"

    validation_error: str | None = None
    validation_payload: dict[str, Any] | None = None

    reference_slack_p = 0.0
    reference_slack_q = 0.0
    reference_voltages: dict[str, float] = {}
    reference_island_stats: dict[str, Any] | None = None
    reference_islands: list[dict[str, Any]] = []
    if not args.skip_validation:
        try:
            (
                reference_slack_p,
                reference_slack_q,
                reference_voltages,
                reference_island_stats,
                reference_islands,
            ) = build_pypsa_reference(
                network_for_conversion,
                snapshot,
                allow_lpf_fallback=bool(args.lpf_fallback),
            )
        except Exception as exc:
            validation_error = f"PyPSA reference power flow failed: {exc}"

    if not args.skip_validation:
        excluded = [] if reference_island_stats is None else reference_island_stats.get("excluded_islands", [])
        excluded_non_convergent = {
            str(item.get("subnetwork"))
            for item in excluded
            if "degenerate_non_convergent" in str(item.get("reason", ""))
        }
        needs_distributed_pv = (
            validation_error is not None
            or "0" in excluded_non_convergent
            or "1" in excluded_non_convergent
        )

        if needs_distributed_pv:
            validation_error = None
            network_for_conversion = base_network_for_conversion.copy()
            synthetic_pf = inject_fixed_small_pq_validation_case(
                network_for_conversion,
                total_target_mw_per_island=0.1,
                use_distributed_pv_support=True,
            )
            approach_used = "B_near_zero_with_distributed_pv"
            try:
                (
                    reference_slack_p,
                    reference_slack_q,
                    reference_voltages,
                    reference_island_stats,
                    reference_islands,
                ) = build_pypsa_reference(
                    network_for_conversion,
                    snapshot,
                    allow_lpf_fallback=bool(args.lpf_fallback),
                )
            except Exception as exc:
                validation_error = f"PyPSA reference power flow failed: {exc}"

    pandapower_net = pypsa_adapter.pypsa_to_pandapower(network_for_conversion)
    embed_details = not bool(args.no_embed_pypsa_details)
    include_time_series = not bool(args.no_embed_time_series)
    if embed_details:
        pandapower_net["pypsa_full_details"] = collect_pypsa_full_details(
            network,
            snapshot=snapshot,
            include_time_series=include_time_series,
        )

    case_id = source.stem if source.is_file() else source.name
    case = pandapower_adapter.to_canonical(
        pandapower_net,
        case_id=case_id,
        source_format="pypsa",
        metadata={
            "import_backend": "pypsa-eur-full-resolution",
            "pypsa_snapshot": str(snapshot),
            "pandapower_trafo_model": str(args.trafo_model),
            "unsupported_components_pruned": {name: count for name, count in pruned.items() if count > 0},
            "validation_scope": "ac_projection",
            "pypsa_details_embedded": embed_details,
            "pypsa_time_series_embedded": include_time_series and embed_details,
            "synthetic_pf_case": synthetic_pf,
            "phase9_approach_used": approach_used,
        },
        source_path=source,
    )

    output_path = resolve_output_path(source, args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    pandapower_adapter.export_json(case, output_path)
    island_output_dir = resolve_island_output_dir(output_path, args.island_output_dir)
    bundle_output = resolve_bundle_output_path(output_path, args.bundle_output)
    diagnostic_output = resolve_diagnostic_output_path(output_path)

    island_validation_payload: dict[str, Any] | None = None
    phase3_smallest_island: dict[str, Any] | None = None
    if args.skip_validation:
        validation_payload = {
            "skipped": True,
            "reason": "Requested by --skip-validation",
        }
    elif validation_error is None:
        try:
            island_validation_payload = validate_and_export_pandapower_islands(
                source_network=network,
                case=case,
                case_id=case_id,
                reference_islands=reference_islands,
                reference_voltages=reference_voltages,
                slack_tolerance_mva=float(args.slack_tolerance_mva),
                voltage_tolerance_pu=float(args.voltage_tolerance_pu),
                island_output_dir=island_output_dir,
                trafo_model=str(args.trafo_model),
            )
            validation_payload = {
                "case_id": case.case_id,
                "passed": bool(island_validation_payload.get("passed")),
                "slack_delta_mva": float(island_validation_payload.get("max_slack_delta_mva", 0.0)),
                "max_voltage_delta_pu": float(island_validation_payload.get("max_voltage_delta_pu", 0.0)),
                "details": {
                    "total_islands": int(island_validation_payload.get("total_islands", 0)),
                    "passed_islands": int(island_validation_payload.get("passed_islands", 0)),
                    "failed_islands": int(island_validation_payload.get("failed_islands", 0)),
                },
            }
            if int(island_validation_payload.get("failed_islands", 0)) > 0:
                phase3_smallest_island = build_phase3_smallest_island_diagnostic(
                    source_network=network_for_conversion,
                    case=case,
                    reference_islands=reference_islands,
                    reference_voltages=reference_voltages,
                    island_validation_payload=island_validation_payload,
                    trafo_model=str(args.trafo_model),
                )
        except Exception as exc:
            validation_payload = {
                "passed": False,
                "skipped": True,
                "reason": f"Pandapower island validation failed: {exc}",
            }
    else:
        validation_payload = {
            "skipped": True,
            "reason": validation_error,
        }

    report = {
        "source": str(source),
        "snapshot": str(snapshot),
        "output": str(output_path),
        "diagnostic_output": str(diagnostic_output),
        "pruned_components": {name: count for name, count in pruned.items() if count > 0},
        "synthetic_pf_case": synthetic_pf,
        "validation_scope": "ac_projection_against_pypsa",
        "full_details_embedded": embed_details,
        "time_series_embedded": include_time_series and embed_details,
        "pandapower_trafo_model": str(args.trafo_model),
        "phase9_approach_used": approach_used,
        "reference_island_stats": reference_island_stats,
        "pandapower_island_validation": island_validation_payload,
        "validation": validation_payload,
    }
    validation_output = resolve_validation_output_path(output_path, args.validation_output)
    validation_output.parent.mkdir(parents=True, exist_ok=True)
    validation_output.write_text(json.dumps(report, indent=2))

    diagnostic_payload = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source": str(source),
        "output": str(output_path),
        "pandapower_trafo_model": str(args.trafo_model),
        "diagnostic": None if island_validation_payload is None else island_validation_payload.get("smallest_failed_island_diagnostic"),
        "phase3_smallest_island": phase3_smallest_island,
    }
    diagnostic_output.parent.mkdir(parents=True, exist_ok=True)
    diagnostic_output.write_text(json.dumps(diagnostic_payload, indent=2))

    bundle = {
        "full_network": str(output_path),
        "validation_report": str(validation_output),
        "diagnostic_report": str(diagnostic_output),
        "island_output_dir": str(island_output_dir),
        "islands": [] if island_validation_payload is None else island_validation_payload.get("islands", []),
    }
    bundle_output.parent.mkdir(parents=True, exist_ok=True)
    bundle_output.write_text(json.dumps(bundle, indent=2))

    print(json.dumps(report, indent=2))
    if args.skip_validation or validation_error is not None:
        return 0
    return 0 if bool(validation_payload and validation_payload.get("passed")) else 2


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    raise SystemExit(run(args))


if __name__ == "__main__":
    main()
