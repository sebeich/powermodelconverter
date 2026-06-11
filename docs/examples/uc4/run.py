#!/usr/bin/env python3
"""
UC4 research-grade reproducer: PyPSA-Eur EHV grid composed with the OSM-derived German
sub-transmission layer (>= 60 kV), carried to full AC analysis at scale.

Three sources, none individually sufficient: PyPSA-Eur's published electricity model stops at
the EHV level and solves linearized flows; the raw OpenStreetMap extract for Germany is topology
data, not a solvable electrical model; and neither ecosystem cross-validates a full AC solution.
The staged overlay (built by `scripts/build_pypsa_eur_hv_de_mv_overlay.py` and imported by
`scripts/import_pypsa_eur_to_pandapower.py`) becomes solvable and verifiable through the
pandapower hub and the PowerModels cross-check exercised here.

Citations:
- Hoersch, J.; Hofmann, F.; Schlachtberger, D.; Brown, T. "PyPSA-Eur: An open optimisation model
  of the European transmission system." Energy Strategy Reviews, 2018.
- Brown, T.; Hoersch, J.; Schlachtberger, D. "PyPSA: Python for Power System Analysis."
  Journal of Open Research Software, 2018.
- OpenStreetMap contributors, Open Database License (ODbL).
- Coffrin, C. et al. "PowerModels.jl: An Open-Source Framework for Exploring Power Flow
  Formulations." PSCC, 2018.
"""

from __future__ import annotations

import json
import math
import os
from pathlib import Path
import sys

CURRENT_DIR = Path(__file__).resolve().parent
EXAMPLES_ROOT = CURRENT_DIR.parent
if str(EXAMPLES_ROOT) not in sys.path:
    sys.path.insert(0, str(EXAMPLES_ROOT))

from _common import REPO_ROOT
from _common import balanced_reference_validation
from _common import ensure_dir
from _common import json_ready
from _common import write_csv
from _common import write_json

import pandapower.toolbox as pp_toolbox

from powermodelconverter.core.model import CanonicalCase
from powermodelconverter.core.pandapower_backend import PandapowerAdapter
from powermodelconverter.importers.pandapower_json import PandapowerImportAdapter
from powermodelconverter.validation.engine import ValidationService


STAGED_ROOT = REPO_ROOT / "validation_cases" / "pypsa-eur" / "hv_all_de_mv" / "outputs"
ISLAND_JSON = STAGED_ROOT / "pypsa_eur_hv_all_de_mv.pandapower_all_islands" / "pypsa_eur_hv_all_de_mv.island_00.pandapower.json"
SOURCE_SUMMARY = STAGED_ROOT / "pypsa_eur_hv_all_de_mv.source.summary.json"
ISLANDS_BUNDLE = STAGED_ROOT / "pypsa_eur_hv_all_de_mv.pandapower.json.islands.bundle.json"

SUBTRANSMISSION_KV = (60.0, 155.0)
BOUNDARY_HV_MIN_KV = 220.0


def detect_julia_binary() -> str:
    return os.environ.get("PMC_JULIA_BINARY") or os.environ.get("JULIA_BIN") or "julia"


def staged_provenance(bus_count: int) -> dict[str, object]:
    """Collect the staging-time provenance: overlay build statistics and the per-island
    PyPSA <-> pandapower validation recorded when the artifact was produced."""
    overlay = json.loads(SOURCE_SUMMARY.read_text())
    overlay.pop("sample_hv_mappings", None)
    bundle = json.loads(ISLANDS_BUNDLE.read_text())
    island_entry = next(
        (entry for entry in bundle.get("islands", []) if int(entry.get("compared_buses", -1)) == bus_count),
        None,
    )
    return {
        "overlay_build": overlay,
        "staged_island_validation": island_entry,
        "regeneration_commands": [
            "python scripts/build_pypsa_eur_hv_de_mv_overlay.py",
            "python scripts/import_pypsa_eur_to_pandapower.py",
        ],
        "disclosures": [
            "OSM line parameters are typed per-voltage-class defaults, not measured impedances.",
            "Load and generation on the overlay are synthetically allocated (degree-weighted loads with distributed PV), not metered values; the case demonstrates methodology, not a historical snapshot.",
        ],
    }


def series_stats(values: list[float]) -> dict[str, float | int | None]:
    if not values:
        return {"count": 0, "min": None, "mean": None, "median": None, "p95": None, "max": None}
    ordered = sorted(values)
    count = len(ordered)

    def quantile(q: float) -> float:
        position = q * (count - 1)
        lower = math.floor(position)
        upper = math.ceil(position)
        if lower == upper:
            return ordered[lower]
        weight = position - lower
        return ordered[lower] * (1 - weight) + ordered[upper] * weight

    return {
        "count": count,
        "min": ordered[0],
        "mean": sum(ordered) / count,
        "median": quantile(0.5),
        "p95": quantile(0.95),
        "max": ordered[-1],
    }


def subtransmission_analysis(net, output_dir: Path) -> dict[str, object]:
    lower_kv, upper_kv = SUBTRANSMISSION_KV
    layer_buses = {
        int(idx) for idx, row in net.bus.iterrows() if lower_kv <= float(row.vn_kv) <= upper_kv
    }

    loading_rows: list[list[object]] = []
    loading_values: list[float] = []
    for idx, row in net.line.iterrows():
        if int(row.from_bus) not in layer_buses or int(row.to_bus) not in layer_buses:
            continue
        loading = float(net.res_line.at[idx, "loading_percent"])
        loading_values.append(loading)
        loading_rows.append(
            [
                str(row.get("name", f"line_{idx}")),
                float(net.bus.at[int(row.from_bus), "vn_kv"]),
                float(net.res_line.at[idx, "p_from_mw"]),
                float(net.res_line.at[idx, "q_from_mvar"]),
                loading,
            ]
        )
    loading_rows.sort(key=lambda entry: -float(entry[4]))
    write_csv(
        output_dir / "subtransmission_loading.csv",
        ["line", "vn_kv", "p_from_mw", "q_from_mvar", "loading_percent"],
        loading_rows,
    )

    boundary_rows: list[list[object]] = []
    for idx, row in net.trafo.iterrows():
        vn_hv = float(net.bus.at[int(row.hv_bus), "vn_kv"])
        vn_lv = float(net.bus.at[int(row.lv_bus), "vn_kv"])
        if vn_hv < BOUNDARY_HV_MIN_KV or vn_lv > upper_kv:
            continue
        boundary_rows.append(
            [
                str(row.get("name", f"trafo_{idx}")),
                vn_hv,
                vn_lv,
                float(net.res_trafo.at[idx, "p_hv_mw"]),
                float(net.res_trafo.at[idx, "q_hv_mvar"]),
                float(net.res_trafo.at[idx, "loading_percent"]),
            ]
        )
    boundary_rows.sort(key=lambda entry: -abs(float(entry[3])))
    write_csv(
        output_dir / "boundary_transformer_flows.csv",
        ["transformer", "vn_hv_kv", "vn_lv_kv", "p_hv_mw", "q_hv_mvar", "loading_percent"],
        boundary_rows,
    )

    voltage_rows: list[list[object]] = []
    voltage_levels: dict[float, list[float]] = {}
    for idx, row in net.bus.iterrows():
        voltage_levels.setdefault(float(row.vn_kv), []).append(float(net.res_bus.at[int(idx), "vm_pu"]))
    for vn_kv in sorted(voltage_levels):
        values = voltage_levels[vn_kv]
        voltage_rows.append([vn_kv, len(values), min(values), sum(values) / len(values), max(values)])
    write_csv(
        output_dir / "voltage_levels.csv",
        ["vn_kv", "bus_count", "min_vm_pu", "mean_vm_pu", "max_vm_pu"],
        voltage_rows,
    )

    boundary_p_values = [float(entry[3]) for entry in boundary_rows]
    return {
        "subtransmission_kv_band": list(SUBTRANSMISSION_KV),
        "subtransmission_bus_count": len(layer_buses),
        "subtransmission_line_loading_percent": series_stats(loading_values),
        "lines_above_80_percent": sum(1 for value in loading_values if value > 80.0),
        "lines_above_100_percent": sum(1 for value in loading_values if value > 100.0),
        "boundary_transformer_count": len(boundary_rows),
        "boundary_transformer_p_hv_mw": series_stats(boundary_p_values),
        "voltage_levels": [
            {"vn_kv": row[0], "bus_count": row[1], "min_vm_pu": row[2], "mean_vm_pu": row[3], "max_vm_pu": row[4]}
            for row in voltage_rows
        ],
    }


def main() -> None:
    output_dir = ensure_dir(REPO_ROOT / "docs" / "generated" / "uc4")
    adapter = PandapowerAdapter()
    validator = ValidationService()

    case = PandapowerImportAdapter().import_case(ISLAND_JSON).copy_with(case_id="pypsa_eur_hv_all_de_mv_island_00")
    # The staged island keeps the element indices of the full network it was carved from;
    # pandapower's PowerModels converter indexes ext_grid positionally and trips over the
    # resulting non-contiguous index, so renumber all element tables first.
    island_net = adapter.to_net(case)
    pp_toolbox.create_continuous_elements_index(island_net)
    case = CanonicalCase.from_pandapower(
        case_id=case.case_id,
        source_format=case.source_format,
        net=island_net,
    )
    reference = balanced_reference_validation(case)
    reference_net = reference["reference_net"]
    provenance = staged_provenance(bus_count=len(reference_net.bus))

    powermodels_json = adapter.export_powermodels_json(case, output_dir / f"{case.case_id}.powermodels.json")
    powermodels_validation = validator.validate_powermodels_export(
        case,
        powermodels_json=powermodels_json,
        julia_binary=detect_julia_binary(),
        julia_script=REPO_ROOT / "src" / "powermodelconverter" / "julia" / "run_powermodels_pf.jl",
        julia_depot=Path(os.environ.get("JULIA_DEPOT_PATH", str(REPO_ROOT / ".julia_depot"))),
    )

    analysis = subtransmission_analysis(reference_net, output_dir)

    summary = {
        "use_case": "uc4",
        "script": "docs/examples/uc4/run.py",
        "inputs": {"staged_island": str(ISLAND_JSON)},
        "model_size": {
            "buses": len(reference_net.bus),
            "lines": len(reference_net.line),
            "transformers": len(reference_net.trafo),
            "voltage_levels_kv": sorted({float(value) for value in reference_net.bus.vn_kv}),
        },
        "provenance": provenance,
        "validations": {
            "pandapower_self_check": reference["validation"],
            "pandapower_to_powermodels": powermodels_validation,
        },
        "analysis": analysis,
        "artifacts": {
            "powermodels_json": str(powermodels_json),
            "subtransmission_loading_csv": str(output_dir / "subtransmission_loading.csv"),
            "boundary_transformer_flows_csv": str(output_dir / "boundary_transformer_flows.csv"),
            "voltage_levels_csv": str(output_dir / "voltage_levels.csv"),
        },
    }
    write_json(output_dir / "summary.json", summary)
    print(json.dumps(json_ready(summary), indent=2))


if __name__ == "__main__":
    main()
