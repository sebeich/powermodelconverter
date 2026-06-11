#!/usr/bin/env python3
"""
UC2 research-grade reproducer: IEEE European LV OpenDSS feeder to PyPSA capacity expansion.

The IEEE CSV benchmark package is unpacked into a generated OpenDSS master file that uses
per-winding transformer syntax. The imported unbalanced feeder is aggregated to the balanced
phase-sum representation used by the PyPSA route, validated against pandapower AC power flow,
and expanded as a 24-hour linear PyPSA investment problem with rooftop PV, home batteries, and
decentral air-source heat pumps.

Citations:
- Schneider, M.; Stetz, T.; Kuehn, M. et al., IEEE European Low Voltage Test Feeder, IEEE PES
  Distribution Test Feeder Working Group benchmark package, v2.
- Brown, T.; Hoersch, J.; Schlachtberger, D. (2018), "PyPSA: Python for Power System Analysis",
  Journal of Open Research Software.
- PyPSA technology-data repository, commit 64c3e6a872ccc86f6fca7d74640f1bc7979e89a4.
"""

from __future__ import annotations

import argparse
import copy
import csv
from dataclasses import asdict
import json
import math
from pathlib import Path
import shutil
import sys
import tempfile
import warnings
import zipfile

CURRENT_DIR = Path(__file__).resolve().parent
EXAMPLES_ROOT = CURRENT_DIR.parent
if str(EXAMPLES_ROOT) not in sys.path:
    sys.path.insert(0, str(EXAMPLES_ROOT))

from _common import REPO_ROOT
from _common import ensure_dir
from _common import json_ready
from _common import write_csv
from _common import write_json

import pandas as pd
import pandapower as pp

warnings.filterwarnings("ignore", category=pd.errors.PerformanceWarning)

from powermodelconverter.core.model import CanonicalCase
from powermodelconverter.core.pandapower_backend import PandapowerAdapter
from powermodelconverter.importers.opendss import OpenDSSImportAdapter
from powermodelconverter.importers.pypsa import PypsaAdapter
from powermodelconverter.validation.engine import ValidationService


IEEE_ZIP = REPO_ROOT / "input" / "ieee_european_lv" / "European_LV_Test_Feeder_v2.zip"
TECHNOLOGY_DATA = REPO_ROOT / "input" / "technology_data" / "costs_2030_64c3e6a8.csv"
PV_PROFILE = CURRENT_DIR / "inputs" / "pv_profile_summer_day.csv"
TECHNOLOGY_DATA_COMMIT = "64c3e6a872ccc86f6fca7d74640f1bc7979e89a4"
CASE_ID = "ieee_european_lv"

FALLBACK_COSTS_EUR_PER_MW = {
    "solar rooftop": 650_000.0,
    "home battery inverter": 304_927.1,
    "home battery storage": 271_290.8,
    "decentral air-sourced heat pump": 1_135_432.6,
}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ieee-zip", default=str(IEEE_ZIP), help="IEEE European LV benchmark ZIP.")
    parser.add_argument("--technology-data", default=str(TECHNOLOGY_DATA), help="technology-data costs CSV.")
    parser.add_argument("--pv-profile", default=str(PV_PROFILE), help="24-hour PV availability CSV.")
    parser.add_argument("--output-dir", default=str(REPO_ROOT / "docs" / "generated" / "uc2"))
    parser.add_argument("--solver", default="highs", help="PyPSA linear optimization solver.")
    return parser


def read_csv_from_zip(zip_path: Path, member: str) -> list[dict[str, str]]:
    with zipfile.ZipFile(zip_path) as archive:
        with archive.open(member) as handle:
            text = handle.read().decode("utf-8-sig")
    rows = [line for line in text.splitlines() if line.strip() and not line.lstrip().startswith("#")]
    return [
        {str(key).strip(): str(value).strip() for key, value in row.items()}
        for row in csv.DictReader(rows)
    ]


def phase_suffix(phases: str) -> str:
    mapping = {"A": "1", "B": "2", "C": "3", "N": "0"}
    nodes = [mapping[token] for token in phases.strip().upper() if token in mapping]
    return "." + ".".join(nodes) if nodes else ".1.2.3"


def load_first_profile_multipliers(zip_path: Path) -> dict[str, float]:
    multipliers: dict[str, float] = {}
    for row in read_csv_from_zip(zip_path, "European_LV_CSV/LoadShapes.csv"):
        shape = row["Name"].strip()
        profile = row["File"].strip()
        with zipfile.ZipFile(zip_path) as archive:
            raw = archive.read(f"European_LV_CSV/Load Profiles/{profile}").decode("utf-8-sig")
        for line in raw.splitlines():
            if not line.strip() or line.lstrip().startswith("#"):
                continue
            try:
                multipliers[shape.lower()] = float(line.split(",")[-1])
                break
            except ValueError:
                continue
        else:
            multipliers[shape.lower()] = 1.0
    return multipliers


def build_safe_opendss_master(zip_path: Path, output_dir: Path) -> Path:
    generated = ensure_dir(output_dir / "opendss_generated")
    master = generated / "Master.dss"
    load_multipliers = load_first_profile_multipliers(zip_path)

    lines: list[str] = [
        "Clear",
        "New Circuit.European_LV basekv=11 pu=1.05 phases=3 bus1=SourceBus.1.2.3 angle=0 frequency=50",
        "Edit Vsource.Source bus1=SourceBus.1.2.3 basekv=11 pu=1.05 phases=3",
    ]

    for row in read_csv_from_zip(zip_path, "European_LV_CSV/LineCodes.csv"):
        lines.append(
            "New Linecode.{Name} nphases=3 r1={R1} x1={X1} r0={R0} x0={X0} c1={C1} c0={C0} units={Units}".format(
                **row
            )
        )

    for row in read_csv_from_zip(zip_path, "European_LV_CSV/Transformer.csv"):
        name = row["Name"].strip()
        kva = float(row["MVA"]) * 1000.0
        lines.extend(
            [
                f"New Transformer.{name} phases={row['phases']} windings=2 xhl={row['%XHL']}",
                f"~ wdg=1 bus={row['bus1']}.1.2.3 conn={row['Conn_pri']} kv={row['kV_pri']} kva={kva:g} %r={row['% resistance']}",
                f"~ wdg=2 bus={row['bus2']}.1.2.3 conn={row['Conn_sec']} kv={row['kV_sec']} kva={kva:g} %r={row['% resistance']}",
            ]
        )

    for row in read_csv_from_zip(zip_path, "European_LV_CSV/Lines.csv"):
        lines.append(
            "New Line.{name} bus1={bus1}{nodes} bus2={bus2}{nodes} phases=3 length={length} units={units} linecode={linecode}".format(
                name=row["Name"].strip(),
                bus1=row["Bus1"].strip(),
                bus2=row["Bus2"].strip(),
                nodes=phase_suffix(row["Phases"]),
                length=row["Length"].strip(),
                units=row["Units"].strip(),
                linecode=row["LineCode"].strip(),
            )
        )

    for row in read_csv_from_zip(zip_path, "European_LV_CSV/Loads.csv"):
        pf = float(row["PF"])
        p_kw = float(row["kW"]) * load_multipliers.get(row["Yearly"].strip().lower(), 1.0)
        q_kvar = p_kw * math.tan(math.acos(pf))
        lines.append(
            "New Load.{name} bus1={bus}{nodes} phases={phases} kv={kv} model={model} conn={conn} kw={kw:.9g} kvar={kvar:.9g}".format(
                name=row["Name"].strip(),
                bus=row["Bus"].strip(),
                nodes=phase_suffix(row["phases"]),
                phases=row["numPhases"].strip(),
                kv=row["kV"].strip(),
                model=row["Model"].strip(),
                conn=row["Connection"].strip(),
                kw=p_kw,
                kvar=q_kvar,
            )
        )

    lines.extend(["Set VoltageBases=[11,0.416,0.23]", "CalcVoltageBases", "Solve"])
    master.write_text("\n".join(lines) + "\n")
    return master


def phase_sum_balanced_case(case: CanonicalCase) -> CanonicalCase:
    net = copy.deepcopy(case.net)

    if hasattr(net, "asymmetric_load") and len(net.asymmetric_load):
        for _, row in net.asymmetric_load.iterrows():
            p_mw = sum(float(row.get(f"p_{phase}_mw", 0.0) or 0.0) for phase in "abc")
            q_mvar = sum(float(row.get(f"q_{phase}_mvar", 0.0) or 0.0) for phase in "abc")
            pp.create_load(net, bus=int(row.bus), p_mw=p_mw, q_mvar=q_mvar, name=str(row["name"]))
        net.asymmetric_load.drop(net.asymmetric_load.index, inplace=True)

    if hasattr(net, "asymmetric_sgen") and len(net.asymmetric_sgen):
        for _, row in net.asymmetric_sgen.iterrows():
            p_mw = sum(float(row.get(f"p_{phase}_mw", 0.0) or 0.0) for phase in "abc")
            q_mvar = sum(float(row.get(f"q_{phase}_mvar", 0.0) or 0.0) for phase in "abc")
            pp.create_sgen(net, bus=int(row.bus), p_mw=p_mw, q_mvar=q_mvar, name=str(row["name"]))
        net.asymmetric_sgen.drop(net.asymmetric_sgen.index, inplace=True)

    return CanonicalCase.from_pandapower(
        case_id=CASE_ID,
        source_format="opendss",
        net=net,
        metadata={
            "is_unbalanced": False,
            "phase_count": 1,
            "aggregation": "balanced phase-sum of OpenDSS single-phase loads and shunts",
            "source_case_id": case.case_id,
        },
        source_path=case.source_path,
    )


def parse_published_voltage_snapshot(snapshot_path: Path) -> dict[str, complex]:
    table = pd.read_excel(snapshot_path, sheet_name="Voltages")
    table = table.rename(columns={column: str(column).strip() for column in table.columns})
    a = complex(math.cos(math.radians(120.0)), math.sin(math.radians(120.0)))
    voltages: dict[str, complex] = {}
    for _, row in table.iterrows():
        phase_values: list[complex] = []
        for idx in (1, 2, 3):
            pu = row.get(f"pu{idx}")
            angle = row.get(f"Angle{idx}")
            if pd.isna(pu) or pd.isna(angle):
                continue
            angle_rad = math.radians(float(angle))
            phase_values.append(complex(float(pu) * math.cos(angle_rad), float(pu) * math.sin(angle_rad)))
        if len(phase_values) != 3:
            continue
        positive_sequence = (phase_values[0] + a * phase_values[1] + a * a * phase_values[2]) / 3.0
        voltages[str(row["Bus"]).strip().lower()] = positive_sequence
    return voltages


def validate_against_published_snapshot(case: CanonicalCase, snapshot_path: Path) -> dict[str, object]:
    reference = parse_published_voltage_snapshot(snapshot_path)
    solved = PandapowerAdapter().run_power_flow(case)
    max_delta = 0.0
    compared = 0
    worst: list[tuple[float, str]] = []
    for idx, row in solved.res_bus.iterrows():
        bus_name = str(solved.bus.at[idx, "name"]).strip().lower()
        expected = reference.get(bus_name)
        if expected is None:
            continue
        angle_rad = math.radians(float(row.va_degree))
        actual = complex(float(row.vm_pu) * math.cos(angle_rad), float(row.vm_pu) * math.sin(angle_rad))
        delta = abs(actual - expected)
        max_delta = max(max_delta, delta)
        compared += 1
        worst.append((delta, bus_name))
    worst.sort(reverse=True)
    return {
        "reference": "published OpenDSS Snapshot_1min_Initialization_Off Peak.xlsx positive-sequence bus phasors",
        "compared_buses": compared,
        "max_voltage_delta_pu": max_delta,
        "tolerance_pu": 1e-3,
        "passed": compared > 0 and max_delta <= 1e-3,
        "worst_buses": [{"bus": bus, "delta_pu": delta} for delta, bus in worst[:10]],
    }


def annualized_costs(costs_csv: Path) -> dict[str, float]:
    if not costs_csv.exists():
        return dict(FALLBACK_COSTS_EUR_PER_MW)

    table = pd.read_csv(costs_csv)
    costs = dict(FALLBACK_COSTS_EUR_PER_MW)
    for technology, parameter in [
        ("solar rooftop", "investment"),
        ("home battery inverter", "investment"),
        ("home battery storage", "investment"),
        ("decentral air-sourced heat pump", "investment"),
    ]:
        hit = table[(table["technology"] == technology) & (table["parameter"] == parameter)]
        if not hit.empty:
            costs[technology] = float(hit.iloc[0]["value"]) * 1000.0
    return costs


def build_planning_network(case: CanonicalCase, pv_profile: Path, costs_csv: Path):
    network = PypsaAdapter().to_net(case)
    snapshots = pd.date_range("2030-07-01 00:00", periods=24, freq="h")
    network.set_snapshots(snapshots)
    network.snapshot_weightings.objective = 365.0
    for carrier in ("AC", "solar rooftop", "decentral air-sourced heat pump", "home battery"):
        if carrier not in network.carriers.index:
            network.add("Carrier", carrier)

    profile = pd.read_csv(pv_profile)["p_max_pu"].astype(float).tolist()
    load_shape = pd.Series(
        [0.56, 0.52, 0.50, 0.49, 0.51, 0.58, 0.70, 0.82, 0.88, 0.86, 0.82, 0.78,
         0.76, 0.75, 0.76, 0.82, 0.94, 1.08, 1.18, 1.16, 1.02, 0.88, 0.74, 0.64],
        index=snapshots,
    )
    heat_shape = pd.Series(
        [0.92, 0.88, 0.86, 0.84, 0.85, 0.90, 1.00, 1.10, 1.08, 1.02, 0.96, 0.90,
         0.84, 0.80, 0.78, 0.82, 0.94, 1.12, 1.22, 1.20, 1.10, 1.02, 0.98, 0.94],
        index=snapshots,
    )
    costs = annualized_costs(costs_csv)

    load_by_bus = network.loads.groupby("bus").p_set.sum().to_dict() if not network.loads.empty else {}
    if not network.loads.empty:
        for load_name, row in network.loads.iterrows():
            network.loads_t.p_set.loc[:, load_name] = float(row.p_set) * load_shape
            network.loads_t.q_set.loc[:, load_name] = float(row.q_set) * load_shape

    for bus_name in network.buses.index:
        base_load_mw = float(load_by_bus.get(bus_name, 0.0))
        if base_load_mw <= 0.0:
            continue
        pv_name = f"pv_{bus_name}"
        hp_name = f"heat_pump_{bus_name}"
        store_name = f"battery_{bus_name}"

        network.add(
            "Generator",
            pv_name,
            bus=bus_name,
            carrier="solar rooftop",
            p_nom_extendable=True,
            p_nom_max=max(base_load_mw * 3.0, 0.003),
            capital_cost=costs["solar rooftop"],
            marginal_cost=0.0,
        )
        network.generators_t.p_max_pu.loc[:, pv_name] = profile

        network.add(
            "Load",
            hp_name,
            bus=bus_name,
            carrier="decentral air-sourced heat pump",
            p_set=base_load_mw * 0.35 / 3.6,
        )
        network.loads_t.p_set.loc[:, hp_name] = base_load_mw * 0.35 / 3.6 * heat_shape
        network.loads_t.q_set.loc[:, hp_name] = 0.0

        network.add(
            "StorageUnit",
            store_name,
            bus=bus_name,
            carrier="home battery",
            p_nom_extendable=True,
            capital_cost=costs["home battery inverter"] + 4.0 * costs["home battery storage"],
            max_hours=4.0,
            efficiency_store=0.96,
            efficiency_dispatch=0.96,
            cyclic_state_of_charge=True,
        )

    return network


def run_capacity_expansion(network, solver: str) -> dict[str, object]:
    try:
        status, condition = network.optimize(solver_name=solver, include_objective_constant=False)
    except Exception as exc:
        return {
            "status": "not_solved",
            "condition": type(exc).__name__,
            "message": str(exc),
            "objective": None,
        }
    return {
        "status": str(status),
        "condition": str(condition),
        "objective": float(network.objective) if network.objective is not None else None,
    }


def expansion_rows(network) -> list[list[object]]:
    rows: list[list[object]] = []
    for name, row in network.generators.iterrows():
        if bool(row.get("p_nom_extendable", False)):
            rows.append([str(row.bus), str(name), str(row.carrier), float(row.p_nom_opt)])
    for name, row in network.storage_units.iterrows():
        if bool(row.get("p_nom_extendable", False)):
            rows.append([str(row.bus), str(name), str(row.carrier), float(row.p_nom_opt)])
    return sorted(rows)


def dispatch_rows(network) -> list[list[object]]:
    rows: list[list[object]] = []
    for snapshot in list(network.snapshots):
        pv_mw = float(network.generators_t.p.loc[snapshot].filter(like="pv_").sum()) if len(network.generators_t.p) else 0.0
        storage_mw = float(network.storage_units_t.p.loc[snapshot].sum()) if len(network.storage_units_t.p) else 0.0
        load_mw = float(network.loads_t.p_set.loc[snapshot].sum()) if len(network.loads_t.p_set) else 0.0
        rows.append([str(snapshot), pv_mw, storage_mw, load_mw])
    return rows


def main() -> None:
    args = build_parser().parse_args()
    output_dir = ensure_dir(Path(args.output_dir))
    source_zip = Path(args.ieee_zip)
    costs_csv = Path(args.technology_data)
    pv_profile = Path(args.pv_profile)

    with tempfile.TemporaryDirectory(prefix="pmc_uc2_") as tmp:
        work = Path(tmp)
        with zipfile.ZipFile(source_zip) as archive:
            archive.extractall(work / "ieee")
        snapshot = work / "ieee" / "Solutions" / "OpenDSS" / "Snapshots" / "Snapshot_1min_Initialization_Off Peak.xlsx"
        snapshot_copy = output_dir / snapshot.name
        shutil.copy2(snapshot, snapshot_copy)

        master = build_safe_opendss_master(source_zip, work)
        imported_case = OpenDSSImportAdapter().import_case(master)
        case = phase_sum_balanced_case(imported_case)

    pandapower = PandapowerAdapter()
    pypsa = PypsaAdapter()
    validator = ValidationService()

    pandapower_json = pandapower.export_json(case, output_dir / f"{CASE_ID}.pandapower.json")
    pypsa_netcdf = pypsa.export_netcdf(case, output_dir / f"{CASE_ID}.pypsa.nc")
    source_snapshot_validation = validate_against_published_snapshot(case, snapshot_copy)
    pypsa_validation = validator.validate_pypsa_export(case, pypsa_path=pypsa_netcdf)

    planning_network = build_planning_network(case, pv_profile, costs_csv)
    planning_status = run_capacity_expansion(planning_network, args.solver)
    expansion_csv = write_csv(output_dir / "expansion.csv", ["bus", "asset", "technology", "installed_mw"], expansion_rows(planning_network))
    dispatch_csv = write_csv(output_dir / "dispatch.csv", ["snapshot", "pv_mw", "battery_dispatch_mw", "load_mw"], dispatch_rows(planning_network))
    planning_network.export_to_netcdf(output_dir / f"{CASE_ID}.planning.pypsa.nc")

    summary = {
        "use_case": "uc2",
        "script": "docs/examples/uc2/run.py",
        "case_id": CASE_ID,
        "inputs": {
            "ieee_zip": source_zip,
            "technology_data": costs_csv,
            "technology_data_commit": TECHNOLOGY_DATA_COMMIT,
            "pv_profile": pv_profile,
            "published_opendss_snapshot": "Solutions/OpenDSS/Snapshots/Snapshot_1min_Initialization_Off Peak.xlsx",
        },
        "model": {
            "source_import": "generated safe OpenDSS master from IEEE CSV package",
            "aggregation": "balanced phase-sum",
            "bus_count": len(case.net.bus),
            "line_count": len(case.net.line),
            "transformer_count": len(case.net.trafo),
            "load_count": len(case.net.load),
        },
        "validation": {
            "published_snapshot_reference": snapshot_copy,
            "opendss_snapshot_to_pandapower": source_snapshot_validation,
            "pandapower_to_pypsa": asdict(pypsa_validation),
        },
        "capacity_expansion": planning_status,
        "artifacts": {
            "pandapower_json": pandapower_json,
            "pypsa_netcdf": pypsa_netcdf,
            "planning_pypsa_netcdf": output_dir / f"{CASE_ID}.planning.pypsa.nc",
            "expansion_csv": expansion_csv,
            "dispatch_csv": dispatch_csv,
        },
    }
    write_json(output_dir / "summary.json", summary)
    print(json.dumps(json_ready(summary), indent=2))


if __name__ == "__main__":
    main()
