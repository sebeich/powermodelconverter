#!/usr/bin/env python3
"""
UC3 research-grade reproducer: CGMES exchange model to N-1 screening and loss-minimization AC-OPF.

The ENTSO-E SmallGrid conformity package is imported through pmc, anchored against the
originating TSO's own solved state (the SV profile shipped inside the package), roundtripped
through the CGMES exporter, screened for single-element contingencies in pandapower, and
finally dispatched with a loss-minimization AC-OPF in PowerModels.jl.

Citations:
- ENTSO-E, "CGMES Conformity Assessment Scheme — Test Configurations", v2.4.15.
- IEC 61970-600-1/600-2, Common Grid Model Exchange Standard (CGMES).
- Thurner et al. (2018), "pandapower — An Open-Source Python Tool for Convenient Modeling,
  Analysis, and Optimization of Electric Power Systems", IEEE TPWRS.
- Coffrin et al. (2018), "PowerModels.jl: An Open-Source Framework for Exploring Power Flow
  Formulations", PSCC.
"""

from __future__ import annotations

import copy
import json
import math
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import xml.etree.ElementTree as ET
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

import pandapower as pp

from powermodelconverter.core.model import CanonicalCase
from powermodelconverter.core.pandapower_backend import PandapowerAdapter
from powermodelconverter.exporters.cgmes import CGMESExportAdapter
from powermodelconverter.importers.cgmes import CGMESImportAdapter
from powermodelconverter.validation.engine import ValidationService


CGMES_DIR = REPO_ROOT / "validation_cases" / "native" / "cgmes"
BASECASE_ZIP = CGMES_DIR / "CGMES_v2.4.15_SmallGridTestConfiguration_BaseCase_Complete_v3.0.0.zip"
CASE_ID = "cgmes_smallgrid_basecase"

SV_ANCHOR_TOLERANCE_PU = 1e-3
VM_BAND = (0.9, 1.1)
LOADING_LIMIT_PERCENT = 100.0


def detect_julia_binary() -> str:
    return os.environ.get("PMC_JULIA_BINARY") or os.environ.get("JULIA_BIN") or "julia"


def julia_depot() -> Path:
    return Path(os.environ.get("JULIA_DEPOT_PATH", str(REPO_ROOT / ".julia_depot")))


def _local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def parse_sv_profile(zip_path: Path) -> dict[str, dict[str, float]]:
    """Extract SvVoltage records (kV magnitude, degree angle) keyed by TopologicalNode rdf id."""
    sv_name = next(
        name for name in zipfile.ZipFile(zip_path).namelist() if "_SV_" in name and name.endswith(".xml")
    )
    with zipfile.ZipFile(zip_path).open(sv_name) as handle:
        root = ET.parse(handle).getroot()
    records: dict[str, dict[str, float]] = {}
    for element in root:
        if _local_name(element.tag) != "SvVoltage":
            continue
        voltage_kv = None
        angle_degree = None
        topological_node = None
        for child in element:
            name = _local_name(child.tag)
            if name == "SvVoltage.v":
                voltage_kv = float(child.text)
            elif name == "SvVoltage.angle":
                angle_degree = float(child.text)
            elif name == "SvVoltage.TopologicalNode":
                resource = next(iter(child.attrib.values()), "")
                topological_node = resource.lstrip("#")
        if topological_node and voltage_kv is not None and angle_degree is not None:
            records[topological_node] = {"v_kv": voltage_kv, "angle_degree": angle_degree}
    return records


def sv_anchor_validation(case: CanonicalCase, solved_net) -> dict[str, object]:
    """Compare the pmc-imported model's power flow against the TSO's published SV solved state."""
    sv_records = parse_sv_profile(BASECASE_ZIP)
    bus_table = solved_net.bus
    id_column = next(
        (column for column in ("origin_id", "cim_topnode") if column in bus_table.columns),
        None,
    )

    max_vm_delta = 0.0
    max_angle_delta = 0.0
    max_complex_delta = 0.0
    compared = 0
    rows: list[list[object]] = []
    for idx, bus in bus_table.iterrows():
        key = str(bus[id_column]) if id_column else None
        record = sv_records.get(key) if key else None
        if record is None:
            continue
        vn_kv = float(bus["vn_kv"])
        reference_vm = record["v_kv"] / vn_kv
        reference_angle = record["angle_degree"]
        actual_vm = float(solved_net.res_bus.at[idx, "vm_pu"])
        actual_angle = float(solved_net.res_bus.at[idx, "va_degree"])
        reference = complex(
            reference_vm * math.cos(math.radians(reference_angle)),
            reference_vm * math.sin(math.radians(reference_angle)),
        )
        actual = complex(
            actual_vm * math.cos(math.radians(actual_angle)),
            actual_vm * math.sin(math.radians(actual_angle)),
        )
        vm_delta = abs(actual_vm - reference_vm)
        angle_delta = abs(actual_angle - reference_angle)
        max_vm_delta = max(max_vm_delta, vm_delta)
        max_angle_delta = max(max_angle_delta, angle_delta)
        max_complex_delta = max(max_complex_delta, abs(actual - reference))
        compared += 1
        rows.append([str(bus.get("name", idx)), vn_kv, reference_vm, actual_vm, vm_delta, reference_angle, actual_angle, angle_delta])

    rows.sort(key=lambda row: -float(row[4]))
    return {
        "reference": "SV profile shipped inside the CGMES BaseCase package (TSO solved state)",
        "sv_node_count": len(sv_records),
        "compared_buses": compared,
        "bus_id_column": id_column,
        "max_vm_delta_pu": max_vm_delta,
        "max_angle_delta_degree": max_angle_delta,
        "max_complex_voltage_delta_pu": max_complex_delta,
        "tolerance_pu": SV_ANCHOR_TOLERANCE_PU,
        "passed": compared > 0 and max_complex_delta <= SV_ANCHOR_TOLERANCE_PU,
        "table_rows": rows,
    }


def validate_case_against_reference(reference_case: CanonicalCase, candidate_case: CanonicalCase) -> object:
    validator = ValidationService()
    reference = PandapowerAdapter().run_power_flow(reference_case)
    slack_p, slack_q = validator._extract_balanced_slack(reference)
    reference_voltages = {
        validator._bus_key(reference, int(idx)): complex(
            float(row.vm_pu) * math.cos(math.radians(float(row.va_degree))),
            float(row.vm_pu) * math.sin(math.radians(float(row.va_degree))),
        )
        for idx, row in reference.res_bus.iterrows()
    }
    return validator.validate_against_pandapower(
        candidate_case,
        reference_slack_p_mw=slack_p,
        reference_slack_q_mvar=slack_q,
        reference_voltages=reference_voltages,
    )


def build_exporter_projection(case: CanonicalCase, solved_net) -> tuple[CanonicalCase, dict[str, object]]:
    """Freeze shunts and wards at their solved-state PQ injections so the CGMES exporter accepts the net.

    The exporter does not yet carry shunt or ward equipment classes; replacing them with the
    constant-PQ injection observed at the solved operating point keeps that operating point an
    exact fixed point of the projected network.
    """
    net = copy.deepcopy(solved_net)
    frozen = {"shunt": {"count": 0, "p_mw": 0.0, "q_mvar": 0.0}, "ward": {"count": 0, "p_mw": 0.0, "q_mvar": 0.0}}

    if hasattr(net, "shunt") and len(net.shunt):
        for idx, row in net.shunt.iterrows():
            p_mw = float(net.res_shunt.at[idx, "p_mw"]) if bool(row.get("in_service", True)) else 0.0
            q_mvar = float(net.res_shunt.at[idx, "q_mvar"]) if bool(row.get("in_service", True)) else 0.0
            pp.create_load(net, int(row.bus), p_mw=p_mw, q_mvar=q_mvar, name=f"frozen_shunt_{idx}")
            frozen["shunt"]["count"] += 1
            frozen["shunt"]["p_mw"] += p_mw
            frozen["shunt"]["q_mvar"] += q_mvar
        net.shunt.drop(net.shunt.index, inplace=True)

    if hasattr(net, "ward") and len(net.ward):
        for idx, row in net.ward.iterrows():
            p_mw = float(net.res_ward.at[idx, "p_mw"]) if bool(row.get("in_service", True)) else 0.0
            q_mvar = float(net.res_ward.at[idx, "q_mvar"]) if bool(row.get("in_service", True)) else 0.0
            pp.create_load(net, int(row.bus), p_mw=p_mw, q_mvar=q_mvar, name=f"frozen_ward_{idx}")
            frozen["ward"]["count"] += 1
            frozen["ward"]["p_mw"] += p_mw
            frozen["ward"]["q_mvar"] += q_mvar
        net.ward.drop(net.ward.index, inplace=True)

    projection = CanonicalCase.from_pandapower(
        case_id=f"{case.case_id}_exporter_projection",
        source_format="pandapower",
        net=net,
    )
    return projection, frozen


def run_roundtrip(case: CanonicalCase, solved_net, output_dir: Path) -> dict[str, object]:
    projection, frozen = build_exporter_projection(case, solved_net)
    export_path = CGMESExportAdapter().export_case(projection, output_dir / f"{CASE_ID}.roundtrip.zip")
    reimported = CGMESImportAdapter().import_case(export_path)
    validation = validate_case_against_reference(projection, reimported)
    return {
        "mode": "full SmallGrid topology with shunts/wards frozen at solved-state PQ",
        "frozen_elements": frozen,
        "artifact": str(export_path),
        "validation": validation,
    }


def _violated_elements(net) -> tuple[set[tuple[str, int]], set[int]]:
    overloaded: set[tuple[str, int]] = set()
    for result_table, element_type in (("res_line", "line"), ("res_trafo", "trafo")):
        results = getattr(net, result_table, None)
        if results is not None and len(results) and "loading_percent" in results.columns:
            for idx, value in results.loading_percent.dropna().items():
                if float(value) > LOADING_LIMIT_PERCENT:
                    overloaded.add((element_type, int(idx)))
    out_of_band = {
        int(idx)
        for idx, vm in net.res_bus.vm_pu.items()
        if float(vm) < VM_BAND[0] or float(vm) > VM_BAND[1]
    }
    return overloaded, out_of_band


def n_minus_1_screening(solved_net, output_dir: Path) -> dict[str, object]:
    base_overloaded, base_out_of_band = _violated_elements(solved_net)
    base_loading = max(
        float(solved_net.res_line.loading_percent.max()),
        float(solved_net.res_trafo.loading_percent.max()),
    )

    rows: list[list[object]] = []
    counters = {"contingencies": 0, "converged": 0, "new_loading_violations": 0, "new_voltage_violations": 0, "non_converged": 0}

    for element_type in ("line", "trafo"):
        table = getattr(solved_net, element_type)
        for idx, row in table.iterrows():
            if not bool(row.get("in_service", True)):
                continue
            counters["contingencies"] += 1
            net = copy.deepcopy(solved_net)
            getattr(net, element_type).at[idx, "in_service"] = False
            name = str(row.get("name", f"{element_type}_{idx}"))
            try:
                pp.runpp(net)
            except Exception as error:  # noqa: BLE001 - record any solver failure as non-convergence
                counters["non_converged"] += 1
                rows.append([element_type, name, False, None, None, None, None, None, type(error).__name__])
                continue
            counters["converged"] += 1
            overloaded, out_of_band = _violated_elements(net)
            new_overloads = len(overloaded - base_overloaded - {(element_type, int(idx))})
            new_voltage = len(out_of_band - base_out_of_band)
            loading_values = []
            for result_table in ("res_line", "res_trafo"):
                results = getattr(net, result_table, None)
                if results is not None and len(results) and "loading_percent" in results.columns:
                    loading_values.extend(float(v) for v in results.loading_percent.dropna())
            max_loading = max(loading_values) if loading_values else None
            min_vm = float(net.res_bus.vm_pu.min())
            max_vm = float(net.res_bus.vm_pu.max())
            counters["new_loading_violations"] += int(new_overloads > 0)
            counters["new_voltage_violations"] += int(new_voltage > 0)
            rows.append([element_type, name, True, max_loading, min_vm, max_vm, new_overloads, new_voltage, ""])

    table_path = write_csv(
        output_dir / "contingency_table.csv",
        ["element_type", "name", "converged", "max_loading_percent", "min_vm_pu", "max_vm_pu", "new_loading_violations", "new_voltage_violations", "error"],
        rows,
    )
    return {
        "screened_elements": counters["contingencies"],
        "converged": counters["converged"],
        "non_converged": counters["non_converged"],
        "base_case": {
            "max_loading_percent": base_loading,
            "overloaded_elements": len(base_overloaded),
            "buses_outside_voltage_band": len(base_out_of_band),
        },
        "cases_with_new_loading_violations": counters["new_loading_violations"],
        "cases_with_new_voltage_violations": counters["new_voltage_violations"],
        "loading_limit_percent": LOADING_LIMIT_PERCENT,
        "voltage_band_pu": list(VM_BAND),
        "thermal_rating_disclosure": "cim2pp does not map CGMES CurrentLimits into max_i_ka for this package; line ratings default to a 1 kA placeholder, so loading percentages are relative indicators rather than absolute thermal violations.",
        "table": str(table_path),
    }


def prepare_loss_min_opf_input(powermodels_json: Path) -> Path:
    """Copy the validated PowerModels export and inject a uniform marginal cost on every generator.

    With identical linear costs on all sources, minimizing total generation cost is equivalent to
    minimizing network losses, which is the natural cost-free objective for a CGMES exchange model
    (the format carries no economic data). Missing operational bounds are filled with a documented
    headroom rule around the base dispatch.
    """
    data = json.loads(powermodels_json.read_text())
    data.pop("user_defined_params", None)

    for gen in data.get("gen", {}).values():
        pg = float(gen.get("pg", 0.0) or 0.0)
        qg = float(gen.get("qg", 0.0) or 0.0)
        pmax = gen.get("pmax")
        if pmax is None or not math.isfinite(float(pmax)) or float(pmax) <= 0.0:
            gen["pmax"] = max(pg, 0.0) + max(0.3 * abs(pg), 0.1)
        pmin = gen.get("pmin")
        if pmin is None or not math.isfinite(float(pmin)):
            gen["pmin"] = min(0.0, pg)
        qmax = gen.get("qmax")
        if qmax is None or not math.isfinite(float(qmax)):
            gen["qmax"] = max(1.5 * abs(qg), 0.5)
        qmin = gen.get("qmin")
        if qmin is None or not math.isfinite(float(qmin)):
            gen["qmin"] = -float(gen["qmax"])
        gen["model"] = 2
        gen["ncost"] = 3
        gen["cost"] = [0.0, 1.0, 0.0]

    for bus in data.get("bus", {}).values():
        vmax = bus.get("vmax")
        if vmax is None or not math.isfinite(float(vmax)) or float(vmax) <= 0.0:
            bus["vmax"] = 1.1
        vmin = bus.get("vmin")
        if vmin is None or not math.isfinite(float(vmin)) or float(vmin) <= 0.0:
            bus["vmin"] = 0.9

    with tempfile.NamedTemporaryFile(mode="w", suffix=".powermodels.json", prefix="pmc_uc3_", delete=False) as handle:
        json.dump(data, handle)
        return Path(handle.name)


def run_loss_min_opf(powermodels_json: Path) -> dict[str, object]:
    opf_input = prepare_loss_min_opf_input(powermodels_json)
    script = CURRENT_DIR / "loss_min_opf.jl"
    project = REPO_ROOT / "src" / "powermodelconverter" / "julia"
    env = {
        **os.environ,
        "JULIA_PROJECT": str(project),
        "JULIA_DEPOT_PATH": str(julia_depot()),
    }
    started = time.time()
    completed = subprocess.run(
        [detect_julia_binary(), str(script), str(opf_input)],
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )
    wall_time = time.time() - started
    if completed.returncode != 0:
        raise RuntimeError(completed.stderr.strip() or completed.stdout.strip() or "Julia OPF run failed")
    payload = None
    for line in reversed(completed.stdout.strip().splitlines()):
        candidate = line.strip()
        if candidate.startswith("{"):
            payload = json.loads(candidate)
            break
    if payload is None:
        raise RuntimeError(f"Julia OPF run returned no JSON payload: {completed.stdout.strip()}")
    payload["wall_time_seconds"] = wall_time
    return payload


def base_case_losses_mw(solved_net) -> float:
    losses = 0.0
    for table in ("res_line", "res_trafo", "res_trafo3w"):
        results = getattr(solved_net, table, None)
        if results is not None and len(results) and "pl_mw" in results.columns:
            losses += float(results.pl_mw.sum())
    return losses


def main() -> None:
    output_dir = ensure_dir(REPO_ROOT / "docs" / "generated" / "uc3")
    adapter = PandapowerAdapter()
    validator = ValidationService()

    case = CGMESImportAdapter().import_case(CGMES_DIR).copy_with(case_id=CASE_ID)
    solved_net = adapter.run_power_flow(case)

    import_validation = validate_case_against_reference(case, case)
    sv_anchor = sv_anchor_validation(case, solved_net)
    write_csv(
        output_dir / "sv_anchor_table.csv",
        ["bus", "vn_kv", "sv_vm_pu", "pmc_vm_pu", "vm_delta_pu", "sv_angle_degree", "pmc_angle_degree", "angle_delta_degree"],
        sv_anchor.pop("table_rows"),
    )

    roundtrip = run_roundtrip(case, solved_net, output_dir)
    contingencies = n_minus_1_screening(solved_net, output_dir)

    powermodels_json = adapter.export_powermodels_json(case, output_dir / f"{CASE_ID}.powermodels.json")
    powermodels_validation = validator.validate_powermodels_export(
        case,
        powermodels_json=powermodels_json,
        julia_binary=detect_julia_binary(),
        julia_script=REPO_ROOT / "src" / "powermodelconverter" / "julia" / "run_powermodels_pf.jl",
        julia_depot=julia_depot(),
    )

    opf = run_loss_min_opf(powermodels_json)
    base_losses = base_case_losses_mw(solved_net)
    base_mva = float(opf.get("base_mva", 1.0))
    total_load_mw = float(opf.get("total_load_pu", 0.0)) * base_mva
    opf_generation_mw = (float(opf["total_pg_pu"]) * base_mva) if opf.get("total_pg_pu") is not None else None
    opf_losses = (opf_generation_mw - total_load_mw) if opf_generation_mw is not None else None

    summary = {
        "use_case": "uc3",
        "script": "docs/examples/uc3/run.py",
        "inputs": {
            "cgmes_package": str(BASECASE_ZIP),
            "boundary_package": str(CGMES_DIR / "CGMES_v2.4.15_SmallGridTestConfiguration_Boundary_v3.0.0.zip"),
        },
        "model_size": case.element_counts,
        "validations": {
            "cgmes_import_self_check": import_validation,
            "sv_anchor": sv_anchor,
            "cgmes_roundtrip": roundtrip,
            "pandapower_to_powermodels": powermodels_validation,
        },
        "n_minus_1": contingencies,
        "loss_min_opf": {
            **opf,
            "objective_interpretation": "uniform unit marginal cost on every generator => objective equals total generation in pu, so the optimum minimizes losses",
            "base_pf_losses_mw": base_losses,
            "opf_generation_mw": opf_generation_mw,
            "opf_total_load_mw": total_load_mw,
            "opf_losses_mw": opf_losses,
            "loss_reduction_mw": (base_losses - opf_losses) if opf_losses is not None else None,
        },
        "artifacts": {
            "sv_anchor_table_csv": str(output_dir / "sv_anchor_table.csv"),
            "contingency_table_csv": str(output_dir / "contingency_table.csv"),
            "powermodels_json": str(powermodels_json),
            "roundtrip_zip": roundtrip["artifact"],
        },
    }
    write_json(output_dir / "summary.json", summary)
    print(json.dumps(json_ready(summary), indent=2))


if __name__ == "__main__":
    main()
