import math
from pathlib import Path

import pandapower as pp

from powermodelconverter.adapters.cgmes_export_adapter import CGMESExportAdapter
from powermodelconverter.adapters.cgmes_import_adapter import CGMESImportAdapter
from powermodelconverter.core.model import CanonicalCase
from powermodelconverter.validation.powerflow import ValidationService


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_cgmes_import_official_smallgrid_runs_and_validates() -> None:
    source = REPO_ROOT / "src/powermodelconverter/data/samples/cgmes/CGMES_v2.4.15_SmallGridTestConfiguration_BaseCase_Complete_v3.0.0.zip"
    boundary = REPO_ROOT / "src/powermodelconverter/data/samples/cgmes/CGMES_v2.4.15_SmallGridTestConfiguration_Boundary_v3.0.0.zip"
    case = CGMESImportAdapter().import_case(source.parent)
    assert case.source_format == "cgmes"
    assert len(case.table("bus")) > 0
    assert len(case.table("line")) > 0

    validator = ValidationService()
    reference = pp.from_json_string(case.metadata["pandapower_json"])
    pp.runpp(reference, algorithm="nr", init="auto", max_iteration=20, numba=False)
    slack_p, slack_q = validator._extract_balanced_slack(reference)
    reference_voltages = {
        validator._bus_key(reference, int(idx)): complex(
            float(row.vm_pu) * math.cos(math.radians(float(row.va_degree))),
            float(row.vm_pu) * math.sin(math.radians(float(row.va_degree))),
        )
        for idx, row in reference.res_bus.iterrows()
    }
    result = validator.validate_against_pandapower(
        case,
        reference_slack_p_mw=slack_p,
        reference_slack_q_mvar=slack_q,
        reference_voltages=reference_voltages,
    )
    assert result.passed is True
    assert source.exists() is True
    assert boundary.exists() is True


def test_balanced_pandapower_export_to_cgmes_roundtrip(tmp_path: Path) -> None:
    net = pp.create_empty_network(sn_mva=100)
    slack_bus = pp.create_bus(net, vn_kv=110, name="SlackBus")
    load_bus = pp.create_bus(net, vn_kv=110, name="LoadBus")
    pp.create_ext_grid(
        net,
        slack_bus,
        vm_pu=1.0,
        va_degree=0.0,
        max_p_mw=500.0,
        min_p_mw=0.0,
        max_q_mvar=500.0,
        min_q_mvar=-500.0,
    )
    pp.create_line_from_parameters(
        net,
        slack_bus,
        load_bus,
        length_km=10.0,
        r_ohm_per_km=0.05,
        x_ohm_per_km=0.2,
        c_nf_per_km=10.0,
        max_i_ka=1.0,
        r0_ohm_per_km=0.15,
        x0_ohm_per_km=0.6,
        c0_nf_per_km=5.0,
        g_us_per_km=0.0,
        g0_us_per_km=0.0,
        name="L1",
    )
    pp.create_load(net, load_bus, p_mw=50.0, q_mvar=10.0, name="LD1")

    case = CanonicalCase.from_pandapower(case_id="cgmes_smoke", source_format="pandapower", net=net)
    export = CGMESExportAdapter().export_case(case, tmp_path / "cgmes_smoke.zip")
    imported = CGMESImportAdapter().import_case(export)

    validator = ValidationService()
    reference = pp.from_json_string(case.metadata["pandapower_json"])
    pp.runpp(reference, algorithm="nr", init="auto", max_iteration=20, numba=False)
    slack_p, slack_q = validator._extract_balanced_slack(reference)
    reference_voltages = {
        validator._bus_key(reference, int(idx)): complex(
            float(row.vm_pu) * math.cos(math.radians(float(row.va_degree))),
            float(row.vm_pu) * math.sin(math.radians(float(row.va_degree))),
        )
        for idx, row in reference.res_bus.iterrows()
    }
    result = validator.validate_against_pandapower(
        imported,
        reference_slack_p_mw=slack_p,
        reference_slack_q_mvar=slack_q,
        reference_voltages=reference_voltages,
    )

    assert result.passed is True
    assert result.slack_delta_mva < 1e-9
    assert result.max_voltage_delta_pu < 1e-12
