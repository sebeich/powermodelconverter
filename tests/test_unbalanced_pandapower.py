from pathlib import Path

import pandapower as pp
import pandapower.networks as pn

from powermodelconverter.adapters.pandapower_adapter import PandapowerAdapter
from powermodelconverter.adapters.pandapower_import_adapter import PandapowerImportAdapter
from powermodelconverter.adapters.powermodels_distribution_adapter import PowerModelsDistributionAdapter
from powermodelconverter.validation.powerflow import ValidationService


def test_unbalanced_pandapower_roundtrip(tmp_path: Path) -> None:
    net = pn.ieee_european_lv_asymmetric()
    source = tmp_path / "ieee_european_lv_asymmetric.json"
    pp.to_json(net, str(source))

    case = PandapowerImportAdapter().import_case(source)
    assert case.is_unbalanced is True
    assert case.phase_count == 3

    result = ValidationService().validate_pandapower_unbalanced_roundtrip(case)
    assert result.passed is True
    assert result.details["mode"] == "unbalanced_3ph"

    validator = ValidationService()
    reference_net = PandapowerAdapter().run_power_flow_3ph(case)
    slack = validator._extract_3ph_slack(reference_net)
    export = PowerModelsDistributionAdapter().export_input(
        case,
        tmp_path / "ieee_european_lv_asymmetric_powermodelsdistribution.dss",
    )
    pmd_result = validator.validate_powermodelsdistribution_export(
        case,
        pmd_input_path=export,
        julia_binary=str(Path.home() / ".julia/juliaup/julia-1.12.3+0.x64.linux.gnu/bin/julia"),
        julia_script=Path(__file__).resolve().parents[1]
        / "src/powermodelconverter/julia_pmd/run_powermodels_distribution_pf.jl",
        julia_depot=Path(__file__).resolve().parents[1] / ".julia_depot",
        julia_project=Path(__file__).resolve().parents[1] / "src/powermodelconverter/julia_pmd",
        reference_slack_p_mw=sum(slack[key] for key in ("p_a_mw", "p_b_mw", "p_c_mw")),
        reference_slack_q_mvar=sum(slack[key] for key in ("q_a_mvar", "q_b_mvar", "q_c_mvar")),
        reference_node_voltages=validator._extract_3ph_node_voltages(reference_net),
    )
    assert pmd_result.passed is True
    assert pmd_result.details["backend"] == "powermodelsdistribution"
