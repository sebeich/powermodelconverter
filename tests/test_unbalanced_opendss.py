from pathlib import Path

from powermodelconverter.importers.opendss import OpenDSSImportAdapter
from powermodelconverter.exporters.powermodels_distribution import PowerModelsDistributionAdapter
from powermodelconverter.runtime import resolve_julia_binary
from powermodelconverter.validation.powerflow import ValidationService


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_unbalanced_opendss_roundtrip_and_pmd_validation(tmp_path: Path) -> None:
    source = REPO_ROOT / "src/powermodelconverter/data/samples/opendss/minimal_unbalanced_3ph.dss"
    adapter = OpenDSSImportAdapter()
    reference = adapter.solve_source_case(source)
    case = adapter.import_case(source)

    assert case.is_unbalanced is True
    assert len(case.table("asymmetric_load")) == 3

    validator = ValidationService()
    pandapower_result = validator.validate_opendss_unbalanced_roundtrip(case, reference)
    assert pandapower_result.passed is True
    assert pandapower_result.details["backend"] == "pandapower"

    export_path = PowerModelsDistributionAdapter().export_input(
        case,
        tmp_path / "minimal_unbalanced_3ph_powermodelsdistribution.dss",
    )
    pmd_result = validator.validate_powermodelsdistribution_export(
        case,
        pmd_input_path=export_path,
        julia_binary=resolve_julia_binary(),
        julia_script=REPO_ROOT / "src/powermodelconverter/julia_pmd/run_powermodels_distribution_pf.jl",
        julia_depot=REPO_ROOT / ".julia_depot",
        julia_project=REPO_ROOT / "src/powermodelconverter/julia_pmd",
        reference_slack_p_mw=reference.slack_p_mw,
        reference_slack_q_mvar=reference.slack_q_mvar,
        reference_node_voltages=reference.node_voltages,
    )
    assert pmd_result.passed is True
    assert pmd_result.details["backend"] == "powermodelsdistribution"
