from pathlib import Path

from powermodelconverter.adapters.matpower_adapter import MatpowerImportAdapter
from powermodelconverter.adapters.opendss_adapter import OpenDSSImportAdapter
from powermodelconverter.adapters.simbench_adapter import SimbenchImportAdapter
from powermodelconverter.validation.powerflow import ValidationService


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_matpower_import() -> None:
    case = MatpowerImportAdapter().import_case(
        REPO_ROOT / "src/powermodelconverter/data/samples/matpower/case9.m"
    )
    assert case.case_id == "case9"
    assert case.source_format == "matpower"
    assert len(case.table("bus")) == 9


def test_opendss_import_and_validation() -> None:
    source = REPO_ROOT / "src/powermodelconverter/data/samples/opendss/minimal_radial.dss"
    adapter = OpenDSSImportAdapter()
    reference = adapter.solve_source_case(source)
    case = adapter.import_case(source)
    result = ValidationService().validate_opendss_roundtrip(case, reference)
    assert result.passed is True


def test_simbench_import_normalizes_indices() -> None:
    case = SimbenchImportAdapter().import_case("1-HV-mixed--0-no_sw")
    assert case.source_format == "simbench"
    assert case.metadata["normalized_indices"] is True
