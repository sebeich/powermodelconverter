from pathlib import Path

import pandapower.networks as pn

from powermodelconverter.adapters.matpower_adapter import MatpowerImportAdapter
from powermodelconverter.adapters.opendss_export_adapter import OpenDSSExportAdapter
from powermodelconverter.adapters.opendss_adapter import OpenDSSImportAdapter
from powermodelconverter.adapters.pandapower_adapter import PandapowerAdapter
from powermodelconverter.adapters.pandapower_import_adapter import PandapowerImportAdapter
from powermodelconverter.adapters.pypsa_adapter import PypsaAdapter
from powermodelconverter.adapters.pypsa_import_adapter import PypsaImportAdapter
from powermodelconverter.adapters.simbench_adapter import SimbenchImportAdapter
from powermodelconverter.core.model import CanonicalCase
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
    assert result.details["compared_buses"] == len(reference.voltages)


def test_simbench_import_normalizes_indices() -> None:
    case = SimbenchImportAdapter().import_case("1-HV-mixed--0-no_sw")
    assert case.source_format == "simbench"
    assert case.metadata["normalized_indices"] is True


def test_pypsa_export_and_import_validate_against_pandapower(tmp_path: Path) -> None:
    source_net = pn.case9()
    case = CanonicalCase.from_pandapower(case_id="case9", source_format="pandapower", net=source_net)

    pypsa_path = PypsaAdapter().export_netcdf(case, tmp_path / "case9.pypsa.nc")
    pypsa_case = PypsaImportAdapter().import_case(pypsa_path)

    result = ValidationService().validate_pypsa_export(case, pypsa_path=pypsa_path)
    assert result.passed is True
    assert result.details["compared_buses"] == len(case.table("bus"))
    assert pypsa_case.source_format == "pypsa"
    assert len(pypsa_case.table("bus")) == len(case.table("bus"))


def test_balanced_pandapower_export_to_opendss(tmp_path: Path) -> None:
    source = REPO_ROOT / "src/powermodelconverter/data/samples/opendss/minimal_radial.dss"
    opendss_case = OpenDSSImportAdapter().import_case(source)
    pp_json = tmp_path / "minimal_radial.pandapower.json"
    PandapowerAdapter().export_json(opendss_case, pp_json)
    case = PandapowerImportAdapter().import_case(pp_json)

    export_path = OpenDSSExportAdapter().export_case(case, tmp_path / "minimal_radial_export.dss")
    reference = OpenDSSImportAdapter().solve_source_case(export_path)
    result = ValidationService().validate_pandapower_case_against_opendss(case, reference)
    assert result.passed is True
