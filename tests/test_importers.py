import json
from pathlib import Path
import subprocess
import sys

import pandapower.networks as pn

from powermodelconverter.adapters.matpower_adapter import MatpowerImportAdapter
from powermodelconverter.adapters.matpower_adapter import MatpowerExportAdapter
from powermodelconverter.adapters.opendss_export_adapter import OpenDSSExportAdapter
from powermodelconverter.adapters.opendss_adapter import OpenDSSImportAdapter, _TransformerSpec
from powermodelconverter.adapters.pandapower_adapter import PandapowerAdapter
from powermodelconverter.adapters.pandapower_import_adapter import PandapowerImportAdapter
from powermodelconverter.adapters.pandapower_split_export_adapter import PandapowerSplitExportAdapter
from powermodelconverter.adapters.pypower_import_adapter import PypowerImportAdapter
from powermodelconverter.adapters.powersystems_adapter import PowerSystemsExportAdapter, PowerSystemsImportAdapter
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


def test_matpower_import_normalizes_newline_separated_matrix_rows(tmp_path: Path) -> None:
    source = tmp_path / "case_newline_rows.m"
    source.write_text(
        "\n".join(
            [
                "function mpc = case_newline_rows",
                "mpc.version = '2';",
                "mpc.baseMVA = 100;",
                "mpc.bus = [",
                "1 3 0 0 0 0 1 1 0 110 1 1.1 0.9",
                "2 1 10 5 0 0 1 1 0 110 1 1.1 0.9",
                "];",
                "mpc.gen = [",
                "1 10 0 100 -100 1 100 1 100 0 0 0 0 0 0 0 0 0 0 0 0",
                "];",
                "mpc.branch = [",
                "1 2 0.01 0.03 0 100 100 100 0 0 1 -360 360",
                "];",
                "",
            ]
        )
    )

    case = MatpowerImportAdapter().import_case(source)
    assert case.case_id == "case_newline_rows"
    assert len(case.table("bus")) == 2


def test_matpower_import_recovers_type3_slack_without_generator(tmp_path: Path) -> None:
    source = tmp_path / "case_missing_ext_grid.m"
    source.write_text(
        "\n".join(
            [
                "function mpc = case_missing_ext_grid",
                "mpc.version = '2';",
                "mpc.baseMVA = 100;",
                "mpc.bus = [",
                "1 3 0 0 0 0 1 1 0 110 1 1.1 0.9",
                "2 2 10 5 0 0 1 1 0 110 1 1.1 0.9",
                "];",
                "mpc.gen = [",
                "2 10 0 100 -100 1 100 1 100 0 0 0 0 0 0 0 0 0 0 0 0",
                "];",
                "mpc.branch = [",
                "1 2 0.01 0.03 0 100 100 100 0 0 1 -360 360",
                "];",
                "",
            ]
        )
    )

    case = MatpowerImportAdapter().import_case(source)
    solved = PandapowerAdapter().run_power_flow(case)
    assert len(solved.ext_grid) == 1


def test_matpower_export_reimports_text_case_file(tmp_path: Path) -> None:
    source_case = MatpowerImportAdapter().import_case(
        REPO_ROOT / "src/powermodelconverter/data/samples/matpower/case9.m"
    )

    exported = MatpowerExportAdapter().export_case(source_case, tmp_path / "case9_roundtrip.m")
    reimported = MatpowerImportAdapter().import_case(exported)

    assert exported.suffix == ".m"
    assert len(reimported.table("bus")) == len(source_case.table("bus"))


def test_powersystems_import_relabels_source_format() -> None:
    case = PowerSystemsImportAdapter().import_case(
        REPO_ROOT / "src/powermodelconverter/data/samples/matpower/case9.m"
    )
    assert case.case_id == "case9"
    assert case.source_format == "powersystems"
    assert len(case.table("bus")) == 9


def test_powersystems_export_writes_matpower_case_file(tmp_path: Path) -> None:
    source_case = MatpowerImportAdapter().import_case(
        REPO_ROOT / "src/powermodelconverter/data/samples/matpower/case9.m"
    )
    exported = PowerSystemsExportAdapter().export_case(source_case, tmp_path / "case9.powersystems.m")
    reimported = MatpowerImportAdapter().import_case(exported)

    assert exported.suffix == ".m"
    assert len(reimported.table("bus")) == len(source_case.table("bus"))

def test_pypower_import_from_python_case(tmp_path: Path) -> None:
    source = tmp_path / "case_py.py"
    source.write_text(
        "\n".join(
            [
                "from numpy import array",
                "",
                "def case_py():",
                "    return {",
                "        'version': '2',",
                "        'baseMVA': 100.0,",
                "        'bus': array([",
                "            [1, 3, 0, 0, 0, 0, 1, 1.0, 0, 110, 1, 1.1, 0.9],",
                "            [2, 1, 90, 30, 0, 0, 1, 1.0, 0, 110, 1, 1.1, 0.9],",
                "        ], dtype=float),",
                "        'gen': array([",
                "            [1, 90, 30, 300, -300, 1.0, 100, 1, 300, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],",
                "        ], dtype=float),",
                "        'branch': array([",
                "            [1, 2, 0.01, 0.03, 0.0, 250, 250, 250, 0, 0, 1, -360, 360],",
                "        ], dtype=float),",
                "    }",
                "",
            ]
        )
    )

    adapter = PypowerImportAdapter()
    case = adapter.import_case(source)
    snapshot = adapter.solve_source_case(source)
    result = ValidationService().validate_against_pandapower(
        case,
        reference_slack_p_mw=snapshot.slack_p_mw,
        reference_slack_q_mvar=snapshot.slack_q_mvar,
        reference_voltages=snapshot.voltages,
    )

    assert case.source_format == "pypower"
    assert len(case.table("bus")) == 2
    assert result.passed is True


def test_pypower_dtu_network_only_import_and_validation() -> None:
    source = REPO_ROOT / "input/DTU_ADN.py"

    adapter = PypowerImportAdapter()
    case = adapter.import_case(source)
    snapshot = adapter.solve_source_case(source)
    result = ValidationService().validate_against_pandapower(
        case,
        reference_slack_p_mw=snapshot.slack_p_mw,
        reference_slack_q_mvar=snapshot.slack_q_mvar,
        reference_voltages=snapshot.voltages,
    )

    assert case.source_format == "pypower"
    assert len(case.table("bus")) == 48
    assert len(case.table("trafo")) > 0
    assert result.passed is True


def test_pypower_dtu7k_connected_network_default_and_explicit_subnet() -> None:
    adapter = PypowerImportAdapter()
    validator = ValidationService()

    default_case = adapter.import_case(REPO_ROOT / "input/DTU7K.py")
    default_snapshot = adapter.solve_source_case(REPO_ROOT / "input/DTU7K.py")
    subnet_26_case = adapter.import_case(f"{REPO_ROOT / 'input/DTU7K.py'}::26")
    subnet_27_case = adapter.import_case(f"{REPO_ROOT / 'input/DTU7K.py'}::27")
    default_result = validator.validate_against_pandapower(
        default_case,
        reference_slack_p_mw=default_snapshot.slack_p_mw,
        reference_slack_q_mvar=default_snapshot.slack_q_mvar,
        reference_voltages=default_snapshot.voltages,
    )

    assert default_case.case_id == "DTU7K_26"
    assert default_case.metadata["pypower_network_id"] == 26
    assert default_result.passed is True
    assert len(default_case.table("bus")) == len(subnet_26_case.table("bus"))
    assert len(default_case.table("trafo")) == len(subnet_26_case.table("trafo"))
    assert subnet_27_case.metadata["pypower_network_id"] == 27
    assert len(subnet_27_case.table("bus")) != 0


def test_opendss_import_and_validation() -> None:
    source = REPO_ROOT / "src/powermodelconverter/data/samples/opendss/minimal_radial.dss"
    adapter = OpenDSSImportAdapter()
    reference = adapter.solve_source_case(source)
    case = adapter.import_case(source)
    result = ValidationService().validate_opendss_roundtrip(case, reference)
    assert result.passed is True
    assert result.details["compared_buses"] == len(reference.voltages)


def test_opendss_import_handles_case_mismatched_redirects(tmp_path: Path) -> None:
    feeder_dir = tmp_path / "case_mismatch"
    feeder_dir.mkdir()
    (feeder_dir / "LineCodes.DSS").write_text(
        "\n".join(
            [
                "New Linecode.test nphases=3 R1=0.01 X1=0.02 R0=0.03 X0=0.04 C1=0 C0=0 units=km",
                "",
            ]
        )
    )
    (feeder_dir / "Master.dss").write_text(
        "\n".join(
            [
                "Clear",
                "New Circuit.case_mismatch basekv=12.47 bus1=source pu=1.0 R1=0 X1=0.0001 R0=0 X0=0.0001",
                "Redirect linecodes.dss",
                "New Line.l1 Bus1=source.1.2.3 Bus2=loadbus.1.2.3 LineCode=test Length=1",
                "New Load.ld1 Bus1=loadbus.1.2.3 phases=3 conn=wye kv=12.47 kW=100 kvar=50",
                "Solve",
                "",
            ]
        )
    )

    case = OpenDSSImportAdapter().import_case(feeder_dir / "Master.dss")
    assert len(case.table("bus")) == 2


def test_pandapower_split_export_validates_minimal_unbalanced_opendss(tmp_path: Path) -> None:
    source = REPO_ROOT / "src/powermodelconverter/data/samples/opendss/minimal_unbalanced_3ph.dss"
    source_case = OpenDSSImportAdapter().import_case(source)
    reference = OpenDSSImportAdapter().solve_source_case(source)

    exported = PandapowerSplitExportAdapter().export_case(
        source_case,
        tmp_path / "minimal_unbalanced_3ph.pandapower-split.json",
    )
    split_case = PandapowerImportAdapter().import_case(exported)
    result = ValidationService().validate_pandapower_split_against_opendss(split_case, reference)

    assert result.passed is True
    assert result.details["mode"] == "phase_split"


def test_opendss_regulator_bank_specs_are_aggregated_for_pandapower() -> None:
    adapter = OpenDSSImportAdapter()
    specs = [
        _TransformerSpec(
            name="reg4a",
            buses=("160", "160r"),
            sn_mva=2.0,
            vn_hv_kv=2.402,
            vn_lv_kv=2.402,
            vk_percent=0.01,
            vkr_percent=0.00001,
            tap_pos=10.0,
            tap_neutral=0.0,
            tap_min=-16.0,
            tap_max=16.0,
            tap_step_percent=0.625,
            phase_count=1,
            is_regulator=True,
        ),
        _TransformerSpec(
            name="reg4b",
            buses=("160", "160r"),
            sn_mva=2.0,
            vn_hv_kv=2.402,
            vn_lv_kv=2.402,
            vk_percent=0.01,
            vkr_percent=0.00001,
            tap_pos=4.0,
            tap_neutral=0.0,
            tap_min=-16.0,
            tap_max=16.0,
            tap_step_percent=0.625,
            phase_count=1,
            is_regulator=True,
        ),
        _TransformerSpec(
            name="reg4c",
            buses=("160", "160r"),
            sn_mva=2.0,
            vn_hv_kv=2.402,
            vn_lv_kv=2.402,
            vk_percent=0.01,
            vkr_percent=0.00001,
            tap_pos=6.0,
            tap_neutral=0.0,
            tap_min=-16.0,
            tap_max=16.0,
            tap_step_percent=0.625,
            phase_count=1,
            is_regulator=True,
        ),
    ]

    aggregated = adapter._aggregate_regulator_specs(specs)

    assert len(aggregated) == 1
    assert aggregated[0].name == "reg4"
    assert aggregated[0].sn_mva == 6.0
    assert aggregated[0].tap_pos == 20.0 / 3.0
    assert aggregated[0].vk_percent == 0.3
    assert aggregated[0].vkr_percent == 0.003


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


def test_cli_precheck_reports_supported_single_target_route() -> None:
    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "powermodelconverter.cli.main",
            "precheck",
            "--source",
            str(REPO_ROOT / "src/powermodelconverter/data/samples/matpower/case9.m"),
            "--target-format",
            "pypsa",
        ],
        check=True,
        capture_output=True,
        text=True,
        cwd=REPO_ROOT,
    )
    payload = json.loads(completed.stdout)
    assert payload["supported"] is True
    assert payload["source_format"] == "matpower"
    assert payload["target_format"] == "pypsa"
    assert payload["component_counts"]["bus"] == 9


def test_cli_translate_exports_only_requested_target(tmp_path: Path) -> None:
    source = tmp_path / "case9.m"
    source.write_text((REPO_ROOT / "src/powermodelconverter/data/samples/matpower/case9.m").read_text())
    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "powermodelconverter.cli.main",
            "translate",
            "--source",
            str(source),
            "--target-format",
            "pypsa",
        ],
        check=True,
        capture_output=True,
        text=True,
        cwd=REPO_ROOT,
    )
    payload = json.loads(completed.stdout)
    expected_output = tmp_path / "case9.pypsa.nc"
    assert expected_output.exists() is True
    assert payload["source_format"] == "matpower"
    assert payload["target_format"] == "pypsa"
    assert Path(payload["target_export"]) == expected_output
    assert payload["target_validation"]["passed"] is True


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
