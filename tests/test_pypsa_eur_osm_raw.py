from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import subprocess
import sys

import pypsa


REPO_ROOT = Path(__file__).resolve().parents[1]


def _load_report_module():
    module_path = REPO_ROOT / "scripts/generate_validation_report.py"
    spec = importlib.util.spec_from_file_location("generate_validation_report", module_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _build_pypsa_eur_like_base_network() -> pypsa.Network:
    network = pypsa.Network()
    network.name = "pypsa_eur_osm_raw_de_60kv_smoke"
    network.sn_mva = 1.0
    network.set_snapshots(["now"])

    for name, v_nom, x, y in (
        ("DE_380", 380.0, 10.0, 52.0),
        ("DE_220", 220.0, 10.4, 52.1),
        ("DE_110", 110.0, 10.7, 52.2),
        ("DE_063", 63.0, 11.0, 52.3),
    ):
        network.add("Bus", name, v_nom=v_nom, x=x, y=y, carrier="AC")

    network.add(
        "Line",
        "L_380_220",
        bus0="DE_380",
        bus1="DE_220",
        r=0.02,
        x=0.18,
        b=0.0,
        g=0.0,
        s_nom=500.0,
        length=1.0,
    )
    network.add(
        "Line",
        "L_220_110",
        bus0="DE_220",
        bus1="DE_110",
        r=0.03,
        x=0.22,
        b=0.0,
        g=0.0,
        s_nom=300.0,
        length=1.0,
    )
    network.add(
        "Line",
        "L_110_063",
        bus0="DE_110",
        bus1="DE_063",
        r=0.04,
        x=0.25,
        b=0.0,
        g=0.0,
        s_nom=160.0,
        length=1.0,
    )

    return network


def test_pypsa_eur_osm_raw_base_topology_converts_validates_and_reports(tmp_path: Path) -> None:
    source = tmp_path / "pypsa_eur_osm_raw_de_60kv_smoke.nc"
    output = tmp_path / "pypsa_eur_osm_raw_de_60kv_smoke.pandapower.json"
    validation_output = tmp_path / "pypsa_eur_osm_raw_de_60kv_smoke.validation.json"
    html_plot = tmp_path / "pypsa_eur_osm_raw_de_60kv_smoke.pandapower.html"

    _build_pypsa_eur_like_base_network().export_to_netcdf(source)

    completed = subprocess.run(
        [
            sys.executable,
            str(REPO_ROOT / "scripts/import_pypsa_eur_to_pandapower.py"),
            "--source",
            str(source),
            "--output",
            str(output),
            "--validation-output",
            str(validation_output),
            "--synthetic-pf-mode",
            "toy-load-pv",
            "--slack-tolerance-mva",
            "0.01",
            "--no-embed-time-series",
        ],
        check=True,
        capture_output=True,
        text=True,
        cwd=REPO_ROOT,
    )

    payload = json.loads(completed.stdout)
    validation = json.loads(validation_output.read_text())
    pandapower_payload = json.loads(output.read_text())

    plot_completed = subprocess.run(
        [
            sys.executable,
            str(REPO_ROOT / "scripts/run_pandapower_json_pf_plot.py"),
            "--input",
            str(output),
            "--output-html",
            str(html_plot),
            "--output-png",
            str(tmp_path / "pypsa_eur_osm_raw_de_60kv_smoke.pandapower.png"),
            "--trafo-model",
            "t",
        ],
        check=True,
        capture_output=True,
        text=True,
        cwd=REPO_ROOT,
    )
    plot_payload = json.loads(plot_completed.stdout)

    report_module = _load_report_module()
    records = report_module.load_pypsa_eur_validation_records(validation_output)

    assert output.exists()
    assert validation_output.exists()
    assert payload["validation"]["passed"] is True
    assert validation["validation"]["passed"] is True
    assert validation["synthetic_pf_case"]["mode"] == "toy_load_pv_per_ac_island"
    assert validation["synthetic_pf_case"]["added_slacks"] == 1
    assert validation["synthetic_pf_case"]["added_loads"] == 1
    assert validation["synthetic_pf_case"]["added_pv_generators"] == 1
    assert pandapower_payload["_class"] == "pandapowerNet"
    assert html_plot.exists()
    assert html_plot.stat().st_size > 1000
    assert plot_payload["output_html"] == str(html_plot)
    assert plot_payload["converged"] is True
    assert "DE_380" in html_plot.read_text()
    assert any(
        record.case_id == "pypsa_eur_osm_raw_de_60kv_smoke" and record.status == "validated"
        for record in records
    )
