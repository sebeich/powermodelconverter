#!/usr/bin/env python3
from __future__ import annotations

import json
import math
from html import escape
from dataclasses import asdict, dataclass
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

import pandapower as pp

from powermodelconverter.adapters.matpower_adapter import MatpowerImportAdapter
from powermodelconverter.adapters.opendss_adapter import OpenDSSImportAdapter
from powermodelconverter.adapters.pandapower_adapter import PandapowerAdapter
from powermodelconverter.adapters.pandapower_import_adapter import PandapowerImportAdapter
from powermodelconverter.adapters.powermodels_distribution_adapter import PowerModelsDistributionAdapter
from powermodelconverter.core.capabilities import capability_rows
from powermodelconverter.validation.powerflow import ValidationResult, ValidationService


REPO_ROOT = Path(__file__).resolve().parents[1]
DOCS_DIR = REPO_ROOT / "docs"
JSON_REPORT = DOCS_DIR / "validation_report.json"
MARKDOWN_REPORT = DOCS_DIR / "validation_report.md"
HTML_REPORT = DOCS_DIR / "validation_report.html"


@dataclass(slots=True)
class RouteRecord:
    case_id: str
    source_tool: str
    export_tool: str
    model_type: str
    status: str
    slack_delta_mva: float | None
    max_voltage_delta_pu: float | None
    compared_points: int | None
    notes: str


def main() -> None:
    DOCS_DIR.mkdir(parents=True, exist_ok=True)

    validator = ValidationService()
    pandapower = PandapowerAdapter()
    records: list[RouteRecord] = []

    with TemporaryDirectory(prefix="pmc_validation_") as tmp:
        tmpdir = Path(tmp)

        # MATPOWER balanced sample
        matpower_case = MatpowerImportAdapter().import_case(
            REPO_ROOT / "src/powermodelconverter/data/samples/matpower/case9.m"
        )
        records.append(
            validation_record(
                case_id=matpower_case.case_id,
                source_tool="matpower",
                export_tool="pandapower",
                model_type="balanced",
                result=self_validate_balanced(matpower_case, pandapower, validator),
                notes="Official MATPOWER case9 imported through pandapower converter.",
            )
        )
        records.append(
            validation_record(
                case_id=matpower_case.case_id,
                source_tool="matpower",
                export_tool="powermodels",
                model_type="balanced",
                result=validate_powermodels(matpower_case, pandapower, validator, tmpdir),
                notes="Balanced export validated with Julia PowerModels AC power flow.",
            )
        )

        # OpenDSS subset sample
        opendss_source = REPO_ROOT / "src/powermodelconverter/data/samples/opendss/minimal_radial.dss"
        opendss_adapter = OpenDSSImportAdapter()
        opendss_reference = opendss_adapter.solve_source_case(opendss_source)
        opendss_case = opendss_adapter.import_case(opendss_source)
        opendss_pp_result = validator.validate_opendss_roundtrip(opendss_case, opendss_reference)
        records.append(
            validation_record(
                case_id=opendss_case.case_id,
                source_tool="opendss",
                export_tool="pandapower",
                model_type="balanced_subset",
                result=opendss_pp_result,
                notes="Validated against OpenDSS source slack power and bus voltages for the currently supported subset.",
            )
        )
        records.append(
            validation_record(
                case_id=opendss_case.case_id,
                source_tool="opendss",
                export_tool="powermodels",
                model_type="balanced_subset",
                result=validate_powermodels(opendss_case, pandapower, validator, tmpdir),
                notes="Balanced-equivalent export validated with Julia PowerModels AC power flow.",
            )
        )

        # OpenDSS unbalanced starter feeder
        opendss_unbalanced_source = (
            REPO_ROOT / "src/powermodelconverter/data/samples/opendss/minimal_unbalanced_3ph.dss"
        )
        opendss_unbalanced_reference = opendss_adapter.solve_source_case(opendss_unbalanced_source)
        opendss_unbalanced_case = opendss_adapter.import_case(opendss_unbalanced_source)
        records.append(
            validation_record(
                case_id=opendss_unbalanced_case.case_id,
                source_tool="opendss",
                export_tool="pandapower",
                model_type="unbalanced_3ph",
                result=validator.validate_opendss_unbalanced_roundtrip(
                    opendss_unbalanced_case,
                    opendss_unbalanced_reference,
                ),
                notes="Unbalanced OpenDSS starter feeder validated against source node voltages with pandapower runpp_3ph.",
            )
        )
        records.append(
            validation_record(
                case_id=opendss_unbalanced_case.case_id,
                source_tool="opendss",
                export_tool="powermodelsdistribution",
                model_type="unbalanced_3ph",
                result=validate_powermodelsdistribution(
                    opendss_unbalanced_case,
                    opendss_unbalanced_reference,
                    validator,
                    tmpdir,
                ),
                notes="Unbalanced OpenDSS starter feeder exported to PowerModelsDistribution and validated against source node voltages.",
            )
        )

        # Pandapower balanced sample generated from validated case9 export
        balanced_pp_path = tmpdir / "case9_from_matpower.pandapower.json"
        pandapower.export_json(matpower_case, balanced_pp_path)
        pp_balanced_case = PandapowerImportAdapter().import_case(balanced_pp_path)
        records.append(
            validation_record(
                case_id=pp_balanced_case.case_id,
                source_tool="pandapower",
                export_tool="pandapower",
                model_type="balanced",
                result=self_validate_balanced(pp_balanced_case, pandapower, validator),
                notes="Pandapower JSON re-import generated from the validated MATPOWER case9 route.",
            )
        )
        records.append(
            validation_record(
                case_id=pp_balanced_case.case_id,
                source_tool="pandapower",
                export_tool="powermodels",
                model_type="balanced",
                result=validate_powermodels(pp_balanced_case, pandapower, validator, tmpdir),
                notes="Balanced pandapower JSON export validated with Julia PowerModels AC power flow.",
            )
        )

        # Pandapower native unbalanced sample
        pp_unbalanced_case = PandapowerImportAdapter().import_case(
            REPO_ROOT / "src/powermodelconverter/data/samples/pandapower/ieee_european_lv_asymmetric.json"
        )
        records.append(
            validation_record(
                case_id=pp_unbalanced_case.case_id,
                source_tool="pandapower",
                export_tool="pandapower",
                model_type="unbalanced_3ph",
                result=validator.validate_pandapower_unbalanced_roundtrip(pp_unbalanced_case),
                notes="Native pandapower 3-phase roundtrip validated with runpp_3ph.",
            )
        )
        records.append(
            validation_record(
                case_id=pp_unbalanced_case.case_id,
                source_tool="pandapower",
                export_tool="powermodelsdistribution",
                model_type="unbalanced_3ph",
                result=validate_powermodelsdistribution_from_pandapower(
                    pp_unbalanced_case,
                    pandapower,
                    validator,
                    tmpdir,
                ),
                notes="Native pandapower 3-phase model exported to PowerModelsDistribution and validated against runpp_3ph node voltages.",
            )
        )

        payload = {
            "capabilities": capability_rows(),
            "records": [asdict(record) for record in records],
        }
        JSON_REPORT.write_text(json.dumps(payload, indent=2))
        MARKDOWN_REPORT.write_text(render_markdown(records))
        HTML_REPORT.write_text(render_html(payload, records))

    print(f"Wrote {JSON_REPORT}")
    print(f"Wrote {MARKDOWN_REPORT}")
    print(f"Wrote {HTML_REPORT}")


def self_validate_balanced(case: Any, pandapower: PandapowerAdapter, validator: ValidationService) -> ValidationResult:
    reference = pandapower.run_power_flow(case)
    reference_slack_p = float(reference.res_ext_grid.p_mw.sum())
    reference_slack_q = float(reference.res_ext_grid.q_mvar.sum())
    reference_voltages = {
        validator._bus_key(reference, int(idx)): complex(
            float(row.vm_pu) * math.cos(math.radians(float(row.va_degree))),
            float(row.vm_pu) * math.sin(math.radians(float(row.va_degree))),
        )
        for idx, row in reference.res_bus.iterrows()
    }
    return validator.validate_against_pandapower(
        case,
        reference_slack_p_mw=reference_slack_p,
        reference_slack_q_mvar=reference_slack_q,
        reference_voltages=reference_voltages,
    )


def validate_powermodels(
    case: Any,
    pandapower: PandapowerAdapter,
    validator: ValidationService,
    tmpdir: Path,
) -> ValidationResult:
    destination = tmpdir / f"{case.case_id}.powermodels.json"
    path = pandapower.export_powermodels_json(case, destination)
    return validator.validate_powermodels_export(
        case,
        powermodels_json=path,
        julia_binary=str(Path.home() / ".julia/juliaup/julia-1.12.3+0.x64.linux.gnu/bin/julia"),
        julia_script=REPO_ROOT / "src/powermodelconverter/julia/run_powermodels_pf.jl",
        julia_depot=REPO_ROOT / ".julia_depot",
    )


def validate_powermodelsdistribution(
    case: Any,
    reference: Any,
    validator: ValidationService,
    tmpdir: Path,
) -> ValidationResult:
    destination = tmpdir / f"{case.case_id}_powermodelsdistribution.dss"
    path = PowerModelsDistributionAdapter().export_input(case, destination)
    return validator.validate_powermodelsdistribution_export(
        case,
        pmd_input_path=path,
        julia_binary=str(Path.home() / ".julia/juliaup/julia-1.12.3+0.x64.linux.gnu/bin/julia"),
        julia_script=REPO_ROOT / "src/powermodelconverter/julia_pmd/run_powermodels_distribution_pf.jl",
        julia_depot=REPO_ROOT / ".julia_depot",
        julia_project=REPO_ROOT / "src/powermodelconverter/julia_pmd",
        reference_slack_p_mw=reference.slack_p_mw,
        reference_slack_q_mvar=reference.slack_q_mvar,
        reference_node_voltages=reference.node_voltages,
    )


def validate_powermodelsdistribution_from_pandapower(
    case: Any,
    pandapower: PandapowerAdapter,
    validator: ValidationService,
    tmpdir: Path,
) -> ValidationResult:
    destination = tmpdir / f"{case.case_id}_powermodelsdistribution.dss"
    path = PowerModelsDistributionAdapter().export_input(case, destination)
    reference_net = pandapower.run_power_flow_3ph(case)
    slack = validator._extract_3ph_slack(reference_net)
    return validator.validate_powermodelsdistribution_export(
        case,
        pmd_input_path=path,
        julia_binary=str(Path.home() / ".julia/juliaup/julia-1.12.3+0.x64.linux.gnu/bin/julia"),
        julia_script=REPO_ROOT / "src/powermodelconverter/julia_pmd/run_powermodels_distribution_pf.jl",
        julia_depot=REPO_ROOT / ".julia_depot",
        julia_project=REPO_ROOT / "src/powermodelconverter/julia_pmd",
        reference_slack_p_mw=sum(slack[key] for key in ("p_a_mw", "p_b_mw", "p_c_mw")),
        reference_slack_q_mvar=sum(slack[key] for key in ("q_a_mvar", "q_b_mvar", "q_c_mvar")),
        reference_node_voltages=validator._extract_3ph_node_voltages(reference_net),
    )


def validation_record(
    *,
    case_id: str,
    source_tool: str,
    export_tool: str,
    model_type: str,
    result: ValidationResult,
    notes: str,
) -> RouteRecord:
    compared_points = result.details.get("compared_buses", result.details.get("compared_nodes"))
    return RouteRecord(
        case_id=case_id,
        source_tool=source_tool,
        export_tool=export_tool,
        model_type=model_type,
        status="validated" if result.passed else "failed",
        slack_delta_mva=result.slack_delta_mva,
        max_voltage_delta_pu=result.max_voltage_delta_pu,
        compared_points=int(compared_points) if compared_points is not None else None,
        notes=notes,
    )


def unsupported_record(
    *,
    case_id: str,
    source_tool: str,
    export_tool: str,
    model_type: str,
    notes: str,
) -> RouteRecord:
    return RouteRecord(
        case_id=case_id,
        source_tool=source_tool,
        export_tool=export_tool,
        model_type=model_type,
        status="not_validated",
        slack_delta_mva=None,
        max_voltage_delta_pu=None,
        compared_points=None,
        notes=notes,
    )


def render_markdown(records: list[RouteRecord]) -> str:
    lines = [
        "# Validation Report",
        "",
        "This file is generated by `scripts/generate_validation_report.py`.",
        "",
        "## Validated Routes",
        "",
        "| Source | Export | Case | Model | Status | Slack Delta (MVA) | Max Voltage Delta (pu) | Compared Points | Notes |",
        "| --- | --- | --- | --- | --- | ---: | ---: | ---: | --- |",
    ]
    for record in records:
        lines.append(
            "| "
            f"{record.source_tool} | {record.export_tool} | {record.case_id} | {record.model_type} | {record.status} | "
            f"{format_metric(record.slack_delta_mva)} | {format_metric(record.max_voltage_delta_pu)} | "
            f"{record.compared_points if record.compared_points is not None else '-'} | {record.notes} |"
        )
    return "\n".join(lines) + "\n"


def format_metric(value: float | None) -> str:
    if value is None:
        return "-"
    return f"{value:.12g}"


def render_html(payload: dict[str, Any], records: list[RouteRecord]) -> str:
    validated = [record for record in records if record.status == "validated"]
    pending = [record for record in records if record.status != "validated"]
    source_tools = sorted({record.source_tool for record in records})
    export_tools = sorted({record.export_tool for record in records})
    three_phase_validated = [record for record in validated if record.model_type == "unbalanced_3ph"]
    strongest_route = min(
        (
            record
            for record in validated
            if record.max_voltage_delta_pu is not None
        ),
        key=lambda record: (record.max_voltage_delta_pu or 0.0, record.slack_delta_mva or 0.0),
        default=None,
    )
    weakest_route = max(
        (
            record
            for record in validated
            if record.max_voltage_delta_pu is not None
        ),
        key=lambda record: (record.max_voltage_delta_pu or 0.0, record.slack_delta_mva or 0.0),
        default=None,
    )
    unbalanced_routes = [record for record in records if "unbalanced" in record.model_type]

    precision_rows = []
    for record in validated:
        slack_score = score_precision(record.slack_delta_mva)
        voltage_score = score_precision(record.max_voltage_delta_pu)
        precision_rows.append(
            f"""
            <tr>
              <td>{escape(record.source_tool)}</td>
              <td>{escape(record.export_tool)}</td>
              <td>{escape(record.case_id)}</td>
              <td>{escape(display_model_type(record.model_type))}</td>
              <td>{format_metric(record.slack_delta_mva)}</td>
              <td>{bar_svg(slack_score, '#1d4ed8')}</td>
              <td>{format_metric(record.max_voltage_delta_pu)}</td>
              <td>{bar_svg(voltage_score, '#059669')}</td>
              <td>{record.compared_points if record.compared_points is not None else '-'}</td>
            </tr>
            """
        )

    matrix_rows = []
    for source_tool in source_tools:
        cells = []
        for export_tool in export_tools:
            matches = [record for record in records if record.source_tool == source_tool and record.export_tool == export_tool]
            if not matches:
                cells.append('<td class="cell cell-empty">-</td>')
                continue
            best = "validated" if any(record.status == "validated" for record in matches) else "not_validated"
            label = "validated" if best == "validated" else "not validated"
            title = " | ".join(f"{record.case_id}: {record.notes}" for record in matches)
            cells.append(
                f'<td class="cell cell-{best}" title="{escape(title)}">{escape(label)}</td>'
            )
        matrix_rows.append(f"<tr><th>{escape(source_tool)}</th>{''.join(cells)}</tr>")

    capability_cards = []
    for row in payload["capabilities"]:
        capability_cards.append(
            f"""
            <article class="cap-card">
              <h3>{escape(str(row['tool']))}</h3>
              <p>{escape(str(row['notes']))}</p>
              <ul>
                <li>Balanced: import={yn(row['balanced_import'])}, export={yn(row['balanced_export'])}, validation={yn(row['balanced_validation'])}</li>
                <li>Unbalanced: import={yn(row['unbalanced_import'])}, export={yn(row['unbalanced_export'])}, validation={yn(row['unbalanced_validation'])}</li>
              </ul>
            </article>
            """
        )

    detail_rows = []
    for record in records:
        detail_rows.append(
            f"""
            <tr>
              <td>{escape(record.source_tool)}</td>
              <td>{escape(record.export_tool)}</td>
              <td>{escape(record.case_id)}</td>
              <td>{escape(display_model_type(record.model_type))}</td>
              <td><span class="pill pill-{record.status}">{escape(record.status)}</span></td>
              <td>{format_metric(record.slack_delta_mva)}</td>
              <td>{format_metric(record.max_voltage_delta_pu)}</td>
              <td>{record.compared_points if record.compared_points is not None else '-'}</td>
              <td>{escape(record.notes)}</td>
            </tr>
            """
        )

    best_voltage = min((record.max_voltage_delta_pu for record in validated if record.max_voltage_delta_pu is not None), default=None)
    worst_voltage = max((record.max_voltage_delta_pu for record in validated if record.max_voltage_delta_pu is not None), default=None)

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>PowerModelConverter Validation Dashboard</title>
  <style>
    :root {{
      --bg: #f3efe7;
      --panel: #fffdf9;
      --ink: #1d1b18;
      --muted: #6a645c;
      --line: #d8d0c4;
      --green: #1f7a5c;
      --amber: #a0671a;
      --red: #9f2d2d;
      --blue: #1d4ed8;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: "IBM Plex Sans", "Segoe UI", sans-serif;
      color: var(--ink);
      background:
        radial-gradient(circle at top left, #fff6d8 0, transparent 28%),
        radial-gradient(circle at bottom right, #dff5ee 0, transparent 26%),
        var(--bg);
    }}
    .wrap {{ max-width: 1200px; margin: 0 auto; padding: 32px 20px 48px; }}
    .hero {{
      background: linear-gradient(135deg, #1b4d3e, #233a72);
      color: white;
      border-radius: 24px;
      padding: 28px;
      box-shadow: 0 18px 50px rgba(20, 30, 40, 0.18);
    }}
    .hero h1 {{ margin: 0 0 10px; font-size: 2rem; }}
    .hero p {{ margin: 0; max-width: 850px; color: rgba(255,255,255,.88); }}
    .grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
      gap: 16px;
      margin-top: 22px;
    }}
    .card, .section {{
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 20px;
      box-shadow: 0 8px 26px rgba(40, 35, 25, 0.06);
    }}
    .card {{ padding: 18px; }}
    .card h2 {{ margin: 0 0 6px; font-size: .95rem; color: var(--muted); }}
    .card .value {{ font-size: 2rem; font-weight: 700; }}
    .card .sub {{ margin-top: 6px; color: var(--muted); font-size: .92rem; }}
    .section {{ margin-top: 22px; padding: 20px; }}
    .section h2 {{ margin: 0 0 14px; font-size: 1.2rem; }}
    .analysis {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
      gap: 14px;
    }}
    .analysis-card {{
      background: linear-gradient(180deg, #fffaf0, #fffdf9);
      border: 1px solid var(--line);
      border-radius: 18px;
      padding: 16px;
    }}
    .analysis-card h3 {{ margin: 0 0 8px; font-size: 1rem; }}
    .analysis-card p {{ margin: 0; color: var(--muted); line-height: 1.45; }}
    .matrix {{
      width: 100%;
      border-collapse: collapse;
      overflow: hidden;
    }}
    .matrix th, .matrix td, .table th, .table td {{
      border-bottom: 1px solid var(--line);
      padding: 10px 12px;
      text-align: left;
      vertical-align: top;
    }}
    .matrix th {{ background: #f8f3ea; }}
    .cell {{ text-align: center; font-weight: 600; text-transform: uppercase; font-size: .78rem; }}
    .cell-validated {{ background: rgba(31, 122, 92, 0.12); color: var(--green); }}
    .cell-not_validated {{ background: rgba(160, 103, 26, 0.12); color: var(--amber); }}
    .cell-empty {{ color: var(--muted); }}
    .caps {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(260px, 1fr));
      gap: 14px;
    }}
    .cap-card {{
      background: #fdfaf3;
      border: 1px solid var(--line);
      border-radius: 18px;
      padding: 16px;
    }}
    .cap-card h3 {{ margin: 0 0 8px; }}
    .cap-card p {{ margin: 0 0 10px; color: var(--muted); }}
    .cap-card ul {{ margin: 0; padding-left: 18px; }}
    .table {{ width: 100%; border-collapse: collapse; }}
    .pill {{
      display: inline-block;
      border-radius: 999px;
      padding: 4px 10px;
      font-size: .78rem;
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: .04em;
    }}
    .pill-validated {{ background: rgba(31,122,92,.12); color: var(--green); }}
    .pill-not_validated {{ background: rgba(160,103,26,.12); color: var(--amber); }}
    .footer {{ margin-top: 18px; color: var(--muted); font-size: .92rem; }}
    svg {{ display: block; }}
  </style>
</head>
<body>
  <div class="wrap">
    <section class="hero">
      <h1>PowerModelConverter Validation Dashboard</h1>
      <p>Generated from the live route inventory. This page shows which conversions are validated, which are still pending, and the measured precision for each route in the current repository state.</p>
    </section>

    <div class="grid">
      <article class="card">
        <h2>Validated Routes</h2>
        <div class="value">{len(validated)}</div>
        <div class="sub">Routes with successful measured validation</div>
      </article>
      <article class="card">
        <h2>Pending Routes</h2>
        <div class="value">{len(pending)}</div>
        <div class="sub">Tracked explicitly, but not yet signed off</div>
      </article>
      <article class="card">
        <h2>Best Voltage Delta</h2>
        <div class="value">{format_metric(best_voltage)}</div>
        <div class="sub">Smallest measured full-voltage mismatch</div>
      </article>
      <article class="card">
        <h2>Worst Validated Voltage Delta</h2>
        <div class="value">{format_metric(worst_voltage)}</div>
        <div class="sub">Largest mismatch among currently validated routes</div>
      </article>
      <article class="card">
        <h2>Validated 3-Phase Routes</h2>
        <div class="value">{len(three_phase_validated)}</div>
        <div class="sub">Routes using explicit unbalanced three-phase node-voltage checks</div>
      </article>
    </div>

    <section class="section">
      <h2>Analysis</h2>
      <div class="analysis">
        <article class="analysis-card">
          <h3>Validation Rule</h3>
          <p>Every signed-off route is checked by slack-power agreement first and full complex-voltage agreement second. Rows labeled 3-phase unbalanced are validated node by node across explicit phase voltages, not just balanced bus magnitudes. The current PMD-backed 3-phase routes use a documented 5e-3 pu voltage threshold.</p>
        </article>
        <article class="analysis-card">
          <h3>Strongest Current Route</h3>
          <p>{escape(describe_route(strongest_route))}</p>
        </article>
        <article class="analysis-card">
          <h3>Largest Current Mismatch</h3>
          <p>{escape(describe_route(weakest_route))}</p>
        </article>
        <article class="analysis-card">
          <h3>Unbalanced Coverage</h3>
          <p>{escape(describe_unbalanced(unbalanced_routes))}</p>
        </article>
      </div>
    </section>

    <section class="section">
      <h2>Conversion Matrix</h2>
      <table class="matrix">
        <thead>
          <tr><th>Source \\ Export</th>{''.join(f'<th>{escape(tool)}</th>' for tool in export_tools)}</tr>
        </thead>
        <tbody>
          {''.join(matrix_rows)}
        </tbody>
      </table>
    </section>

    <section class="section">
      <h2>Precision Plots</h2>
      <table class="table">
        <thead>
          <tr>
            <th>Source</th>
            <th>Export</th>
            <th>Case</th>
            <th>Model</th>
            <th>Slack Delta</th>
            <th>Slack Precision</th>
            <th>Voltage Delta</th>
            <th>Voltage Precision</th>
            <th>Compared Points</th>
          </tr>
        </thead>
        <tbody>
          {''.join(precision_rows)}
        </tbody>
      </table>
    </section>

    <section class="section">
      <h2>Tool Capabilities</h2>
      <div class="caps">
        {''.join(capability_cards)}
      </div>
    </section>

    <section class="section">
      <h2>Route Details</h2>
      <table class="table">
        <thead>
          <tr>
            <th>Source</th>
            <th>Export</th>
            <th>Case</th>
            <th>Model</th>
            <th>Status</th>
            <th>Slack Delta</th>
            <th>Voltage Delta</th>
            <th>Compared Points</th>
            <th>Notes</th>
          </tr>
        </thead>
        <tbody>
          {''.join(detail_rows)}
        </tbody>
      </table>
      <p class="footer">Generated by <code>scripts/generate_validation_report.py</code>.</p>
    </section>
  </div>
</body>
</html>
"""


def score_precision(value: float | None) -> float:
    if value is None:
        return 0.0
    if value == 0:
        return 1.0
    return min(1.0, max(0.0, -math.log10(value) / 12.0))


def bar_svg(score: float, color: str) -> str:
    width = max(2, int(round(score * 180)))
    return (
        '<svg width="190" height="14" viewBox="0 0 190 14" aria-hidden="true">'
        '<rect x="0" y="2" width="190" height="10" rx="5" fill="#ece5d8"></rect>'
        f'<rect x="0" y="2" width="{width}" height="10" rx="5" fill="{color}"></rect>'
        "</svg>"
    )


def yn(value: Any) -> str:
    return "yes" if value else "no"


def describe_route(record: RouteRecord | None) -> str:
    if record is None:
        return "No validated route with voltage metrics is available yet."
    return (
        f"{record.source_tool} -> {record.export_tool} on {record.case_id} "
        f"({record.model_type}) with slack delta {format_metric(record.slack_delta_mva)} MVA "
        f"and max voltage delta {format_metric(record.max_voltage_delta_pu)} pu."
    )


def describe_unbalanced(routes: list[RouteRecord]) -> str:
    validated = [record for record in routes if record.status == "validated"]
    pending = [record for record in routes if record.status != "validated"]
    if not routes:
        return "No unbalanced routes are tracked in the current report."
    return (
        f"{len(validated)} unbalanced route(s) are validated and {len(pending)} remain pending. "
        "Today that includes native pandapower 3-phase roundtrip validation and an "
        "OpenDSS starter feeder validated to both pandapower and PowerModelsDistribution."
    )


def display_model_type(model_type: str) -> str:
    mapping = {
        "balanced": "Balanced",
        "balanced_subset": "Balanced Subset",
        "unbalanced_3ph": "3-Phase Unbalanced",
        "balanced_or_mixed": "Native/Non-Exchange",
    }
    return mapping.get(model_type, model_type.replace("_", " ").title())


if __name__ == "__main__":
    main()
