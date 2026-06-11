from __future__ import annotations

from dataclasses import asdict
import importlib.util
import json
from pathlib import Path
import sys
from typing import Any

from powermodelconverter.core.capabilities import capability_rows


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_REPORT_PATH = REPO_ROOT / "docs" / "validation_report.json"
_SCRIPT_PATH = REPO_ROOT / "scripts" / "generate_validation_report.py"


def _load_generator_module() -> Any:
    spec = importlib.util.spec_from_file_location("pmc_generate_validation_report", _SCRIPT_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load report generator script at {_SCRIPT_PATH}.")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def generate_full_report() -> dict[str, Any]:
    module = _load_generator_module()
    module.main()
    return load_report(DEFAULT_REPORT_PATH)


def build_report_payload(records: list[dict[str, Any]]) -> dict[str, Any]:
    module = _load_generator_module()
    route_records = [
        module.RouteRecord(
            case_id=str(record.get("case_id", "")),
            source_tool=str(record.get("source_tool", "")),
            export_tool=str(record.get("export_tool", "")),
            model_type=str(record.get("model_type", "")),
            status=str(record.get("status", "")),
            slack_delta_mva=record.get("slack_delta_mva"),
            max_voltage_delta_pu=record.get("max_voltage_delta_pu"),
            compared_points=record.get("compared_points"),
            notes=str(record.get("notes", "")),
        )
        for record in records
    ]
    summary = module.build_summary(route_records)
    return {
        "summary": asdict(summary),
        "capabilities": capability_rows(),
        "records": records,
    }


def write_report_artifacts(payload: dict[str, Any], output_path: str | Path = DEFAULT_REPORT_PATH) -> dict[str, Any]:
    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    output_file.write_text(json.dumps(payload, indent=2, sort_keys=False))

    module = _load_generator_module()
    route_records = [
        module.RouteRecord(
            case_id=str(record.get("case_id", "")),
            source_tool=str(record.get("source_tool", "")),
            export_tool=str(record.get("export_tool", "")),
            model_type=str(record.get("model_type", "")),
            status=str(record.get("status", "")),
            slack_delta_mva=record.get("slack_delta_mva"),
            max_voltage_delta_pu=record.get("max_voltage_delta_pu"),
            compared_points=record.get("compared_points"),
            notes=str(record.get("notes", "")),
        )
        for record in payload.get("records", [])
    ]
    summary = module.ValidationSummary(**payload.get("summary", {}))
    markdown_path = output_file.with_suffix(".md")
    html_path = output_file.with_suffix(".html")
    markdown_path.write_text(module.render_markdown(summary, route_records))
    html_path.write_text(module.render_html(payload, route_records))
    return payload


def load_report(path: str | Path = DEFAULT_REPORT_PATH) -> dict[str, Any]:
    report_path = Path(path)
    return json.loads(report_path.read_text())


def list_report_records(path: str | Path = DEFAULT_REPORT_PATH) -> list[dict[str, Any]]:
    payload = load_report(path)
    return list(payload.get("records", []))
