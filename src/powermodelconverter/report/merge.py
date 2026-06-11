from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .generator import build_report_payload, write_report_artifacts


def merge_partial_results(
    input_dir: str | Path,
    output_path: str | Path,
    existing_report: str | Path | None = None,
) -> dict[str, Any]:
    input_path = Path(input_dir)
    merged: dict[tuple[str, str, str], dict[str, Any]] = {}
    partial_keys: set[tuple[str, str, str]] = set()

    if existing_report is not None:
        existing_payload = json.loads(Path(existing_report).read_text())
        for record in existing_payload.get("records", []):
            key = (
                str(record.get("case_id", "")),
                str(record.get("source_tool", "")),
                str(record.get("export_tool", "")),
            )
            merged[key] = record

    for partial in sorted(input_path.glob("*.json")):
        payload = json.loads(partial.read_text())
        records = payload if isinstance(payload, list) else payload.get("records", [])
        for record in records:
            key = (
                str(record.get("case_id", "")),
                str(record.get("source_tool", "")),
                str(record.get("export_tool", "")),
            )
            if key in partial_keys and merged.get(key) != record:
                raise ValueError(f"Conflicting validation records encountered for key {key}.")
            partial_keys.add(key)
            merged[key] = record

    ordered_records = [merged[key] for key in sorted(merged)]
    output_payload = build_report_payload(ordered_records)
    write_report_artifacts(output_payload, output_path)
    return output_payload
