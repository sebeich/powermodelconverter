#!/usr/bin/env python3
from __future__ import annotations

"""Run a ding0 import smoke test through the pandapower hub.

This script imports a ding0 source, exports the direct canonical pandapower
artifact, exports a PyPSA NetCDF artifact from the same canonical case, validates
that PyPSA export against pandapower power flow, then re-imports the PyPSA file
and writes the PyPSA-to-pandapower JSON artifact.

Examples:
    python docs/examples/ding0_import_smoke.py --source ags:09462000
    python docs/examples/ding0_import_smoke.py --source mvgd:1234,5678
    python docs/examples/ding0_import_smoke.py --source validation_cases/ding0/example.pkl
"""

import argparse
from dataclasses import asdict
import json
from pathlib import Path
from typing import Any

from powermodelconverter.core.pandapower_backend import PandapowerAdapter
from powermodelconverter.importers.ding0 import Ding0ImportAdapter
from powermodelconverter.importers.pypsa import PypsaAdapter, PypsaImportAdapter
from powermodelconverter.validation.powerflow import ValidationResult
from powermodelconverter.validation.powerflow import ValidationService


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Import ding0 and exercise pandapower/PyPSA spokes.")
    parser.add_argument(
        "--source",
        default="ags:09462000",
        help="ding0 source: ags:09462000, mvgd:1234,5678, a .pkl path, or a single MV grid district ID.",
    )
    parser.add_argument(
        "--output-dir",
        default="artifacts/examples/ding0_smoke",
        help="Directory for generated pandapower JSON, PyPSA NetCDF, and validation JSON artifacts.",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    ding0 = Ding0ImportAdapter()
    pandapower = PandapowerAdapter()
    pypsa = PypsaAdapter()
    validator = ValidationService()

    case = ding0.import_case(args.source)
    stem = sanitize(case.case_id)

    direct_json = pandapower.export_json(case, output_dir / f"{stem}.pandapower.json")
    pypsa_nc = pypsa.export_netcdf(case, output_dir / f"{stem}.pypsa.nc")
    try:
        pypsa_validation = validator.validate_pypsa_export(case, pypsa_path=pypsa_nc)
        pypsa_validation_payload: dict[str, Any] = asdict(pypsa_validation)
    except Exception as exc:
        pypsa_validation = ValidationResult(
            case_id=case.case_id,
            passed=False,
            slack_delta_mva=float("nan"),
            max_voltage_delta_pu=float("nan"),
            details={"error": str(exc), "validation_status": "pending"},
            status="pending",
            source_tool="ding0",
            export_tool="pypsa",
            model_type="balanced",
            notes="PyPSA export was written, but power-flow validation did not converge for this pending ding0 route.",
        )
        pypsa_validation_payload = asdict(pypsa_validation)

    pypsa_case = PypsaImportAdapter().import_case(pypsa_nc)
    pypsa_to_pandapower_json = pandapower.export_json(
        pypsa_case,
        output_dir / f"{stem}.pypsa_to_pandapower.json",
    )

    summary = {
        "case_id": case.case_id,
        "source_format": case.source_format,
        "metadata": {
            "mv_grid_district_ids": case.metadata.get("mv_grid_district_ids"),
            "mv_grid_count": case.metadata.get("mv_grid_count"),
            "lv_grid_count": case.metadata.get("lv_grid_count"),
            "ding0_skipped_districts": case.metadata.get("ding0_skipped_districts"),
            "validation_status": case.metadata.get("validation_status"),
        },
        "direct_pandapower_json": str(direct_json),
        "pypsa_netcdf": str(pypsa_nc),
        "pypsa_to_pandapower_json": str(pypsa_to_pandapower_json),
        "pypsa_validation": pypsa_validation_payload,
        "counts": {
            "ding0_bus": len(case.table("bus")),
            "ding0_line": len(case.table("line")),
            "ding0_trafo": len(case.table("trafo")),
            "pypsa_to_pandapower_bus": len(pypsa_case.table("bus")),
            "pypsa_to_pandapower_line": len(pypsa_case.table("line")),
            "pypsa_to_pandapower_trafo": len(pypsa_case.table("trafo")),
        },
    }
    summary_path = output_dir / f"{stem}.summary.json"
    summary_path.write_text(json.dumps(summary, indent=2))
    print(json.dumps(summary, indent=2))


def sanitize(value: str) -> str:
    return "".join(char if char.isalnum() or char in {"-", "_"} else "_" for char in value)


if __name__ == "__main__":
    main()
