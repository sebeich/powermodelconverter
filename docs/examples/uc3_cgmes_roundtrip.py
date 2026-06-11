#!/usr/bin/env python3
from __future__ import annotations

import argparse
import math
from pathlib import Path

import pandapower as pp

from _common import default_output_dir
from _common import ensure_dir
from _common import write_json

from powermodelconverter.core.model import CanonicalCase
from powermodelconverter.core.pandapower_backend import PandapowerAdapter
from powermodelconverter.exporters.cgmes import CGMESExportAdapter
from powermodelconverter.importers.cgmes import CGMESImportAdapter
from powermodelconverter.validation.engine import ValidationService


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Working hub-and-spoke example: CGMES import and roundtrip validation."
    )
    parser.add_argument(
        "--input",
        default="validation_cases/native/cgmes",
        help="CGMES directory or ZIP file.",
    )
    parser.add_argument(
        "--output-dir",
        default=str(default_output_dir("uc3")),
        help="Directory for exported artifacts and JSON summaries.",
    )
    return parser


def validate_case_against_reference(reference_case, candidate_case) -> object:
    validator = ValidationService()
    reference = PandapowerAdapter().run_power_flow(reference_case)
    slack_p, slack_q = validator._extract_balanced_slack(reference)
    reference_voltages = {
        validator._bus_key(reference, int(idx)): complex(
            float(row.vm_pu) * math.cos(math.radians(float(row.va_degree))),
            float(row.vm_pu) * math.sin(math.radians(float(row.va_degree))),
        )
        for idx, row in reference.res_bus.iterrows()
    }
    return validator.validate_against_pandapower(
        candidate_case,
        reference_slack_p_mw=slack_p,
        reference_slack_q_mvar=slack_q,
        reference_voltages=reference_voltages,
    )


def build_smoke_case() -> CanonicalCase:
    net = pp.create_empty_network(sn_mva=100.0)
    slack_bus = pp.create_bus(net, vn_kv=110.0, name="SlackBus")
    load_bus = pp.create_bus(net, vn_kv=110.0, name="LoadBus")
    pp.create_ext_grid(
        net,
        slack_bus,
        vm_pu=1.0,
        va_degree=0.0,
        max_p_mw=500.0,
        min_p_mw=0.0,
        max_q_mvar=500.0,
        min_q_mvar=-500.0,
    )
    pp.create_line_from_parameters(
        net,
        slack_bus,
        load_bus,
        length_km=10.0,
        r_ohm_per_km=0.05,
        x_ohm_per_km=0.2,
        c_nf_per_km=10.0,
        max_i_ka=1.0,
        r0_ohm_per_km=0.15,
        x0_ohm_per_km=0.6,
        c0_nf_per_km=5.0,
        g_us_per_km=0.0,
        g0_us_per_km=0.0,
        name="L1",
    )
    pp.create_load(net, load_bus, p_mw=50.0, q_mvar=10.0, name="LD1")
    return CanonicalCase.from_pandapower(
        case_id="cgmes_smoke",
        source_format="pandapower",
        net=net,
    )


def main() -> None:
    args = build_parser().parse_args()
    output_dir = ensure_dir(Path(args.output_dir))

    imported = CGMESImportAdapter().import_case(args.input)
    import_validation = validate_case_against_reference(imported, imported)

    supported_subset = build_smoke_case()
    cgmes_export_path = CGMESExportAdapter().export_case(
        supported_subset,
        output_dir / f"{supported_subset.case_id}.roundtrip.zip",
    )
    roundtripped = CGMESImportAdapter().import_case(cgmes_export_path)
    roundtrip_validation = validate_case_against_reference(supported_subset, roundtripped)

    summary_path = write_json(
        output_dir / "summary.json",
        {
            "use_case": "uc3",
            "script": "docs/examples/uc3_cgmes_roundtrip.py",
            "input": Path(args.input),
            "case_id": imported.case_id,
            "artifacts": {
                "cgmes_roundtrip_zip": cgmes_export_path,
            },
            "validations": {
                "cgmes_import": import_validation,
                "cgmes_roundtrip": roundtrip_validation,
            },
            "notes": {
                "roundtrip_mode": "uses a branch-supported smoke network for deterministic CGMES export/import validation",
            },
        },
    )
    print(summary_path)


if __name__ == "__main__":
    main()
