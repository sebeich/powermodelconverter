#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pandapower as pp
from pandapower.plotting.geo import convert_geodata_to_geojson
from pandapower.plotting.simple_plot import simple_plot


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Import a pandapower JSON, run power flow, and export a PNG network plot."
    )
    parser.add_argument(
        "--input",
        required=True,
        help="Path to pandapower JSON file",
    )
    parser.add_argument(
        "--output-png",
        default=None,
        help="Output PNG path (default: <input_stem>.pf.png)",
    )
    parser.add_argument(
        "--output-pf-json",
        default=None,
        help="Optional path to export the post-powerflow pandapower JSON",
    )
    parser.add_argument(
        "--algorithm",
        default="nr",
        choices=["nr", "bfsw", "gs", "fdbx", "fdxb"],
        help="pandapower power flow algorithm",
    )
    parser.add_argument(
        "--init",
        default="auto",
        choices=["auto", "flat", "dc", "results"],
        help="Initialization method for power flow",
    )
    parser.add_argument(
        "--trafo-model",
        default="t",
        choices=["t", "pi"],
        help="Transformer equivalent model for runpp",
    )
    parser.add_argument(
        "--dpi",
        type=int,
        default=200,
        help="PNG export DPI",
    )
    return parser


def resolve_output_png(input_path: Path, output_png: str | None) -> Path:
    if output_png:
        return Path(output_png)
    return input_path.with_suffix(".pf.png")


def _extract_pypsa_coords(net: pp.pandapowerNet) -> pd.DataFrame | None:
    pypsa_details = net.get("pypsa_full_details", {})
    records = (
        pypsa_details.get("components", {})
        .get("buses", {})
        .get("records", [])
    )
    if not records:
        return None

    rows: list[dict[str, float]] = []
    bus_name_to_index = {str(row.name): int(row.Index) for row in net.bus.itertuples()}
    for record in records:
        name = str(record.get("_index", ""))
        if name not in bus_name_to_index:
            continue
        x_val = record.get("x")
        y_val = record.get("y")
        if x_val is None or y_val is None:
            continue
        try:
            x_float = float(x_val)
            y_float = float(y_val)
        except Exception:
            continue
        if not np.isfinite(x_float) or not np.isfinite(y_float):
            continue
        rows.append({"bus_idx": bus_name_to_index[name], "x": x_float, "y": y_float})

    if not rows:
        return None
    df = pd.DataFrame(rows).drop_duplicates(subset=["bus_idx"]).set_index("bus_idx")
    return df


def ensure_simple_plot_geodata(net: pp.pandapowerNet) -> bool:
    if "geo" in net.bus.columns and int(net.bus["geo"].notna().sum()) > 0:
        return True

    pypsa_coords = _extract_pypsa_coords(net)
    if pypsa_coords is None or len(pypsa_coords) == 0:
        return False

    net.bus_geodata = pypsa_coords.reindex(net.bus.index)
    convert_geodata_to_geojson(net)
    return "geo" in net.bus.columns and int(net.bus["geo"].notna().sum()) > 0


def render_network_png(net: pp.pandapowerNet, output_png: Path, dpi: int) -> None:
    if not ensure_simple_plot_geodata(net):
        raise RuntimeError(
            "No usable geodata found in pandapower JSON (and no embedded PyPSA x/y). "
            "simple_plot requires coordinates or optional 'igraph' for auto-layout."
        )

    fig = plt.figure(figsize=(14, 10))
    ax = fig.add_subplot(111)
    simple_plot(net, ax=ax, show_plot=False, respect_switches=True)
    ax.set_title(f"pandapower simple_plot: {net.name if hasattr(net, 'name') else 'network'}")
    fig.tight_layout()
    fig.savefig(str(output_png), dpi=dpi, bbox_inches="tight")
    plt.close(fig)


def run(args: argparse.Namespace) -> int:
    input_path = Path(args.input)
    if not input_path.exists():
        raise FileNotFoundError(f"Input JSON not found: {input_path}")

    output_png = resolve_output_png(input_path, args.output_png)
    output_png.parent.mkdir(parents=True, exist_ok=True)

    net = pp.from_json(str(input_path))

    pp.runpp(
        net,
        algorithm=args.algorithm,
        init=args.init,
        trafo_model=args.trafo_model,
        calculate_voltage_angles=True,
        numba=False,
    )

    render_network_png(net, output_png=output_png, dpi=args.dpi)

    if args.output_pf_json:
        output_pf = Path(args.output_pf_json)
        output_pf.parent.mkdir(parents=True, exist_ok=True)
        pp.to_json(net, str(output_pf))

    summary = {
        "input": str(input_path),
        "output_png": str(output_png),
        "converged": bool(getattr(net, "converged", False)),
        "bus_count": int(len(net.bus)),
        "line_count": int(len(net.line)),
        "trafo_count": int(len(net.trafo)),
    }
    if len(net.res_ext_grid):
        summary["slack_p_mw"] = float(net.res_ext_grid.p_mw.sum())
        summary["slack_q_mvar"] = float(net.res_ext_grid.q_mvar.sum())

    print(json.dumps(summary, indent=2))
    return 0


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    raise SystemExit(run(args))


if __name__ == "__main__":
    main()
