from __future__ import annotations

from dataclasses import dataclass
import math
from pathlib import Path
from typing import Any

import pandas as pd
import pandapower as pp
from pandapower.plotting.geo import convert_geodata_to_geojson
import pypsa

from powermodelconverter.core.contracts import ImportAdapter
from powermodelconverter.core.pandapower_backend import PandapowerAdapter
from powermodelconverter.core.model import CanonicalCase


@dataclass(slots=True)
class PypsaResultSnapshot:
    slack_p_mw: float
    slack_q_mvar: float
    voltages: dict[str, complex]


class PypsaAdapter:
    format_name = "pypsa"

    def __init__(self) -> None:
        self._pandapower = PandapowerAdapter()

    def import_case(self, source: str | Path) -> CanonicalCase:
        path = Path(source)
        network = self.load_network(path)
        self._assert_supported_pypsa_network(network)
        net = self.pypsa_to_pandapower(network)
        return self._pandapower.to_canonical(
            net,
            case_id=path.stem if path.is_file() else path.name,
            source_format=self.format_name,
            metadata={
                "import_backend": f"pypsa:{self._detect_storage_format(path)}",
                "pypsa_snapshot": str(network.snapshots[0]),
            },
            source_path=path,
        )

    def load_network(self, source: str | Path) -> pypsa.Network:
        path = Path(source)
        network = pypsa.Network()
        if path.is_dir():
            network.import_from_csv_folder(path)
            return network
        suffix = path.suffix.lower()
        if suffix in {".nc", ".netcdf"}:
            network.import_from_netcdf(path)
            return network
        if suffix in {".h5", ".hdf5"}:
            network.import_from_hdf5(path)
            return network
        raise ValueError(f"Unsupported PyPSA source format for {path}. Use a .nc/.netcdf, .h5/.hdf5, or CSV folder.")

    def solve_source_case(self, source: str | Path) -> PypsaResultSnapshot:
        network = self.load_network(source)
        self._assert_supported_pypsa_network(network)
        return self._solve_network(network)

    def export_netcdf(self, case: CanonicalCase, destination: str | Path) -> Path:
        path = Path(destination)
        path.parent.mkdir(parents=True, exist_ok=True)
        network = self.to_net(case)
        network.export_to_netcdf(path)
        return path

    def to_net(self, case: CanonicalCase) -> pypsa.Network:
        if case.is_unbalanced:
            raise ValueError("PyPSA export is only supported for balanced cases.")
        net = self._pandapower.to_net(case)
        normalized = self._normalize_pandapower_names(net)
        network = pypsa.Network()
        network.import_from_pandapower_net(normalized, extra_line_data=True)
        self._assert_supported_pypsa_network(network)
        return network

    def pypsa_to_pandapower(self, network: pypsa.Network) -> Any:
        self._assert_supported_pypsa_network(network)
        net = pp.create_empty_network(
            sn_mva=float(getattr(network, "sn_mva", 1.0) or 1.0),
            f_hz=float(getattr(network, "f_hz", 50.0) or 50.0),
        )
        bus_lookup: dict[str, int] = {}

        for bus_name, row in network.buses.iterrows():
            idx = pp.create_bus(
                net,
                vn_kv=float(row.v_nom),
                name=str(bus_name),
                type=str(row.type) if "type" in row and pd.notna(row.type) else "b",
            )
            bus_lookup[str(bus_name)] = idx

        self._apply_bus_geodata_from_pypsa(network=network, net=net, bus_lookup=bus_lookup)

        for line_name, row in network.lines.iterrows():
            length_km = float(row.length) if "length" in row and float(row.length) > 0 else 1.0
            parallel = max(int(round(float(row.num_parallel))), 1) if "num_parallel" in row else 1
            c_nf_per_km = float(row.b) / (2 * math.pi * net.f_hz) * 1e9 / (length_km * parallel) if float(row.b) else 0.0
            g_us_per_km = float(row.g) * 1e6 / (length_km * parallel) if float(row.g) else 0.0
            max_i_ka_total = float(row.s_nom) / float(network.buses.loc[row.bus0, "v_nom"]) if float(row.s_nom) else 1.0
            max_i_ka = max_i_ka_total / parallel
            pp.create_line_from_parameters(
                net,
                from_bus=bus_lookup[str(row.bus0)],
                to_bus=bus_lookup[str(row.bus1)],
                length_km=length_km,
                r_ohm_per_km=float(row.r) * parallel / length_km,
                x_ohm_per_km=float(row.x) * parallel / length_km,
                c_nf_per_km=c_nf_per_km,
                g_us_per_km=g_us_per_km,
                max_i_ka=max_i_ka,
                parallel=parallel,
                name=str(line_name),
            )

        for trafo_name, row in network.transformers.iterrows():
            if not math.isclose(float(row.tap_ratio), 1.0, rel_tol=0.0, abs_tol=1e-9):
                raise ValueError(f"PyPSA transformer {trafo_name} uses tap_ratio={row.tap_ratio}, which is not supported yet.")
            z = math.hypot(float(row.r), float(row.x))
            y = math.hypot(float(row.g), float(row.b))
            sn_mva = float(row.s_nom)
            raw_tap_side = None if "tap_side" not in row or pd.isna(row.tap_side) else str(row.tap_side).strip().lower()
            tap_side = None
            if raw_tap_side in {"0", "hv", "high", "high_voltage"}:
                tap_side = "hv"
            elif raw_tap_side in {"1", "lv", "low", "low_voltage"}:
                tap_side = "lv"
            pp.create_transformer_from_parameters(
                net,
                hv_bus=bus_lookup[str(row.bus0)],
                lv_bus=bus_lookup[str(row.bus1)],
                sn_mva=sn_mva,
                vn_hv_kv=float(network.buses.loc[row.bus0, "v_nom"]),
                vn_lv_kv=float(network.buses.loc[row.bus1, "v_nom"]),
                vkr_percent=float(row.r) * 100.0,
                vk_percent=z * 100.0,
                pfe_kw=float(row.g) * sn_mva * 1000.0,
                i0_percent=y * 100.0,
                shift_degree=float(row.phase_shift) if "phase_shift" in row else 0.0,
                tap_side=tap_side,
                name=str(trafo_name),
            )

        for shunt_name, row in network.shunt_impedances.iterrows():
            vn_kv = float(network.buses.loc[row.bus, "v_nom"])
            pp.create_shunt(
                net,
                bus=bus_lookup[str(row.bus)],
                p_mw=float(row.g) * vn_kv * vn_kv,
                q_mvar=float(row.b) * vn_kv * vn_kv,
                vn_kv=vn_kv,
                name=str(shunt_name),
            )

        for load_name, row in network.loads.iterrows():
            pp.create_load(
                net,
                bus=bus_lookup[str(row.bus)],
                p_mw=float(row.p_set),
                q_mvar=float(row.q_set),
                name=str(load_name),
            )

        for generator_name, row in network.generators.iterrows():
            control = str(row.control)
            vm_pu = float(row.vm_pu) if "vm_pu" in row and pd.notna(row.vm_pu) else float(
                network.buses.loc[row.bus, "v_mag_pu_set"]
            )
            if control == "Slack":
                pp.create_ext_grid(
                    net,
                    bus=bus_lookup[str(row.bus)],
                    vm_pu=vm_pu,
                    name=str(generator_name),
                )
            elif control == "PV":
                pp.create_gen(
                    net,
                    bus=bus_lookup[str(row.bus)],
                    p_mw=float(row.p_set),
                    vm_pu=vm_pu,
                    name=str(generator_name),
                )
            else:
                pp.create_sgen(
                    net,
                    bus=bus_lookup[str(row.bus)],
                    p_mw=float(row.p_set),
                    q_mvar=float(row.q_set),
                    name=str(generator_name),
                )

        return net

    def _apply_bus_geodata_from_pypsa(
        self,
        *,
        network: pypsa.Network,
        net: Any,
        bus_lookup: dict[str, int],
    ) -> None:
        if "x" not in network.buses.columns or "y" not in network.buses.columns:
            return

        geodata_rows: list[dict[str, float | int]] = []
        for bus_name in network.buses.index:
            x_raw = network.buses.at[bus_name, "x"]
            y_raw = network.buses.at[bus_name, "y"]
            if pd.isna(x_raw) or pd.isna(y_raw):
                continue
            try:
                x_val = float(x_raw)
                y_val = float(y_raw)
            except Exception:
                continue
            if not math.isfinite(x_val) or not math.isfinite(y_val):
                continue
            geodata_rows.append(
                {
                    "bus": int(bus_lookup[str(bus_name)]),
                    "x": x_val,
                    "y": y_val,
                }
            )

        if not geodata_rows:
            return

        geodata_df = pd.DataFrame(geodata_rows).drop_duplicates(subset=["bus"]).set_index("bus")
        net.bus_geodata = geodata_df.reindex(net.bus.index)

        try:
            convert_geodata_to_geojson(net)
        except Exception:
            pass

    def run_power_flow(self, case: CanonicalCase) -> PypsaResultSnapshot:
        network = self.to_net(case)
        return self._solve_network(network)

    def solve_network_file(self, path: Path) -> PypsaResultSnapshot:
        network = self.load_network(path)
        self._assert_supported_pypsa_network(network)
        return self._solve_network(network)

    def _solve_network(self, network: pypsa.Network) -> PypsaResultSnapshot:
        self._assert_supported_pypsa_network(network)
        snapshot = network.snapshots[0]
        network.pf()
        voltages: dict[str, complex] = {}
        for bus_name in network.buses.index:
            vm = float(network.buses_t.v_mag_pu.loc[snapshot, bus_name])
            va = float(network.buses_t.v_ang.loc[snapshot, bus_name])
            voltages[str(bus_name)] = complex(vm * math.cos(va), vm * math.sin(va))

        slack_names = network.generators.index[network.generators.control == "Slack"]
        slack_p = 0.0
        slack_q = 0.0
        for generator_name in slack_names:
            slack_p += float(network.generators_t.p.loc[snapshot, generator_name])
            slack_q += float(network.generators_t.q.loc[snapshot, generator_name])

        return PypsaResultSnapshot(slack_p_mw=slack_p, slack_q_mvar=slack_q, voltages=voltages)

    def _assert_supported_pypsa_network(self, network: pypsa.Network) -> None:
        unsupported = []
        for component_name in ("links", "stores", "storage_units"):
            component = getattr(network, component_name, None)
            if component is not None and not component.empty:
                unsupported.append(component_name)
        if unsupported:
            names = ", ".join(unsupported)
            raise ValueError(
                f"PyPSA support currently targets AC transmission-style networks only. Unsupported populated components: {names}."
            )

    def _normalize_pandapower_names(self, net: Any) -> Any:
        normalized = pp.from_json_string(pp.to_json(net, filename=None))
        for table_name in ("bus", "line", "trafo", "load", "gen", "sgen", "ext_grid", "shunt"):
            table = getattr(normalized, table_name, None)
            if table is None or table.empty or "name" not in table.columns:
                continue
            names = []
            counts: dict[str, int] = {}
            for idx, value in table["name"].items():
                if pd.isna(value) or not str(value).strip():
                    base = f"{table_name}_{idx}"
                else:
                    base = str(value).strip()
                counts[base] = counts.get(base, 0) + 1
                suffix = counts[base]
                names.append(base if suffix == 1 else f"{base}_{suffix}")
            table["name"] = names
        return normalized

    def _detect_storage_format(self, path: Path) -> str:
        if path.is_dir():
            return "csv_folder"
        suffix = path.suffix.lower()
        if suffix in {".nc", ".netcdf"}:
            return "netcdf"
        if suffix in {".h5", ".hdf5"}:
            return "hdf5"
        return suffix.lstrip(".") or "unknown"


class PypsaImportAdapter(ImportAdapter):
    source_format = "pypsa"

    def __init__(self) -> None:
        self._pypsa = PypsaAdapter()

    def import_case(self, source: str | Path, **kwargs: Any) -> CanonicalCase:
        return self._pypsa.import_case(source)


def import_pypsa(path: str | Path, **kwargs: Any) -> CanonicalCase:
    return PypsaImportAdapter().import_case(path, **kwargs)


def solve_pypsa_reference(path: str | Path) -> PypsaResultSnapshot:
    return PypsaAdapter().solve_source_case(path)


__all__ = [
    "PypsaAdapter",
    "PypsaImportAdapter",
    "PypsaResultSnapshot",
    "import_pypsa",
    "solve_pypsa_reference",
]
