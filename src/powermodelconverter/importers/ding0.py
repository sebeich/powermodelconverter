from __future__ import annotations

import logging
import math
import pickle
import re
import inspect
from decimal import Decimal
from pathlib import Path
from typing import Any

import pandas as pd
import pandapower as pp

from powermodelconverter.core.contracts import ImportAdapter
from powermodelconverter.core.model import CanonicalCase

LOGGER = logging.getLogger(__name__)


def _load_ding0() -> tuple[Any, Any, str]:
    try:
        import ding0
        from ding0.core import NetworkDing0
        from ding0.tools.results import load_nd_from_pickle

        return NetworkDing0, load_nd_from_pickle, str(getattr(ding0, "__version__", "unknown"))
    except ImportError as exc:
        raise ImportError(
            "ding0 is not installed. Install the optional support extra with "
            "pip install -e '.[ding0]' and then install ding0 without its stale "
            "pandas pin: pip install --no-deps 'ding0==0.2.1'"
        ) from exc


class Ding0ImportAdapter(ImportAdapter):
    source_format = "ding0"

    def import_case(self, source: str | Path | dict[str, Any] | int, **kwargs: Any) -> CanonicalCase:
        if isinstance(source, dict):
            if "ags" in source:
                return self.import_from_region_ags(str(source["ags"]), failsafe=bool(kwargs.get("failsafe", True)))
            if "mv_grid_district_ids" in source:
                ids = [int(value) for value in source["mv_grid_district_ids"]]
                return self.import_from_grid_districts(ids, failsafe=bool(kwargs.get("failsafe", False)))
            raise ValueError("ding0 source dict must contain 'ags' or 'mv_grid_district_ids'.")

        if isinstance(source, int):
            return self.import_from_grid_districts([source], failsafe=bool(kwargs.get("failsafe", False)))

        source_text = str(source)
        if source_text.startswith("ags:"):
            return self.import_from_region_ags(source_text.split(":", 1)[1], failsafe=bool(kwargs.get("failsafe", True)))
        if source_text.startswith("mvgd:"):
            ids = [int(part.strip()) for part in source_text.split(":", 1)[1].split(",") if part.strip()]
            return self.import_from_grid_districts(ids, failsafe=bool(kwargs.get("failsafe", False)))

        path = Path(source_text)
        if path.exists() and path.suffix.lower() in {".pkl", ".pickle"}:
            return self.import_from_pickle(path)
        if source_text.isdigit() and not re.fullmatch(r"\d{8}", source_text):
            return self.import_from_grid_districts([int(source_text)], failsafe=bool(kwargs.get("failsafe", False)))
        if re.fullmatch(r"\d{8}", source_text):
            return self.import_from_region_ags(source_text, failsafe=bool(kwargs.get("failsafe", True)))
        raise ValueError(
            "Unsupported ding0 source. Use a .pkl path, ags:09462000, mvgd:1234,5678, "
            "an integer MV grid district ID, or {'ags': ...}/ {'mv_grid_district_ids': ...}."
        )

    def import_from_pickle(self, pkl_path: Path) -> CanonicalCase:
        _, load_nd_from_pickle, version = _load_ding0()
        loaded = load_nd_from_pickle(str(pkl_path))
        if isinstance(loaded, CanonicalCase):
            return loaded.copy_with(source_path=pkl_path)
        return self._canonicalize(loaded, ding0_version=version, source_path=pkl_path)

    def import_from_grid_districts(
        self,
        mv_grid_district_ids: list[int],
        *,
        failsafe: bool = False,
    ) -> CanonicalCase:
        NetworkDing0, _, version = _load_ding0()
        ids = [int(value) for value in mv_grid_district_ids]
        if not ids:
            raise ValueError("At least one MV grid district ID is required.")
        nd = NetworkDing0(name=f"mvgd_{'_'.join(str(value) for value in ids)}")
        try:
            self._run_ding0(nd, ids, failsafe=failsafe)
        except Exception as exc:
            if not _is_oedialect_advanced_api_failure(exc):
                raise
            LOGGER.warning(
                "ding0/oedialect advanced SQL endpoint is unavailable; "
                "falling back to OEP rows API hub-and-spoke import for MV grid districts %s.",
                ids,
            )
            return self._canonicalize_oep_hub_spoke(ids, ding0_version=version, fallback_error=str(exc))
        return self._canonicalize(nd, ding0_version=version, requested_mv_grid_district_ids=ids)

    def import_from_region_ags(
        self,
        ags: str,
        *,
        failsafe: bool = True,
    ) -> CanonicalCase:
        from powermodelconverter.importers import _ding0_region

        session, engine = _ding0_region.open_oedb_session()
        try:
            polygon = _ding0_region.get_polygon_for_ags(engine, ags)
            ids = _ding0_region.find_mv_grid_districts(engine, polygon)
        finally:
            close = getattr(session, "close", None)
            if callable(close):
                close()
        if not ids:
            raise RuntimeError(f"No ding0 MV grid districts intersect AGS {ags}.")
        return self.import_from_grid_districts(ids, failsafe=failsafe)

    def import_from_ding0_network(self, nd: Any) -> CanonicalCase:
        try:
            import ding0

            version = str(getattr(ding0, "__version__", "unknown"))
        except ImportError:
            version = "unknown"
        return self._canonicalize(nd, ding0_version=version)

    def _run_ding0(self, nd: Any, mv_grid_district_ids: list[int], *, failsafe: bool) -> None:
        run = getattr(nd, "run_ding0", None)
        if not callable(run):
            raise TypeError("ding0 NetworkDing0 object does not expose run_ding0().")
        signature = inspect.signature(run)
        params = signature.parameters
        kwargs: dict[str, Any] = {}
        if "failsafe" in params:
            kwargs["failsafe"] = failsafe
        elif failsafe:
            LOGGER.warning("Installed ding0 run_ding0() does not support failsafe=True; continuing without it.")
        if "mv_grid_district_no" in params:
            kwargs["mv_grid_district_no"] = mv_grid_district_ids
        elif "mv_grid_districts_no" in params:
            kwargs["mv_grid_districts_no"] = mv_grid_district_ids
        elif "mv_grid_district_ids" in params:
            kwargs["mv_grid_district_ids"] = mv_grid_district_ids

        if "session" in params:
            from sqlalchemy.orm import sessionmaker
            from powermodelconverter.importers import _ding0_region

            credential_session, engine = _ding0_region.open_oedb_session()
            close_credential_session = getattr(credential_session, "close", None)
            session = sessionmaker(bind=engine)()
            try:
                run(session, **kwargs)
            finally:
                close = getattr(session, "close", None)
                if callable(close):
                    close()
                if callable(close_credential_session):
                    close_credential_session()
            return
        run(**kwargs)

    def _canonicalize(
        self,
        nd: Any,
        *,
        ding0_version: str,
        requested_mv_grid_district_ids: list[int] | None = None,
        source_path: Path | None = None,
    ) -> CanonicalCase:
        net = pp.create_empty_network(sn_mva=1.0, f_hz=50.0)
        bus_lookup: dict[int, int] = {}
        node_index: dict[int, Any] = {}
        mv_lv_station_mapping: dict[str, str] = {}

        mv_grid_districts = list(_call_iter(nd, "mv_grid_districts"))
        for mvgd in mv_grid_districts:
            for node in _call_iter(mvgd, "graph_nodes_sorted"):
                self._ensure_node_bus(net, node, bus_lookup, node_index)
            for edge in _call_iter(mvgd, "graph_edges"):
                self._add_edge(net, edge, bus_lookup, node_index)
            for load in _collect_ding0_elements(mvgd, ("loads", "mv_loads")):
                self._add_load_or_sgen(net, load, bus_lookup, node_index, kind="load")
            for generator in _collect_ding0_elements(mvgd, ("generators", "mv_generators")):
                self._add_load_or_sgen(net, generator, bus_lookup, node_index, kind="sgen")

            for lv_grid in _iter_lv_grids(mvgd):
                station = _first_non_none(getattr(lv_grid, attr, None) for attr in ("station", "lv_station", "station_node"))
                if station is not None:
                    self._ensure_node_bus(net, station, bus_lookup, node_index)
                    mv_bus = self._find_mv_station_bus(net, station, bus_lookup, node_index)
                    lv_bus = bus_lookup[id(station)]
                    if mv_bus != lv_bus:
                        self._add_transformer(net, station, hv_bus=mv_bus, lv_bus=lv_bus)
                        mv_lv_station_mapping[str(net.bus.at[lv_bus, "name"])] = str(net.bus.at[mv_bus, "name"])
                for node in _call_iter(lv_grid, "graph_nodes_sorted"):
                    self._ensure_node_bus(net, node, bus_lookup, node_index)
                for edge in _call_iter(lv_grid, "graph_edges"):
                    self._add_edge(net, edge, bus_lookup, node_index)
                for load in _collect_ding0_elements(lv_grid, ("loads", "lv_loads")):
                    self._add_load_or_sgen(net, load, bus_lookup, node_index, kind="load")
                for generator in _collect_ding0_elements(lv_grid, ("generators", "lv_generators")):
                    self._add_load_or_sgen(net, generator, bus_lookup, node_index, kind="sgen")

        self._ensure_slack(net)
        self._assert_supported(net)

        mv_ids = _mv_grid_district_ids(mv_grid_districts) or list(requested_mv_grid_district_ids or [])
        skipped = _skipped_districts(nd, requested_mv_grid_district_ids or mv_ids)
        if skipped:
            LOGGER.warning("ding0 skipped MV grid districts during failsafe run: %s", skipped)

        metadata = {
            "ding0_run_id": str(getattr(nd, "name", "") or "network"),
            "mv_grid_district_ids": [int(value) for value in mv_ids],
            "mv_grid_count": len(mv_grid_districts),
            "lv_grid_count": sum(1 for mvgd in mv_grid_districts for _ in _iter_lv_grids(mvgd)),
            "geo_crs": "EPSG:4326",
            "source": "ding0+oedb",
            "ding0_version": ding0_version,
            "mv_lv_station_mapping": mv_lv_station_mapping,
            "ding0_node_index": {str(bus): _stable_id(node) for bus, node in node_index.items()},
            "ding0_skipped_districts": skipped,
            "validation_status": "pending",
        }
        case_id = f"ding0_{metadata['ding0_run_id']}"
        return CanonicalCase.from_pandapower(
            case_id=case_id,
            source_format=self.source_format,
            net=net,
            metadata=metadata,
            source_path=source_path,
        )

    def _ensure_node_bus(
        self,
        net: Any,
        node: Any,
        bus_lookup: dict[int, int],
        node_index: dict[int, Any],
        *,
        voltage_kv: float | None = None,
    ) -> int:
        key = id(node)
        if key in bus_lookup:
            return bus_lookup[key]
        vn_kv = voltage_kv or _node_voltage_kv(node)
        name = _name_for("ding0_bus", node)
        bus_type = "n"
        class_name = type(node).__name__.lower()
        if "station" in class_name:
            bus_type = "b"
        idx = pp.create_bus(net, vn_kv=vn_kv, name=name, type=bus_type)
        bus_lookup[key] = int(idx)
        node_index[int(idx)] = node
        _add_bus_geodata(net, int(idx), node)
        if "mvstation" in class_name or "mv_station" in class_name:
            pp.create_ext_grid(net, bus=idx, name=_name_for("ding0_ext_grid", node), vm_pu=1.0)
        return int(idx)

    def _add_edge(self, net: Any, edge: Any, bus_lookup: dict[int, int], node_index: dict[int, Any]) -> None:
        from_node, to_node, branch = _split_edge(edge)
        if from_node is None or to_node is None:
            return
        from_bus = self._ensure_node_bus(net, from_node, bus_lookup, node_index)
        to_bus = self._ensure_node_bus(net, to_node, bus_lookup, node_index)
        if from_bus == to_bus:
            return

        params = _line_parameters(branch)
        line_idx = pp.create_line_from_parameters(
            net,
            from_bus=from_bus,
            to_bus=to_bus,
            length_km=params["length_km"],
            r_ohm_per_km=params["r_ohm_per_km"],
            x_ohm_per_km=params["x_ohm_per_km"],
            c_nf_per_km=params["c_nf_per_km"],
            max_i_ka=params["max_i_ka"],
            parallel=1,
            name=_name_for("ding0_line", branch),
        )
        if "std_type" not in net.line.columns:
            net.line["std_type"] = pd.NA
        net.line.at[line_idx, "std_type"] = _branch_type_name(branch)
        _add_line_geodata(net, int(line_idx), from_node, to_node)

    def _add_transformer(self, net: Any, station: Any, *, hv_bus: int, lv_bus: int) -> None:
        if hv_bus == lv_bus:
            return
        params = _trafo_parameters(station, net, hv_bus, lv_bus)
        pp.create_transformer_from_parameters(
            net,
            hv_bus=hv_bus,
            lv_bus=lv_bus,
            name=_name_for("ding0_trafo", station),
            **params,
        )

    def _add_load_or_sgen(
        self,
        net: Any,
        element: Any,
        bus_lookup: dict[int, int],
        node_index: dict[int, Any],
        *,
        kind: str,
    ) -> None:
        node = _attached_node(element)
        if node is None:
            return
        bus = self._ensure_node_bus(net, node, bus_lookup, node_index)
        p_mw = _active_power_mw(element)
        q_mvar = _reactive_power_mvar(element, p_mw)
        name = _name_for(f"ding0_{kind}", element)
        if kind == "load":
            pp.create_load(net, bus=bus, p_mw=max(p_mw, 0.0), q_mvar=q_mvar, name=name)
        else:
            pp.create_sgen(net, bus=bus, p_mw=max(p_mw, 0.0), q_mvar=q_mvar, name=name, type=_element_type(element))

    def _find_mv_station_bus(
        self,
        net: Any,
        station: Any,
        bus_lookup: dict[int, int],
        node_index: dict[int, Any],
    ) -> int:
        for attr in ("mv_grid", "grid", "mv_station", "station_node"):
            candidate = getattr(station, attr, None)
            if candidate is not None and id(candidate) in bus_lookup:
                return bus_lookup[id(candidate)]
        for bus_idx, row in net.bus.iterrows():
            if float(row.vn_kv) >= 1.0 and "mvstation" in str(row.get("name", "")).lower():
                return int(bus_idx)
        return self._ensure_node_bus(net, station, bus_lookup, node_index, voltage_kv=max(_node_voltage_kv(station), 10.0))

    def _ensure_slack(self, net: Any) -> None:
        if len(net.ext_grid):
            return
        if len(net.bus) == 0:
            return
        mv_candidates = [int(idx) for idx, row in net.bus.iterrows() if float(row.vn_kv) >= 1.0]
        bus = mv_candidates[0] if mv_candidates else int(net.bus.index[0])
        pp.create_ext_grid(net, bus=bus, name="ding0_ext_grid_fallback", vm_pu=1.0)

    def _assert_supported(self, net: Any) -> None:
        if len(net.bus) <= 0:
            raise ValueError("ding0 import produced no pandapower buses.")
        if len(net.ext_grid) < 1:
            raise ValueError("ding0 import produced no slack/ext_grid; expected an HV/MV station.")
        unsupported = [name for name in ("link", "store", "storage_unit") if hasattr(net, name) and len(getattr(net, name))]
        if unsupported:
            raise ValueError(f"ding0 import produced unsupported PyPSA-incompatible elements: {unsupported}")

    def _canonicalize_oep_hub_spoke(
        self,
        mv_grid_district_ids: list[int],
        *,
        ding0_version: str,
        fallback_error: str,
    ) -> CanonicalCase:
        from powermodelconverter.importers import _ding0_region

        net = pp.create_empty_network(sn_mva=1.0, f_hz=50.0)
        mv_lv_station_mapping: dict[str, str] = {}
        node_index: dict[str, str] = {}
        mv_buses: dict[int, int] = {}
        mvlv_lv_buses: dict[int, int] = {}

        for subst_id in mv_grid_district_ids:
            where = [f"subst_id={int(subst_id)}", "version=v0.4.5"]
            hvmv_rows = _ding0_region.fetch_oep_rows("grid", "ego_dp_hvmv_substation", where=where, limit=10)
            if not hvmv_rows:
                continue
            hvmv = hvmv_rows[0]
            lon, lat = _row_lon_lat(hvmv, geom_srid="EPSG:3035")
            hv_bus = pp.create_bus(net, vn_kv=110.0, name=f"ding0_oep_hv_bus_hvmv_{subst_id}", type="b")
            mv_bus = pp.create_bus(net, vn_kv=20.0, name=f"ding0_oep_mv_bus_hvmv_{subst_id}", type="b")
            _set_bus_geodata(net, int(hv_bus), lon, lat)
            _set_bus_geodata(net, int(mv_bus), lon, lat)
            pp.create_ext_grid(net, bus=hv_bus, name=f"ding0_oep_ext_grid_hvmv_{subst_id}", vm_pu=1.0)
            pp.create_transformer_from_parameters(
                net,
                hv_bus=hv_bus,
                lv_bus=mv_bus,
                sn_mva=40.0,
                vn_hv_kv=110.0,
                vn_lv_kv=20.0,
                vk_percent=12.0,
                vkr_percent=0.5,
                pfe_kw=0.0,
                i0_percent=0.0,
                name=f"ding0_oep_trafo_hvmv_{subst_id}",
            )
            mv_buses[int(subst_id)] = int(mv_bus)
            node_index[str(mv_bus)] = str(subst_id)

            lv_station_rows = _ding0_region.fetch_oep_rows("grid", "ego_dp_mvlv_substation", where=where, limit=50_000)
            lv_grid_rows = _ding0_region.fetch_oep_rows("grid", "ego_dp_lv_griddistrict", where=where, limit=50_000)
            lv_load_by_station = _sum_lv_peak_load_by_station(lv_grid_rows)

            for station in lv_station_rows:
                station_id = _int_value(station.get("mvlv_subst_id"))
                if station_id is None:
                    continue
                s_lon, s_lat = _row_lon_lat(station, geom_srid="EPSG:3035")
                station_mv_bus = pp.create_bus(
                    net,
                    vn_kv=20.0,
                    name=f"ding0_oep_mv_bus_mvlv_{station_id}",
                    type="b",
                )
                station_lv_bus = pp.create_bus(
                    net,
                    vn_kv=0.4,
                    name=f"ding0_oep_lv_bus_mvlv_{station_id}",
                    type="b",
                )
                _set_bus_geodata(net, int(station_mv_bus), s_lon, s_lat)
                _set_bus_geodata(net, int(station_lv_bus), s_lon, s_lat)
                line_idx = pp.create_line_from_parameters(
                    net,
                    from_bus=mv_bus,
                    to_bus=station_mv_bus,
                    length_km=max(_distance_km((lon, lat), (s_lon, s_lat)), 0.001),
                    r_ohm_per_km=0.32,
                    x_ohm_per_km=0.12,
                    c_nf_per_km=0.0,
                    max_i_ka=0.4,
                    parallel=1,
                    name=f"ding0_oep_line_hvmv_{subst_id}_mvlv_{station_id}",
                )
                _set_line_geodata(net, int(line_idx), (lon, lat), (s_lon, s_lat))
                pp.create_transformer_from_parameters(
                    net,
                    hv_bus=station_mv_bus,
                    lv_bus=station_lv_bus,
                    sn_mva=0.63,
                    vn_hv_kv=20.0,
                    vn_lv_kv=0.4,
                    vk_percent=6.0,
                    vkr_percent=1.2,
                    pfe_kw=0.0,
                    i0_percent=0.0,
                    name=f"ding0_oep_trafo_mvlv_{station_id}",
                )
                p_mw = lv_load_by_station.get(station_id, 0.0)
                if p_mw > 0.0:
                    pp.create_load(
                        net,
                        bus=station_lv_bus,
                        p_mw=p_mw,
                        q_mvar=_q_from_cosphi(p_mw, 0.95),
                        name=f"ding0_oep_load_mvlv_{station_id}",
                    )
                mvlv_lv_buses[station_id] = int(station_lv_bus)
                mv_lv_station_mapping[str(net.bus.at[station_lv_bus, "name"])] = str(net.bus.at[station_mv_bus, "name"])
                node_index[str(station_lv_bus)] = str(station_id)

            self._add_oep_generators(net, int(subst_id), mv_bus, mvlv_lv_buses)

        self._assert_supported(net)
        metadata = {
            "ding0_run_id": f"oep_hub_spoke_{'_'.join(str(value) for value in mv_grid_district_ids)}",
            "mv_grid_district_ids": [int(value) for value in mv_grid_district_ids],
            "mv_grid_count": len(mv_buses),
            "lv_grid_count": len(mvlv_lv_buses),
            "geo_crs": "EPSG:4326",
            "source": "ding0+oedb",
            "ding0_version": ding0_version,
            "mv_lv_station_mapping": mv_lv_station_mapping,
            "ding0_node_index": node_index,
            "ding0_skipped_districts": [int(value) for value in mv_grid_district_ids if int(value) not in mv_buses],
            "validation_status": "pending",
            "oedb_access_mode": "oep_rows_api_hub_spoke_fallback",
            "oedialect_error": fallback_error,
        }
        return CanonicalCase.from_pandapower(
            case_id=f"ding0_{metadata['ding0_run_id']}",
            source_format=self.source_format,
            net=net,
            metadata=metadata,
        )

    def _add_oep_generators(self, net: Any, subst_id: int, mv_bus: int, mvlv_lv_buses: dict[int, int]) -> None:
        from powermodelconverter.importers import _ding0_region

        where = [f"subst_id={int(subst_id)}", "version=v0.4.5"]
        rows = _ding0_region.fetch_oep_rows("supply", "ego_dp_res_powerplant", where=where, limit=50_000)
        rows.extend(_ding0_region.fetch_oep_rows("supply", "ego_dp_conv_powerplant", where=where, limit=50_000))
        for row in rows:
            p_mw = _capacity_mw(row)
            if p_mw <= 0.0:
                continue
            station_id = _int_value(row.get("mvlv_subst_id"))
            bus = mvlv_lv_buses.get(station_id, mv_bus) if station_id is not None else mv_bus
            gen_id = row.get("id", "unknown")
            gen_type = str(row.get("generation_type") or row.get("fuel") or "unknown")
            subtype = str(row.get("generation_subtype") or "unknown")
            pp.create_sgen(
                net,
                bus=bus,
                p_mw=p_mw,
                q_mvar=0.0,
                name=f"ding0_oep_sgen_{gen_type}_{subtype}_{gen_id}",
                type=gen_type,
            )


def _call_iter(obj: Any, name: str) -> list[Any]:
    value = getattr(obj, name, None)
    if value is None:
        return []
    if callable(value):
        value = value()
    if value is None:
        return []
    if isinstance(value, dict):
        return list(value.values())
    return list(value)


def _collect_ding0_elements(obj: Any, names: tuple[str, ...]) -> list[Any]:
    items: list[Any] = []
    for name in names:
        items.extend(_call_iter(obj, name))
    return items


def _iter_lv_grids(mvgd: Any) -> list[Any]:
    grids: list[Any] = []
    for load_area in _call_iter(mvgd, "lv_load_areas"):
        for district in _call_iter(load_area, "lv_grid_districts"):
            grid = getattr(district, "lv_grid", None)
            if callable(grid):
                grid = grid()
            if grid is not None:
                grids.append(grid)
    for grid in _call_iter(mvgd, "lv_grids"):
        grids.append(grid)
    return grids


def _first_non_none(values: Any) -> Any:
    for value in values:
        if callable(value):
            value = value()
        if value is not None:
            return value
    return None


def _stable_id(obj: Any) -> str:
    for attr in ("id_db", "id", "grid_id"):
        value = getattr(obj, attr, None)
        if value is not None:
            return str(value)
    return str(obj)


def _name_for(prefix: str, obj: Any) -> str:
    return f"{prefix}_{type(obj).__name__}_{_stable_id(obj)}"


def _node_voltage_kv(node: Any) -> float:
    for attr in ("v_nom", "v_nom_kv", "voltage_level", "voltage_kv", "vn_kv"):
        value = getattr(node, attr, None)
        if value is not None:
            try:
                number = float(value)
                return number / 1000.0 if number > 1000.0 else number
            except (TypeError, ValueError):
                pass
    class_name = type(node).__name__.lower()
    if class_name.startswith("lv") or "lv" in class_name:
        return 0.4
    return 20.0


def _geo_xy(obj: Any) -> tuple[float, float] | None:
    geo = getattr(obj, "geo_data", None)
    if geo is None:
        return None
    x = getattr(geo, "x", None)
    y = getattr(geo, "y", None)
    if x is None or y is None:
        return None
    try:
        return float(x), float(y)
    except (TypeError, ValueError):
        return None


def _add_bus_geodata(net: Any, bus_idx: int, node: Any) -> None:
    xy = _geo_xy(node)
    if xy is None:
        return
    if not hasattr(net, "bus_geodata") or net.bus_geodata is None:
        net["bus_geodata"] = pd.DataFrame(columns=["x", "y"])
    net.bus_geodata.loc[bus_idx, ["x", "y"]] = xy


def _add_line_geodata(net: Any, line_idx: int, from_node: Any, to_node: Any) -> None:
    start = _geo_xy(from_node)
    end = _geo_xy(to_node)
    if start is None or end is None:
        return
    if not hasattr(net, "line_geodata") or net.line_geodata is None:
        net["line_geodata"] = pd.DataFrame(columns=["coords"])
    net.line_geodata.at[line_idx, "coords"] = [start, end]


def _set_bus_geodata(net: Any, bus_idx: int, lon: float, lat: float) -> None:
    if not hasattr(net, "bus_geodata") or net.bus_geodata is None:
        net["bus_geodata"] = pd.DataFrame(columns=["x", "y"])
    net.bus_geodata.loc[bus_idx, ["x", "y"]] = [float(lon), float(lat)]


def _set_line_geodata(net: Any, line_idx: int, start: tuple[float, float], end: tuple[float, float]) -> None:
    if not hasattr(net, "line_geodata") or net.line_geodata is None:
        net["line_geodata"] = pd.DataFrame(columns=["coords"])
    net.line_geodata.at[line_idx, "coords"] = [start, end]


def _row_lon_lat(row: dict[str, Any], *, geom_srid: str) -> tuple[float, float]:
    lon = _float_value(row.get("lon"))
    lat = _float_value(row.get("lat"))
    if lon is not None and lat is not None:
        return lon, lat
    geom = row.get("geom") or row.get("point")
    if not geom:
        return 0.0, 0.0
    try:
        from pyproj import Transformer
        from shapely import wkb

        point = wkb.loads(bytes.fromhex(str(geom)))
        if geom_srid == "EPSG:4326":
            return float(point.x), float(point.y)
        transformer = Transformer.from_crs(geom_srid, "EPSG:4326", always_xy=True)
        return tuple(float(value) for value in transformer.transform(point.x, point.y))  # type: ignore[return-value]
    except Exception:
        return 0.0, 0.0


def _sum_lv_peak_load_by_station(rows: list[dict[str, Any]]) -> dict[int, float]:
    result: dict[int, float] = {}
    peak_columns = (
        "sector_peakload_residential",
        "sector_peakload_retail",
        "sector_peakload_industrial",
        "sector_peakload_agricultural",
    )
    for row in rows:
        station_id = _int_value(row.get("mvlv_subst_id"))
        if station_id is None:
            continue
        peak_gw = sum(_float_value(row.get(column)) or 0.0 for column in peak_columns)
        if peak_gw <= 0.0:
            peak_gw = _float_value(row.get("sector_peakload_sum")) or _float_value(row.get("peak_load")) or 0.0
        result[station_id] = result.get(station_id, 0.0) + max(peak_gw * 1000.0, 0.0)
    return result


def _capacity_mw(row: dict[str, Any]) -> float:
    value = _float_value(row.get("electrical_capacity"))
    if value is None:
        value = _float_value(row.get("capacity"))
    if value is None:
        return 0.0
    return value / 1000.0 if value > 10.0 else value


def _float_value(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return float(value)
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _int_value(value: Any) -> int | None:
    if value is None:
        return None
    try:
        if isinstance(value, float) and math.isnan(value):
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _distance_km(start: tuple[float, float], end: tuple[float, float]) -> float:
    lon1, lat1 = start
    lon2, lat2 = end
    radius_km = 6371.0
    d_lat = math.radians(lat2 - lat1)
    d_lon = math.radians(lon2 - lon1)
    a = math.sin(d_lat / 2.0) ** 2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(d_lon / 2.0) ** 2
    return 2.0 * radius_km * math.asin(min(1.0, math.sqrt(a)))


def _q_from_cosphi(p_mw: float, cosphi: float) -> float:
    return abs(p_mw) * math.tan(math.acos(max(min(cosphi, 1.0), 0.01)))


def _is_oedialect_advanced_api_failure(exc: Exception) -> bool:
    text = f"{type(exc).__name__}: {exc}"
    cause = exc.__cause__
    while cause is not None:
        text += f"\n{type(cause).__name__}: {cause}"
        cause = cause.__cause__
    context = exc.__context__
    while context is not None:
        text += f"\n{type(context).__name__}: {context}"
        context = context.__context__
    return "405" in text and ("Method Not Allowed" in text or "ConnectionException" in text)


def _split_edge(edge: Any) -> tuple[Any | None, Any | None, Any]:
    if isinstance(edge, tuple):
        if len(edge) >= 3:
            return edge[0], edge[1], edge[2]
        if len(edge) == 2:
            return edge[0], edge[1], edge
    return getattr(edge, "node_from", None) or getattr(edge, "from_node", None), getattr(edge, "node_to", None) or getattr(edge, "to_node", None), edge


def _line_parameters(branch: Any) -> dict[str, float]:
    branch_type = getattr(branch, "type", None)
    if isinstance(branch_type, dict):
        lookup = branch_type
    else:
        lookup = getattr(branch_type, "__dict__", {}) if branch_type is not None else {}
    length = _float_attr(branch, ("length", "length_km"), 1.0)
    if length > 100.0:
        length = length / 1000.0
    return {
        "length_km": max(length, 1e-6),
        "r_ohm_per_km": max(_float_lookup(lookup, ("r_ohm_per_km", "r_per_km", "R_per_km"), _float_attr(branch, ("r_ohm_per_km", "r_per_km"), 0.1)), 1e-9),
        "x_ohm_per_km": max(_float_lookup(lookup, ("x_ohm_per_km", "x_per_km", "X_per_km"), _float_attr(branch, ("x_ohm_per_km", "x_per_km"), 0.08)), 1e-9),
        "c_nf_per_km": max(_float_lookup(lookup, ("c_nf_per_km", "c_per_km"), _float_attr(branch, ("c_nf_per_km", "c_per_km"), 0.0)), 0.0),
        "max_i_ka": max(_float_lookup(lookup, ("max_i_ka", "i_max_th", "I_max_th"), _float_attr(branch, ("max_i_ka", "i_max_th"), 0.2)), 1e-6),
    }


def _branch_type_name(branch: Any) -> str:
    branch_type = getattr(branch, "type", None)
    if branch_type is None:
        return ""
    if isinstance(branch_type, dict):
        for key in ("name", "type", "std_type"):
            if key in branch_type:
                return str(branch_type[key])
    return str(branch_type)


def _trafo_parameters(station: Any, net: Any, hv_bus: int, lv_bus: int) -> dict[str, float]:
    hv_kv = float(net.bus.at[hv_bus, "vn_kv"])
    lv_kv = float(net.bus.at[lv_bus, "vn_kv"])
    return {
        "sn_mva": max(_float_attr(station, ("s_nom", "s_nom_mva", "sn_mva"), 0.63), 1e-6),
        "vn_hv_kv": max(hv_kv, lv_kv),
        "vn_lv_kv": min(hv_kv, lv_kv),
        "vk_percent": max(_float_attr(station, ("vk_percent", "vsc_percent"), 6.0), 1e-6),
        "vkr_percent": max(_float_attr(station, ("vkr_percent", "vscr_percent"), 1.2), 0.0),
        "pfe_kw": max(_float_attr(station, ("pfe_kw",), 0.0), 0.0),
        "i0_percent": max(_float_attr(station, ("i0_percent",), 0.0), 0.0),
        "shift_degree": _float_attr(station, ("shift_degree",), 0.0),
    }


def _attached_node(element: Any) -> Any:
    for attr in ("bus", "node", "grid_node", "connects_to", "station"):
        value = getattr(element, attr, None)
        if value is not None:
            return value
    return None


def _active_power_mw(element: Any) -> float:
    value = _float_attr(element, ("p_mw", "peak_load", "peak_load_mw", "capacity", "capacity_mw", "p_nom"), 0.0)
    return value / 1000.0 if value > 1000.0 else value


def _reactive_power_mvar(element: Any, p_mw: float) -> float:
    q = _float_attr(element, ("q_mvar", "q_mw"), math.nan)
    if math.isfinite(q):
        return q
    cos_phi = _float_attr(element, ("cos_phi",), 0.95)
    cos_phi = min(max(abs(cos_phi), 1e-6), 1.0)
    return p_mw * math.tan(math.acos(cos_phi))


def _element_type(element: Any) -> str:
    parts = []
    for attr in ("type", "subtype", "generation_type"):
        value = getattr(element, attr, None)
        if value is not None:
            parts.append(str(value))
    return "/".join(parts) if parts else type(element).__name__


def _float_attr(obj: Any, attrs: tuple[str, ...], default: float) -> float:
    for attr in attrs:
        value = getattr(obj, attr, None)
        if value is None:
            continue
        try:
            return float(value)
        except (TypeError, ValueError):
            continue
    return default


def _float_lookup(lookup: dict[str, Any], keys: tuple[str, ...], default: float) -> float:
    for key in keys:
        if key not in lookup:
            continue
        try:
            return float(lookup[key])
        except (TypeError, ValueError):
            continue
    return default


def _mv_grid_district_ids(mv_grid_districts: list[Any]) -> list[int]:
    ids: list[int] = []
    for mvgd in mv_grid_districts:
        value = getattr(mvgd, "id_db", None) or getattr(mvgd, "grid_district_id", None) or getattr(mvgd, "id", None)
        if value is None:
            continue
        try:
            ids.append(int(value))
        except (TypeError, ValueError):
            continue
    return ids


def _skipped_districts(nd: Any, requested: list[int]) -> list[int]:
    for attr in ("skipped_mv_grid_districts", "ding0_skipped_districts", "skipped_districts"):
        value = getattr(nd, attr, None)
        if value is None:
            continue
        try:
            return [int(item) for item in value]
        except TypeError:
            return []
    present = set(_mv_grid_district_ids(list(_call_iter(nd, "mv_grid_districts"))))
    return [int(value) for value in requested if int(value) not in present]


def import_ding0(path: str | Path, **kwargs: Any) -> CanonicalCase:
    return Ding0ImportAdapter().import_case(path, **kwargs)


__all__ = ["Ding0ImportAdapter", "import_ding0"]
