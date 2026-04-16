from __future__ import annotations

from dataclasses import dataclass
import importlib
import importlib.util
import math
from pathlib import Path
import pkgutil
import sys
import types
from typing import Any

import numpy as np
import pandapower as pp
from pandapower.pypower import idx_brch as pp_idx_brch
from pandapower.pypower.idx_brch import BR_B, BR_R, BR_STATUS, BR_X, F_BUS, RATE_A, SHIFT, TAP, T_BUS
from pandapower.pypower.idx_bus import BASE_KV, BS, BUS_I, BUS_TYPE, GS, PD, PQ, QD, REF, VA, VM, VMAX, VMIN
from pandapower.pypower.idx_gen import GEN_BUS, GEN_STATUS, PG, PMAX, PMIN, QG, QMAX, QMIN, VG

from powermodelconverter.adapters.base import ImportAdapter
from powermodelconverter.adapters.pandapower_adapter import PandapowerAdapter
from powermodelconverter.core.model import CanonicalCase


@dataclass(slots=True)
class PypowerResultSnapshot:
    slack_p_mw: float
    slack_q_mvar: float
    voltages: dict[str, complex]


class PypowerImportAdapter(ImportAdapter):
    source_format = "pypower"
    _REQUIRED_PYPOWER_BRANCH_COLS = (
        max(
            (
                getattr(pp_idx_brch, name)
                for name in dir(pp_idx_brch)
                if name.startswith("BR_") and isinstance(getattr(pp_idx_brch, name), int)
            ),
            default=-1,
        )
        + 1
    )

    _DTU_TRAFO_LOCATIONS = np.array(
        [0, 3, 5, 8, 11, 14, 15, 16, 18, 20, 22, 25, 27, 29, 31, 34, 36, 38, 39, 43, 47, 48, 50],
        dtype=int,
    )

    def __init__(self) -> None:
        self._pandapower = PandapowerAdapter()
        self._dtu_connected_cache: dict[tuple[str, int], dict[str, Any]] = {}

    def import_case(self, source: str | Path, **kwargs: Any) -> CanonicalCase:
        source_spec = self._parse_source_spec(source)
        path = source_spec["path"]
        source_solution = self._load_source_solution(path, source_spec=source_spec, **kwargs)
        ppc = source_solution["ppc"]
        net = self._ppc_to_pandapower_net(ppc)
        snapshot = source_solution.get("snapshot")
        if snapshot is not None:
            self._attach_source_voltage_initialization(net, snapshot)
        case_id = path.stem
        if source_spec["network_id"] is not None:
            case_id = f"{case_id}_{source_spec['network_id']}"
        return self._pandapower.to_canonical(
            net,
            case_id=case_id,
            source_format=self.source_format,
            metadata={
                "import_backend": "pypower:python_case",
                "pypower_source_kind": self._source_kind(path, source_spec=source_spec),
                "pypower_network_id": source_spec["network_id"],
                "pypower_timeseries_index": source_solution.get("timeseries_index"),
                "pypower_sample_index": source_solution.get("sample_index"),
            },
            source_path=path,
        )

    def solve_source_case(self, source: str | Path, **kwargs: Any) -> PypowerResultSnapshot:
        source_spec = self._parse_source_spec(source)
        path = source_spec["path"]
        source_solution = self._load_source_solution(path, source_spec=source_spec, **kwargs)
        snapshot = source_solution.get("snapshot")
        if snapshot is not None:
            return snapshot
        case = self.import_case(source, **kwargs)
        net = self._pandapower.run_power_flow(case)
        slack_p = float(net.res_ext_grid.p_mw.sum()) if len(net.ext_grid) else 0.0
        slack_q = float(net.res_ext_grid.q_mvar.sum()) if len(net.ext_grid) else 0.0
        if len(net.gen) and len(net.res_gen) and "slack" in net.gen.columns:
            slack_mask = net.gen["slack"].fillna(False)
            if bool(slack_mask.any()):
                slack_p += float(net.res_gen.loc[slack_mask, "p_mw"].sum())
                slack_q += float(net.res_gen.loc[slack_mask, "q_mvar"].sum())
        voltages = {
            self._bus_key(net, int(idx)): complex(
                float(row.vm_pu) * math.cos(math.radians(float(row.va_degree))),
                float(row.vm_pu) * math.sin(math.radians(float(row.va_degree))),
            )
            for idx, row in net.res_bus.iterrows()
        }
        return PypowerResultSnapshot(slack_p_mw=slack_p, slack_q_mvar=slack_q, voltages=voltages)

    def _load_source_solution(self, path: Path, *, source_spec: dict[str, Any], **kwargs: Any) -> dict[str, Any]:
        if path.suffix.lower() != ".py":
            raise ValueError(f"Unsupported PYPOWER source format for {path}. Use a Python case file.")
        if path.stem == "DTU_ADN" and (path.parent / "DTU_ADN").is_dir():
            return {"ppc": self._load_dtu_network_only_ppc(path)}
        return self._load_python_case_solution(path, source_spec=source_spec, **kwargs)

    def _load_dtu_network_only_ppc(self, path: Path) -> dict[str, Any]:
        data_root = path.parent / "DTU_ADN"
        bus = np.genfromtxt(data_root / "network_60kV" / "bus_60kV.csv", delimiter=",", dtype=float)
        gen = np.genfromtxt(data_root / "network_60kV" / "gen_60kV.csv", delimiter=",", dtype=float)
        branch = np.genfromtxt(data_root / "network_60kV" / "line_60kV.csv", delimiter=",", dtype=float)

        bus[:, BUS_I] -= 1
        gen[:, GEN_BUS] -= 1
        branch[:, F_BUS] -= 1
        branch[:, T_BUS] -= 1
        branch[:, RATE_A] = 100.0
        branch[0, RATE_A] = 250.0
        branch[self._DTU_TRAFO_LOCATIONS, TAP] = 1.0

        return {
            "version": "2",
            "baseMVA": 100.0,
            "bus": bus,
            "gen": gen,
            "branch": branch,
        }

    def _load_python_case_solution(self, path: Path, *, source_spec: dict[str, Any], **kwargs: Any) -> dict[str, Any]:
        self._install_pypower_aliases()
        self._install_optional_dependency_stubs()
        module_name = f"_pmc_pypower_{path.stem}"
        spec = importlib.util.spec_from_file_location(module_name, path)
        if spec is None or spec.loader is None:
            raise ValueError(f"Could not load PYPOWER source module from {path}")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        connected_net_solution = self._load_connected_dtu_style_module(module, source_spec=source_spec)
        if connected_net_solution is not None:
            return connected_net_solution

        case_function = getattr(module, path.stem, None)
        if callable(case_function):
            ppc = case_function(**kwargs)
            if self._looks_like_ppc(ppc):
                return {"ppc": ppc}

        module_ppc = getattr(module, "ppc", None)
        if self._looks_like_ppc(module_ppc):
            return {"ppc": module_ppc}

        for name in dir(module):
            candidate = getattr(module, name)
            if not callable(candidate) or name.startswith("_"):
                continue
            try:
                ppc = candidate()
            except TypeError:
                continue
            if self._looks_like_ppc(ppc):
                return {"ppc": ppc}

        raise ValueError(
            f"Could not find a PYPOWER case dictionary in {path}. "
            "Expected a case function, a module-level ppc variable, or a zero-argument callable returning a ppc dict."
        )

    def _load_connected_dtu_style_module(self, module: Any, *, source_spec: dict[str, Any]) -> dict[str, Any] | None:
        make_connected_net = getattr(module, "make_connected_net", None)
        if not callable(make_connected_net):
            return None

        network_id = source_spec["network_id"]
        if network_id is None:
            parse_args = getattr(module, "parse_args", None)
            if callable(parse_args):
                original_argv = sys.argv
                try:
                    sys.argv = [str(source_spec["path"])]
                    parsed = parse_args()
                except SystemExit:
                    parsed = None
                finally:
                    sys.argv = original_argv
                if parsed is not None and hasattr(parsed, "network_id"):
                    network_id = int(parsed.network_id)
        if network_id is None:
            network_id = 26
        source_spec["network_id"] = int(network_id)
        cache_key = (str(source_spec["path"]), int(network_id))
        cached = self._dtu_connected_cache.get(cache_key)
        if cached is not None:
            return cached
        solved = self._solve_connected_dtu_operating_point(module, int(network_id), source_spec["path"])
        self._dtu_connected_cache[cache_key] = solved
        return solved

    def _solve_connected_dtu_operating_point(self, module: Any, network_id: int, path: Path) -> dict[str, Any]:
        parsed = self._parse_module_defaults(module, path)
        n_train = int(getattr(parsed, "n_train", 256))
        n_test = int(getattr(parsed, "n_test", 32))
        seed = int(getattr(parsed, "seed", network_id))
        lv_range = (float(getattr(parsed, "lv_min", -0.20)), float(getattr(parsed, "lv_max", 0.20)))
        mv_range = (float(getattr(parsed, "mv_min", 0.75)), float(getattr(parsed, "mv_max", 1.35)))
        gen_range = (float(getattr(parsed, "gen_min", 0.55)), float(getattr(parsed, "gen_max", 1.55)))
        time_correlation = float(getattr(parsed, "time_correlation", 0.35))
        shared_correlation = float(getattr(parsed, "shared_correlation", 0.15))
        transformer_correlation = float(getattr(parsed, "transformer_correlation", 0.15))
        local_noise = float(getattr(parsed, "local_noise", 0.20))

        rng = np.random.default_rng(seed)
        net = module.make_connected_net(network_id)
        topology_df, lv_groups, mv_buses = module.build_topology_tables(net, network_id)
        lv_buses = topology_df["lv_bus_connected"].astype(int).tolist()
        gen_buses = np.where(np.isin(net.net["gen"][:, GEN_BUS].astype(int), lv_buses + mv_buses))[0]

        start_time = getattr(module.pd, "Timestamp")(year=2015, month=7, day=1, hour=0)
        end_time = getattr(module.pd, "Timestamp")(year=2015, month=7, day=30, hour=23)
        module.DTU_ADN.init_timeseries_index(start_time.to_pydatetime(), end_time.to_pydatetime())
        start_idx = int(module.DTU_ADN.start_idx[0])
        end_idx = int(module.DTU_ADN.end_idx[0])

        total_samples = n_train + n_test
        available_samples = end_idx - start_idx + 1
        candidate_samples = max(total_samples * 6, total_samples + 24)
        if candidate_samples <= available_samples:
            max_start = max(start_idx, end_idx - candidate_samples + 1)
            start_sample = int(rng.integers(start_idx, max_start + 1))
            timeseries_indices = np.arange(start_sample, start_sample + candidate_samples, dtype=int)
        else:
            timeseries_indices = rng.integers(start_idx, end_idx + 1, size=candidate_samples, dtype=int)
        lv_p_setpoints, lv_q_setpoints, mv_scales, gen_scales = module.build_correlated_profiles(
            rng=rng,
            total_samples=candidate_samples,
            lv_groups=lv_groups,
            mv_count=len(mv_buses),
            gen_count=len(gen_buses),
            lv_range=lv_range,
            mv_range=mv_range,
            gen_range=gen_range,
            time_correlation=time_correlation,
            shared_correlation=shared_correlation,
            transformer_correlation=transformer_correlation,
            local_noise=local_noise,
        )

        for sample_index, timeseries_index in enumerate(timeseries_indices):
            solved = self._run_connected_dtu_sample(
                module=module,
                network_id=network_id,
                timeseries_index=int(timeseries_index),
                lv_buses=lv_buses,
                mv_buses=mv_buses,
                lv_p=lv_p_setpoints[sample_index],
                lv_q=lv_q_setpoints[sample_index],
                mv_scale=mv_scales[sample_index],
                gen_scale=gen_scales[sample_index],
            )
            if solved is None:
                continue
            result_ppc = solved["result"]
            snapshot = self._snapshot_from_ppc(result_ppc)
            return {
                "ppc": result_ppc,
                "snapshot": snapshot,
                "timeseries_index": int(timeseries_index),
                "sample_index": int(sample_index),
            }
        raise ValueError(
            f"Could not obtain a converged DTU connected-network source execution for subnet {network_id} from {path}."
        )

    def _run_connected_dtu_sample(
        self,
        *,
        module: Any,
        network_id: int,
        timeseries_index: int,
        lv_buses: list[int],
        mv_buses: list[int],
        lv_p: np.ndarray[Any, Any],
        lv_q: np.ndarray[Any, Any],
        mv_scale: np.ndarray[Any, Any],
        gen_scale: np.ndarray[Any, Any],
    ) -> dict[str, Any] | None:
        net = module.make_connected_net(network_id)
        bus_numbers = net.net["bus"][:, BUS_I].astype(int)
        bus_row = {bus_no: idx for idx, bus_no in enumerate(bus_numbers)}
        lv_rows = np.array([bus_row[bus] for bus in lv_buses], dtype=int)
        mv_rows = np.array([bus_row[bus] for bus in mv_buses], dtype=int)
        gen_rows = np.where(np.isin(net.net["gen"][:, GEN_BUS].astype(int), lv_buses + mv_buses))[0]

        module.quiet_call(net.gen_and_demand_net_60, timeseries_index)
        module.quiet_call(net.gen_and_demand_net, timeseries_index, np.array([network_id]))
        self._ensure_pypower_branch_width(net.net)
        options = module.ppoption(ENFORCE_Q_LIMS=0, VERBOSE=0, OUT_ALL=0)

        for shrink in (1.0, 0.7, 0.45, 0.25, 0.12):
            module.apply_scales(
                net,
                lv_rows,
                mv_rows,
                gen_rows,
                shrink * lv_p,
                shrink * lv_q,
                1.0 + shrink * (mv_scale - 1.0),
                1.0 + shrink * (gen_scale - 1.0),
            )
            result, success = module.quiet_call(module.DTU_ADN.runpf, net.net, ppopt=options)
            if success:
                return {"result": result}
            module.quiet_call(net.gen_and_demand_net_60, timeseries_index)
            module.quiet_call(net.gen_and_demand_net, timeseries_index, np.array([network_id]))
            self._ensure_pypower_branch_width(net.net)
        return None

    def _ensure_pypower_branch_width(self, ppc: dict[str, Any]) -> None:
        required_cols = int(self._REQUIRED_PYPOWER_BRANCH_COLS)
        if required_cols <= 0:
            return
        branch = np.asarray(ppc.get("branch", []), dtype=float)
        if branch.ndim != 2 or branch.shape[1] >= required_cols:
            return
        padded = np.zeros((branch.shape[0], required_cols), dtype=float)
        padded[:, : branch.shape[1]] = branch
        ppc["branch"] = padded

    def _snapshot_from_ppc(self, ppc: dict[str, Any]) -> PypowerResultSnapshot:
        bus = np.asarray(ppc["bus"], dtype=float)
        gen = np.asarray(ppc["gen"], dtype=float)
        slack_buses = {
            int(round(float(row[BUS_I])))
            for row in bus
            if int(round(float(row[BUS_TYPE]))) == REF
        }
        slack_p = 0.0
        slack_q = 0.0
        for row in gen:
            if int(round(float(row[GEN_BUS]))) in slack_buses:
                slack_p += float(row[PG])
                slack_q += float(row[QG])
        voltages = {
            str(int(round(float(row[BUS_I])))): complex(
                float(row[VM]) * math.cos(math.radians(float(row[VA]))),
                float(row[VM]) * math.sin(math.radians(float(row[VA]))),
            )
            for row in bus
        }
        return PypowerResultSnapshot(slack_p_mw=slack_p, slack_q_mvar=slack_q, voltages=voltages)

    def _attach_source_voltage_initialization(self, net: Any, snapshot: PypowerResultSnapshot) -> None:
        init_vm: list[float] = []
        init_va: list[float] = []
        for _, row in net.bus.iterrows():
            key = str(row["name"]).strip()
            value = snapshot.voltages.get(key)
            if value is None:
                init_vm.append(1.0)
                init_va.append(0.0)
                continue
            init_vm.append(abs(value))
            init_va.append(math.degrees(math.atan2(value.imag, value.real)))
        net.bus["pmc_init_vm_pu"] = init_vm
        net.bus["pmc_init_va_degree"] = init_va

    def _parse_module_defaults(self, module: Any, path: Path) -> Any:
        parse_args = getattr(module, "parse_args", None)
        if not callable(parse_args):
            return types.SimpleNamespace()
        original_argv = sys.argv
        try:
            sys.argv = [str(path)]
            return parse_args()
        except SystemExit:
            return types.SimpleNamespace()
        finally:
            sys.argv = original_argv

    def _ppc_to_pandapower_net(self, ppc: dict[str, Any]) -> Any:
        bus = np.asarray(ppc["bus"], dtype=float)
        gen = np.asarray(ppc["gen"], dtype=float)
        branch = np.asarray(ppc["branch"], dtype=float)
        base_mva = float(ppc.get("baseMVA", 100.0))
        net = pp.create_empty_network(sn_mva=base_mva, f_hz=50.0)

        bus_lookup: dict[int, int] = {}
        bus_kv: dict[int, float] = {}
        bus_types: dict[int, int] = {}
        bus_rows: dict[int, np.ndarray[Any, Any]] = {}

        for row in bus:
            ext_id = int(round(float(row[BUS_I])))
            vn_kv = float(row[BASE_KV]) if len(row) > BASE_KV else 0.0
            pp_idx = pp.create_bus(
                net,
                vn_kv=vn_kv if vn_kv > 0 else 1.0,
                name=str(ext_id),
                max_vm_pu=float(row[VMAX]) if len(row) > VMAX else 1.1,
                min_vm_pu=float(row[VMIN]) if len(row) > VMIN else 0.9,
                in_service=True,
            )
            bus_lookup[ext_id] = pp_idx
            bus_kv[ext_id] = vn_kv if vn_kv > 0 else 1.0
            bus_types[ext_id] = int(round(float(row[BUS_TYPE]))) if len(row) > BUS_TYPE else PQ
            bus_rows[ext_id] = row

            gs = float(row[GS]) if len(row) > GS else 0.0
            bs = float(row[BS]) if len(row) > BS else 0.0
            if abs(gs) > 0.0 or abs(bs) > 0.0:
                pp.create_shunt(net, bus=pp_idx, p_mw=gs, q_mvar=-bs, vn_kv=bus_kv[ext_id], name=f"shunt_{ext_id}")

            pd = float(row[PD]) if len(row) > PD else 0.0
            qd = float(row[QD]) if len(row) > QD else 0.0
            if abs(pd) > 0.0 or abs(qd) > 0.0:
                pp.create_load(net, bus=pp_idx, p_mw=pd, q_mvar=qd, name=f"load_{ext_id}")

        ext_grid_buses: set[int] = set()
        for idx, row in enumerate(gen):
            if len(row) <= GEN_STATUS or int(round(float(row[GEN_STATUS]))) <= 0:
                continue
            ext_bus = int(round(float(row[GEN_BUS])))
            if ext_bus not in bus_lookup:
                continue
            bus_row = bus_rows.get(ext_bus)
            vm_pu = float(row[VG]) if len(row) > VG else float(bus_row[VM]) if bus_row is not None and len(bus_row) > VM else 1.0
            bus_type = bus_types.get(ext_bus, PQ)
            if bus_type == REF and ext_bus not in ext_grid_buses:
                pp.create_ext_grid(
                    net,
                    bus=bus_lookup[ext_bus],
                    vm_pu=vm_pu,
                    name=f"ext_grid_{idx}",
                    in_service=True,
                )
                ext_grid_buses.add(ext_bus)
                continue

            pp.create_gen(
                net,
                bus=bus_lookup[ext_bus],
                p_mw=float(row[PG]) if len(row) > PG else 0.0,
                vm_pu=vm_pu,
                min_p_mw=float(row[PMIN]) if len(row) > PMIN else math.nan,
                max_p_mw=float(row[PMAX]) if len(row) > PMAX else math.nan,
                min_q_mvar=float(row[QMIN]) if len(row) > QMIN else math.nan,
                max_q_mvar=float(row[QMAX]) if len(row) > QMAX else math.nan,
                name=f"gen_{idx}",
                slack=bus_type == REF,
                in_service=True,
            )

        for idx, row in enumerate(branch):
            if len(row) <= BR_STATUS or int(round(float(row[BR_STATUS]))) <= 0:
                continue
            from_bus = int(round(float(row[F_BUS])))
            to_bus = int(round(float(row[T_BUS])))
            if from_bus not in bus_lookup or to_bus not in bus_lookup:
                continue
            if self._is_transformer_branch(row, bus_kv[from_bus], bus_kv[to_bus]):
                self._create_transformer_from_branch(
                    net=net,
                    row=row,
                    from_bus=from_bus,
                    to_bus=to_bus,
                    bus_lookup=bus_lookup,
                    bus_kv=bus_kv,
                    base_mva=base_mva,
                    index=idx,
                )
                continue
            self._create_line_from_branch(
                net=net,
                row=row,
                from_bus=from_bus,
                to_bus=to_bus,
                bus_lookup=bus_lookup,
                bus_kv=bus_kv,
                base_mva=base_mva,
                index=idx,
            )

        return net

    def _create_line_from_branch(
        self,
        *,
        net: Any,
        row: np.ndarray[Any, Any],
        from_bus: int,
        to_bus: int,
        bus_lookup: dict[int, int],
        bus_kv: dict[int, float],
        base_mva: float,
        index: int,
    ) -> None:
        vn_kv = max(bus_kv[from_bus], bus_kv[to_bus], 1e-6)
        z_base = (vn_kv**2) / base_mva
        y_base = 1.0 / z_base
        b_pu = float(row[BR_B]) if len(row) > BR_B else 0.0
        c_nf_per_km = abs(b_pu * y_base) / (2.0 * math.pi * net.f_hz) * 1e9 if abs(b_pu) > 0.0 else 0.0
        rate_a = float(row[RATE_A]) if len(row) > RATE_A else 0.0
        max_i_ka = rate_a / (math.sqrt(3.0) * vn_kv) if rate_a > 0.0 else 999.0
        pp.create_line_from_parameters(
            net,
            from_bus=bus_lookup[from_bus],
            to_bus=bus_lookup[to_bus],
            length_km=1.0,
            r_ohm_per_km=float(row[BR_R]) * z_base,
            x_ohm_per_km=float(row[BR_X]) * z_base,
            c_nf_per_km=c_nf_per_km,
            max_i_ka=max_i_ka,
            name=f"branch_{index}",
            in_service=True,
        )

    def _create_transformer_from_branch(
        self,
        *,
        net: Any,
        row: np.ndarray[Any, Any],
        from_bus: int,
        to_bus: int,
        bus_lookup: dict[int, int],
        bus_kv: dict[int, float],
        base_mva: float,
        index: int,
    ) -> None:
        from_kv = bus_kv[from_bus]
        to_kv = bus_kv[to_bus]
        hv_bus = from_bus if from_kv >= to_kv else to_bus
        lv_bus = to_bus if hv_bus == from_bus else from_bus
        rate_a = float(row[RATE_A]) if len(row) > RATE_A and float(row[RATE_A]) > 0.0 else base_mva
        scale = rate_a / base_mva
        tap_ratio = float(row[TAP]) if len(row) > TAP and abs(float(row[TAP])) > 0.0 else 1.0
        shift_degree = float(row[SHIFT]) if len(row) > SHIFT else 0.0
        tap_side = "hv" if hv_bus == from_bus else "lv"
        tap_step_percent = abs(tap_ratio - 1.0) * 100.0
        tap_pos = 0.0
        create_kwargs: dict[str, Any] = {
            "hv_bus": bus_lookup[hv_bus],
            "lv_bus": bus_lookup[lv_bus],
            "sn_mva": rate_a,
            "vn_hv_kv": max(from_kv, to_kv),
            "vn_lv_kv": min(from_kv, to_kv),
            "vk_percent": math.hypot(float(row[BR_R]), float(row[BR_X])) * 100.0 * scale,
            "vkr_percent": float(row[BR_R]) * 100.0 * scale,
            "pfe_kw": 0.0,
            "i0_percent": 0.0,
            "shift_degree": shift_degree,
            "name": f"trafo_{index}",
            "in_service": True,
        }
        if tap_step_percent > 0.0:
            create_kwargs.update(
                {
                    "tap_side": tap_side,
                    "tap_neutral": 0.0,
                    "tap_min": -1.0,
                    "tap_max": 1.0,
                    "tap_step_percent": tap_step_percent,
                    "tap_pos": 1.0 if tap_ratio > 1.0 else -1.0,
                }
            )
        pp.create_transformer_from_parameters(net, **create_kwargs)

    def _is_transformer_branch(self, row: np.ndarray[Any, Any], from_kv: float, to_kv: float) -> bool:
        tap_ratio = float(row[TAP]) if len(row) > TAP else 0.0
        shift_degree = float(row[SHIFT]) if len(row) > SHIFT else 0.0
        return not math.isclose(from_kv, to_kv, rel_tol=0.0, abs_tol=1e-9) or abs(tap_ratio) > 0.0 or abs(shift_degree) > 0.0

    def _install_pypower_aliases(self) -> None:
        if importlib.util.find_spec("pypower") is not None:
            return
        pkg = importlib.import_module("pandapower.pypower")
        sys.modules.setdefault("pypower", pkg)
        for module_info in pkgutil.iter_modules(pkg.__path__):
            alias = f"pypower.{module_info.name}"
            target = f"pandapower.pypower.{module_info.name}"
            sys.modules.setdefault(alias, importlib.import_module(target))
        if "pypower.api" not in sys.modules:
            api_module = types.ModuleType("pypower.api")
            api_module.ppoption = sys.modules["pypower.ppoption"].ppoption
            sys.modules["pypower.api"] = api_module
        self._install_legacy_pypower_helpers()

    def _install_legacy_pypower_helpers(self) -> None:
        helper_builders: dict[str, Any] = {
            "pypower.ext2int": lambda: self._simple_callable_module("pypower.ext2int", "ext2int", lambda value: value),
            "pypower.int2ext": lambda: self._simple_callable_module("pypower.int2ext", "int2ext", lambda value: value),
            "pypower.loadcase": lambda: self._simple_callable_module("pypower.loadcase", "loadcase", self._loadcase_compat),
            "pypower.savecase": lambda: self._simple_callable_module("pypower.savecase", "savecase", self._savecase_compat),
        }
        for module_name, builder in helper_builders.items():
            if module_name not in sys.modules:
                sys.modules[module_name] = builder()

    def _simple_callable_module(self, module_name: str, attr_name: str, func: Any) -> types.ModuleType:
        module = types.ModuleType(module_name)
        setattr(module, attr_name, func)
        return module

    def _loadcase_compat(self, value: Any) -> Any:
        if isinstance(value, dict):
            return value
        raise RuntimeError(f"loadcase compatibility shim only supports in-memory case dicts, got {type(value)!r}")

    def _savecase_compat(self, *args: Any, **kwargs: Any) -> None:
        raise RuntimeError("savecase is not available in this environment")

    def _install_optional_dependency_stubs(self) -> None:
        if "openpyxl" in sys.modules:
            return
        if importlib.util.find_spec("openpyxl") is None:
            stub = types.ModuleType("openpyxl")

            def _missing_openpyxl(*args: Any, **kwargs: Any) -> None:
                raise RuntimeError("openpyxl is not available in this environment")

            stub.load_workbook = _missing_openpyxl
            sys.modules["openpyxl"] = stub

    def _source_kind(self, path: Path, *, source_spec: dict[str, Any]) -> str:
        if path.stem == "DTU_ADN" and (path.parent / "DTU_ADN").is_dir():
            return "dtu_network_only"
        if source_spec["network_id"] is not None or path.stem.lower() == "dtu7k":
            return "dtu_connected_network"
        return "python_module"

    def _parse_source_spec(self, source: str | Path) -> dict[str, Any]:
        raw = str(source)
        if "::" not in raw:
            return {"path": Path(raw), "network_id": None}
        path_text, network_id_text = raw.rsplit("::", 1)
        if not network_id_text.strip():
            return {"path": Path(path_text), "network_id": None}
        return {"path": Path(path_text), "network_id": int(network_id_text)}

    def _looks_like_ppc(self, value: Any) -> bool:
        return isinstance(value, dict) and all(key in value for key in ("bus", "gen", "branch"))

    def _bus_key(self, net: Any, bus_idx: int) -> str:
        value = net.bus.loc[bus_idx, "name"] if "name" in net.bus.columns else None
        if value is None:
            return f"BUS{bus_idx}"
        text = str(value).strip()
        return text if text else f"BUS{bus_idx}"
