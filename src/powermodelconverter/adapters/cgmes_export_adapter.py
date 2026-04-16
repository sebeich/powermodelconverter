from __future__ import annotations

import math
import uuid
import zipfile
from collections import OrderedDict
from pathlib import Path
from xml.etree import ElementTree as ET

from powermodelconverter.adapters.pandapower_adapter import PandapowerAdapter
from powermodelconverter.core.model import CanonicalCase


RDF_NS = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
CIM_NS = "http://iec.ch/TC57/2013/CIM-schema-cim16#"
MD_NS = "http://iec.ch/TC57/61970-552/ModelDescription/1#"
ENTSOE_NS = "http://entsoe.eu/CIM/SchemaExtension/3/1#"

ET.register_namespace("rdf", RDF_NS)
ET.register_namespace("cim", CIM_NS)
ET.register_namespace("md", MD_NS)
ET.register_namespace("entsoe", ENTSOE_NS)


class CGMESExportAdapter:
    format_name = "cgmes"

    def __init__(self) -> None:
        self._pandapower = PandapowerAdapter()

    def export_case(self, case: CanonicalCase, destination: str | Path) -> Path:
        if case.is_unbalanced:
            raise ValueError("CGMES export currently supports balanced cases only.")

        path = Path(destination)
        path.parent.mkdir(parents=True, exist_ok=True)

        net = self._pandapower.to_net(case)
        self._validate_supported_net(net)
        solved_net = self._pandapower.run_power_flow(case)

        profile_ids = {
            "eq": self._urn(),
            "ssh": self._urn(),
            "tp": self._urn(),
            "sv": self._urn(),
        }
        assets = self._build_assets(case.case_id, net, solved_net)
        documents = {
            f"{case.case_id}_EQ.xml": self._render_eq(case.case_id, profile_ids, assets),
            f"{case.case_id}_SSH.xml": self._render_ssh(case.case_id, profile_ids, assets),
            f"{case.case_id}_TP.xml": self._render_tp(case.case_id, profile_ids, assets),
            f"{case.case_id}_SV.xml": self._render_sv(case.case_id, profile_ids, assets),
        }
        with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            for name, data in documents.items():
                archive.writestr(name, data)
        return path

    def _validate_supported_net(self, net: object) -> None:
        ext_grid_count = len(getattr(net, "ext_grid"))
        gen_table = getattr(net, "gen")
        if ext_grid_count + len(gen_table) == 0:
            raise ValueError("CGMES export requires at least one source element (ext_grid or gen).")
        unsupported_tables = {
            "sgen": "static generators",
            "switch": "switches",
            "shunt": "shunts",
            "impedance": "impedances",
            "ward": "wards",
            "xward": "extended wards",
            "motor": "motors",
            "storage": "storage units",
        }
        for table_name, description in unsupported_tables.items():
            table = getattr(net, table_name, None)
            if table is not None and len(table):
                raise ValueError(f"CGMES export does not yet support {description}.")

    def _build_assets(self, case_id: str, net: object, solved_net: object) -> dict[str, object]:
        base_voltage_ids: OrderedDict[float, str] = OrderedDict()
        for _, row in net.bus.iterrows():
            kv = float(row.vn_kv)
            base_voltage_ids.setdefault(kv, self._rdf_id())

        region_id = self._rdf_id()
        subregion_id = self._rdf_id()
        operational_limit_type_id = self._rdf_id()

        buses: dict[int, dict[str, object]] = {}
        for idx, row in net.bus.iterrows():
            buses[int(idx)] = {
                "rdf_id": self._rdf_id(),
                "voltage_level_id": self._rdf_id(),
                "substation_id": self._rdf_id(),
                "name": self._bus_name(row.get("name"), int(idx)),
                "vn_kv": float(row.vn_kv),
                "base_voltage_id": base_voltage_ids[float(row.vn_kv)],
                "vm_pu": float(solved_net.res_bus.at[idx, "vm_pu"]),
                "va_degree": float(solved_net.res_bus.at[idx, "va_degree"]),
            }

        loads: list[dict[str, object]] = []
        for idx, row in net.load.iterrows():
            loads.append(
                {
                    "rdf_id": self._rdf_id(),
                    "terminal_id": self._rdf_id(),
                    "response_id": self._rdf_id(),
                    "name": self._name(row.get("name"), f"Load_{idx}"),
                    "bus": int(row.bus),
                    "p_mw": float(row.p_mw),
                    "q_mvar": float(row.q_mvar),
                    "in_service": bool(row.get("in_service", True)),
                }
            )

        lines: list[dict[str, object]] = []
        for idx, row in net.line.iterrows():
            max_i_ka = float(row.max_i_ka) if not math.isnan(float(row.max_i_ka)) else 1.0
            r1 = float(row.r_ohm_per_km)
            x1 = float(row.x_ohm_per_km)
            c1 = float(row.c_nf_per_km)
            r0 = self._line_param(row, "r0_ohm_per_km", r1)
            x0 = self._line_param(row, "x0_ohm_per_km", x1)
            c0 = self._line_param(row, "c0_nf_per_km", c1)
            g1 = self._line_param(row, "g_us_per_km", 0.0)
            g0 = self._line_param(row, "g0_us_per_km", g1)
            lines.append(
                {
                    "rdf_id": self._rdf_id(),
                    "terminal_from_id": self._rdf_id(),
                    "terminal_to_id": self._rdf_id(),
                    "limit_from_id": self._rdf_id(),
                    "limit_to_id": self._rdf_id(),
                    "current_limit_from_id": self._rdf_id(),
                    "current_limit_to_id": self._rdf_id(),
                    "name": self._name(row.get("name"), f"Line_{idx}"),
                    "from_bus": int(row.from_bus),
                    "to_bus": int(row.to_bus),
                    "base_voltage_id": buses[int(row.from_bus)]["base_voltage_id"],
                    "length_km": float(row.length_km),
                    "r": r1 * float(row.length_km),
                    "x": x1 * float(row.length_km),
                    "r0": r0 * float(row.length_km),
                    "x0": x0 * float(row.length_km),
                    "bch": 2.0 * math.pi * float(getattr(net, "f_hz", 50.0)) * c1 * 1e-9 * float(row.length_km),
                    "b0ch": 2.0 * math.pi * float(getattr(net, "f_hz", 50.0)) * c0 * 1e-9 * float(row.length_km),
                    "gch": g1 * 1e-6 * float(row.length_km),
                    "g0ch": g0 * 1e-6 * float(row.length_km),
                    "max_i_a": max_i_ka * 1000.0,
                    "in_service": bool(row.get("in_service", True)),
                }
            )

        transformers: list[dict[str, object]] = []
        for idx, row in net.trafo.iterrows():
            hv_bus = int(row.hv_bus)
            lv_bus = int(row.lv_bus)
            sn_mva = float(row.sn_mva)
            vn_hv_kv = float(row.vn_hv_kv)
            vn_lv_kv = float(row.vn_lv_kv)
            shift_degree = float(row.get("shift_degree", 0.0))
            vector_group = str(row.get("vector_group", "") or "")
            r_pu = float(row.vkr_percent) / 100.0
            z_pu = float(row.vk_percent) / 100.0
            x_pu = math.sqrt(max(z_pu * z_pu - r_pu * r_pu, 0.0))
            z_base_hv = (vn_hv_kv * vn_hv_kv) / max(sn_mva, 1e-9)
            z_base_lv = (vn_lv_kv * vn_lv_kv) / max(sn_mva, 1e-9)
            r_hv_ohm = 0.5 * r_pu * z_base_hv
            x_hv_ohm = 0.5 * x_pu * z_base_hv
            r_lv_ohm = 0.5 * r_pu * z_base_lv
            x_lv_ohm = 0.5 * x_pu * z_base_lv
            if abs(r_hv_ohm) < 1e-12 and abs(x_hv_ohm) < 1e-12:
                x_hv_ohm = 1e-6
            tap_side = str(row.get("tap_side", "") or "").strip().lower()
            tap_neutral = self._float_or_none(row.get("tap_neutral", None))
            tap_min = self._float_or_none(row.get("tap_min", None))
            tap_max = self._float_or_none(row.get("tap_max", None))
            tap_step_percent = self._float_or_none(row.get("tap_step_percent", None))
            tap_pos = self._float_or_none(row.get("tap_pos", None))
            has_tap_data = (
                tap_side in {"hv", "lv"}
                and tap_neutral is not None
                and tap_min is not None
                and tap_max is not None
                and tap_step_percent is not None
            )
            transformers.append(
                {
                    "rdf_id": self._rdf_id(),
                    "terminal_hv_id": self._rdf_id(),
                    "terminal_lv_id": self._rdf_id(),
                    "end_hv_id": self._rdf_id(),
                    "end_lv_id": self._rdf_id(),
                    "tap_changer_id": self._rdf_id() if has_tap_data else None,
                    "name": self._name(row.get("name"), f"Transformer_{idx}"),
                    "hv_bus": hv_bus,
                    "lv_bus": lv_bus,
                    "in_service": bool(row.get("in_service", True)),
                    "rated_s_mva": sn_mva,
                    "rated_u_hv_kv": vn_hv_kv,
                    "rated_u_lv_kv": vn_lv_kv,
                    "r_hv_ohm": r_hv_ohm,
                    "x_hv_ohm": x_hv_ohm,
                    "r_lv_ohm": r_lv_ohm,
                    "x_lv_ohm": x_lv_ohm,
                    "connection_hv": self._winding_connection_symbol(vector_group, side="hv"),
                    "connection_lv": self._winding_connection_symbol(vector_group, side="lv"),
                    "phase_angle_clock_hv": 0,
                    "phase_angle_clock_lv": int(round(shift_degree / 30.0)) % 12,
                    "tap_side": tap_side,
                    "tap_neutral": int(round(tap_neutral)) if tap_neutral is not None else 0,
                    "tap_min": int(round(tap_min)) if tap_min is not None else 0,
                    "tap_max": int(round(tap_max)) if tap_max is not None else 0,
                    "tap_step_percent": float(tap_step_percent) if tap_step_percent is not None else 0.0,
                    "tap_pos": int(round(tap_pos)) if tap_pos is not None else None,
                    "base_voltage_hv_id": buses[hv_bus]["base_voltage_id"],
                    "base_voltage_lv_id": buses[lv_bus]["base_voltage_id"],
                }
            )

        sources: list[dict[str, object]] = []
        for idx, row in net.ext_grid.iterrows():
            bus = int(row.bus)
            p_res = float(solved_net.res_ext_grid.at[idx, "p_mw"])
            q_res = float(solved_net.res_ext_grid.at[idx, "q_mvar"])
            p_mw = -p_res
            q_mvar = -q_res
            sources.append(
                {
                    "source_kind": "external_network",
                    "external_network_id": self._rdf_id(),
                    "terminal_id": self._rdf_id(),
                    "reg_control_id": self._rdf_id(),
                    "name": self._name(row.get("name"), f"Slack_{idx}"),
                    "bus": bus,
                    "conducting_equipment_id": None,
                    "is_slack": True,
                    "target_vm_pu": float(row.get("vm_pu", 1.0)),
                    "p_mw": p_mw,
                    "q_mvar": q_mvar,
                    "min_p_mw": float(row.get("min_p_mw", min(0.0, p_mw))),
                    "max_p_mw": float(row.get("max_p_mw", max(abs(p_mw) * 1.5, 1.0))),
                    "min_q_mvar": float(row.get("min_q_mvar", min(-abs(q_mvar) * 2.0, q_mvar, -1.0))),
                    "max_q_mvar": float(row.get("max_q_mvar", max(abs(q_mvar) * 2.0, q_mvar, 1.0))),
                    "in_service": bool(row.get("in_service", True)),
                }
            )

        for idx, row in net.gen.iterrows():
            bus = int(row.bus)
            p_mw = float(solved_net.res_gen.at[idx, "p_mw"])
            q_mvar = float(solved_net.res_gen.at[idx, "q_mvar"])
            sources.append(
                {
                    "source_kind": "synchronous_machine",
                    "generating_unit_id": self._rdf_id(),
                    "machine_id": self._rdf_id(),
                    "terminal_id": self._rdf_id(),
                    "reg_control_id": self._rdf_id(),
                    "name": self._name(row.get("name"), f"Generator_{idx}"),
                    "bus": bus,
                    "conducting_equipment_id": None,
                    "is_slack": bool(row.get("slack", False)),
                    "target_vm_pu": float(row.get("vm_pu", solved_net.res_bus.at[bus, "vm_pu"])),
                    "p_mw": p_mw,
                    "q_mvar": q_mvar,
                    "min_p_mw": float(row.get("min_p_mw", min(0.0, p_mw))),
                    "max_p_mw": float(row.get("max_p_mw", max(abs(p_mw) * 1.5, 1.0))),
                    "min_q_mvar": float(row.get("min_q_mvar", min(-abs(q_mvar) * 2.0, q_mvar, -1.0))),
                    "max_q_mvar": float(row.get("max_q_mvar", max(abs(q_mvar) * 2.0, q_mvar, 1.0))),
                    "in_service": bool(row.get("in_service", True)),
                }
            )

        sources_sorted = sorted(sources, key=lambda source: (not bool(source["is_slack"]), str(source["name"])))
        for source in sources_sorted:
            bus_idx = int(source["bus"])
            if source["source_kind"] == "external_network":
                source["conducting_equipment_id"] = source["external_network_id"]
            else:
                source["conducting_equipment_id"] = source["machine_id"]
            source["rated_u_kv"] = float(net.bus.at[bus_idx, "vn_kv"])
            source["target_vm_kv"] = float(net.bus.at[bus_idx, "vn_kv"]) * float(source["target_vm_pu"])
            source["rated_s_mva"] = max(
                abs(float(source["p_mw"])),
                abs(float(source["q_mvar"])),
                abs(float(source["max_p_mw"])),
                abs(float(source["max_q_mvar"])),
                1.0,
            )

        return {
            "case_id": case_id,
            "scenario_time": "2030-01-02T09:00:00",
            "modeling_authority": "https://powermodelconverter.local",
            "description": "Generated by PowerModelConverter for validation-first CGMES interchange.",
            "region_id": region_id,
            "subregion_id": subregion_id,
            "operational_limit_type_id": operational_limit_type_id,
            "base_voltage_ids": base_voltage_ids,
            "buses": buses,
            "loads": loads,
            "lines": lines,
            "transformers": transformers,
            "sources": sources_sorted,
        }

    def _render_eq(self, case_id: str, profile_ids: dict[str, str], assets: dict[str, object]) -> bytes:
        root = self._rdf_root()
        self._full_model(root, profile_ids["eq"], "http://entsoe.eu/CIM/EquipmentCore/3/1", [], assets)
        self._append_base_structure(root, assets)
        self._append_terminals_eq(root, assets)
        self._append_line_eq(root, assets)
        self._append_transformer_eq(root, assets)
        self._append_load_eq(root, assets)
        self._append_sources_eq(root, assets)
        self._append_limit_type_eq(root, assets)
        return self._xml_bytes(root)

    def _render_ssh(self, case_id: str, profile_ids: dict[str, str], assets: dict[str, object]) -> bytes:
        root = self._rdf_root()
        self._full_model(root, profile_ids["ssh"], "http://entsoe.eu/CIM/SteadyStateHypothesis/1/1", [profile_ids["eq"]], assets)
        self._append_terminal_states_ssh(root, assets)
        self._append_load_ssh(root, assets)
        self._append_sources_ssh(root, assets)
        self._append_transformer_ssh(root, assets)
        self._append_line_ssh(root, assets)
        return self._xml_bytes(root)

    def _render_tp(self, case_id: str, profile_ids: dict[str, str], assets: dict[str, object]) -> bytes:
        root = self._rdf_root()
        self._full_model(root, profile_ids["tp"], "http://entsoe.eu/CIM/Topology/4/1", [profile_ids["eq"]], assets)
        self._append_topology_tp(root, assets)
        return self._xml_bytes(root)

    def _render_sv(self, case_id: str, profile_ids: dict[str, str], assets: dict[str, object]) -> bytes:
        root = self._rdf_root()
        self._full_model(
            root,
            profile_ids["sv"],
            "http://entsoe.eu/CIM/StateVariables/4/1",
            [profile_ids["eq"], profile_ids["tp"]],
            assets,
        )
        self._append_state_variables_sv(root, assets)
        return self._xml_bytes(root)

    def _append_base_structure(self, root: ET.Element, assets: dict[str, object]) -> None:
        base_voltage_ids = assets["base_voltage_ids"]
        buses = assets["buses"]
        region = ET.SubElement(root, self._tag("GeographicalRegion"), {self._rdf("ID"): assets["region_id"]})
        ET.SubElement(region, self._tag("IdentifiedObject.name")).text = "PMC Region"
        subregion = ET.SubElement(root, self._tag("SubGeographicalRegion"), {self._rdf("ID"): assets["subregion_id"]})
        ET.SubElement(subregion, self._tag("IdentifiedObject.name")).text = "PMC Subregion"
        ET.SubElement(subregion, self._tag("SubGeographicalRegion.Region"), {self._rdf("resource"): f"#{assets['region_id']}"})

        for kv, rdf_id in base_voltage_ids.items():
            element = ET.SubElement(root, self._tag("BaseVoltage"), {self._rdf("ID"): rdf_id})
            ET.SubElement(element, self._tag("IdentifiedObject.name")).text = f"{kv:.6g}"
            ET.SubElement(element, self._tag("BaseVoltage.nominalVoltage")).text = f"{kv:.12g}"

        for bus in buses.values():
            substation = ET.SubElement(root, self._tag("Substation"), {self._rdf("ID"): bus["substation_id"]})
            ET.SubElement(substation, self._tag("IdentifiedObject.name")).text = f"{bus['name']}_Substation"
            ET.SubElement(substation, self._tag("Substation.Region"), {self._rdf("resource"): f"#{assets['subregion_id']}"})

            voltage_level = ET.SubElement(root, self._tag("VoltageLevel"), {self._rdf("ID"): bus["voltage_level_id"]})
            ET.SubElement(voltage_level, self._tag("IdentifiedObject.name")).text = f"{bus['name']}_VL"
            ET.SubElement(voltage_level, self._tag("VoltageLevel.Substation"), {self._rdf("resource"): f"#{bus['substation_id']}"})
            ET.SubElement(voltage_level, self._tag("VoltageLevel.BaseVoltage"), {self._rdf("resource"): f"#{bus['base_voltage_id']}"})

    def _append_terminals_eq(self, root: ET.Element, assets: dict[str, object]) -> None:
        for line in assets["lines"]:
            self._terminal_eq(root, line["terminal_from_id"], f"{line['name']}_0", line["rdf_id"], 1)
            self._terminal_eq(root, line["terminal_to_id"], f"{line['name']}_1", line["rdf_id"], 2)
        for transformer in assets["transformers"]:
            self._terminal_eq(root, transformer["terminal_hv_id"], f"{transformer['name']}_HV", transformer["rdf_id"], 1)
            self._terminal_eq(root, transformer["terminal_lv_id"], f"{transformer['name']}_LV", transformer["rdf_id"], 2)
        for load in assets["loads"]:
            self._terminal_eq(root, load["terminal_id"], load["name"], load["rdf_id"], 1)
        for source in assets["sources"]:
            self._terminal_eq(root, source["terminal_id"], source["name"], source["conducting_equipment_id"], 1)

    def _append_line_eq(self, root: ET.Element, assets: dict[str, object]) -> None:
        for line in assets["lines"]:
            element = ET.SubElement(root, self._tag("ACLineSegment"), {self._rdf("ID"): line["rdf_id"]})
            ET.SubElement(element, self._tag("IdentifiedObject.name")).text = str(line["name"])
            ET.SubElement(element, self._tag("Equipment.aggregate")).text = "false"
            ET.SubElement(
                element,
                self._tag("Equipment.EquipmentContainer"),
                {self._rdf("resource"): f"#{assets['buses'][line['from_bus']]['voltage_level_id']}"},
            )
            ET.SubElement(
                element,
                self._tag("ConductingEquipment.BaseVoltage"),
                {self._rdf("resource"): f"#{line['base_voltage_id']}"},
            )
            ET.SubElement(element, self._tag("Conductor.length")).text = f"{line['length_km']:.12g}"
            for key in ("b0ch", "bch", "g0ch", "gch", "r", "r0", "x", "x0"):
                ET.SubElement(element, self._tag(f"ACLineSegment.{key}")).text = f"{line[key]:.12g}"
            ET.SubElement(element, self._tag("ACLineSegment.shortCircuitEndTemperature")).text = "80"

            for limit_id, terminal_id, current_limit_id in (
                (line["limit_from_id"], line["terminal_from_id"], line["current_limit_from_id"]),
                (line["limit_to_id"], line["terminal_to_id"], line["current_limit_to_id"]),
            ):
                limit_set = ET.SubElement(root, self._tag("OperationalLimitSet"), {self._rdf("ID"): limit_id})
                ET.SubElement(limit_set, self._tag("IdentifiedObject.name")).text = "Ratings"
                ET.SubElement(limit_set, self._tag("OperationalLimitSet.Terminal"), {self._rdf("resource"): f"#{terminal_id}"})
                current_limit = ET.SubElement(root, self._tag("CurrentLimit"), {self._rdf("ID"): current_limit_id})
                ET.SubElement(current_limit, self._tag("IdentifiedObject.name")).text = f"{line['name']}_limit"
                ET.SubElement(current_limit, self._tag("CurrentLimit.value")).text = f"{line['max_i_a']:.12g}"
                ET.SubElement(current_limit, self._tag("OperationalLimit.OperationalLimitSet"), {self._rdf("resource"): f"#{limit_id}"})
                ET.SubElement(current_limit, self._tag("OperationalLimit.OperationalLimitType"), {self._rdf("resource"): f"#{assets['operational_limit_type_id']}"})

    def _append_transformer_eq(self, root: ET.Element, assets: dict[str, object]) -> None:
        for transformer in assets["transformers"]:
            element = ET.SubElement(root, self._tag("PowerTransformer"), {self._rdf("ID"): transformer["rdf_id"]})
            ET.SubElement(element, self._tag("IdentifiedObject.name")).text = str(transformer["name"])
            ET.SubElement(element, self._tag("Equipment.aggregate")).text = "false"
            ET.SubElement(
                element,
                self._tag("Equipment.EquipmentContainer"),
                {self._rdf("resource"): f"#{assets['buses'][transformer['hv_bus']]['voltage_level_id']}"},
            )

            hv_end = ET.SubElement(root, self._tag("PowerTransformerEnd"), {self._rdf("ID"): transformer["end_hv_id"]})
            ET.SubElement(hv_end, self._tag("IdentifiedObject.name")).text = f"{transformer['name']}_HV"
            ET.SubElement(hv_end, self._tag("TransformerEnd.endNumber")).text = "1"
            ET.SubElement(hv_end, self._tag("PowerTransformerEnd.PowerTransformer"), {self._rdf("resource"): f"#{transformer['rdf_id']}"})
            ET.SubElement(hv_end, self._tag("PowerTransformerEnd.Terminal"), {self._rdf("resource"): f"#{transformer['terminal_hv_id']}"})
            ET.SubElement(hv_end, self._tag("PowerTransformerEnd.ratedS")).text = f"{transformer['rated_s_mva']:.12g}"
            ET.SubElement(hv_end, self._tag("PowerTransformerEnd.ratedU")).text = f"{transformer['rated_u_hv_kv']:.12g}"
            ET.SubElement(hv_end, self._tag("PowerTransformerEnd.r")).text = f"{transformer['r_hv_ohm']:.12g}"
            ET.SubElement(hv_end, self._tag("PowerTransformerEnd.x")).text = f"{transformer['x_hv_ohm']:.12g}"
            ET.SubElement(hv_end, self._tag("PowerTransformerEnd.g")).text = "0"
            ET.SubElement(hv_end, self._tag("PowerTransformerEnd.b")).text = "0"
            ET.SubElement(hv_end, self._tag("TransformerEnd.BaseVoltage"), {self._rdf("resource"): f"#{transformer['base_voltage_hv_id']}"})
            ET.SubElement(hv_end, self._tag("PowerTransformerEnd.phaseAngleClock")).text = str(int(transformer["phase_angle_clock_hv"]))
            ET.SubElement(
                hv_end,
                self._tag("PowerTransformerEnd.connectionKind"),
                {self._rdf("resource"): f"http://iec.ch/TC57/2013/CIM-schema-cim16#WindingConnection.{transformer['connection_hv']}"},
            )

            lv_end = ET.SubElement(root, self._tag("PowerTransformerEnd"), {self._rdf("ID"): transformer["end_lv_id"]})
            ET.SubElement(lv_end, self._tag("IdentifiedObject.name")).text = f"{transformer['name']}_LV"
            ET.SubElement(lv_end, self._tag("TransformerEnd.endNumber")).text = "2"
            ET.SubElement(lv_end, self._tag("PowerTransformerEnd.PowerTransformer"), {self._rdf("resource"): f"#{transformer['rdf_id']}"})
            ET.SubElement(lv_end, self._tag("PowerTransformerEnd.Terminal"), {self._rdf("resource"): f"#{transformer['terminal_lv_id']}"})
            ET.SubElement(lv_end, self._tag("PowerTransformerEnd.ratedS")).text = f"{transformer['rated_s_mva']:.12g}"
            ET.SubElement(lv_end, self._tag("PowerTransformerEnd.ratedU")).text = f"{transformer['rated_u_lv_kv']:.12g}"
            ET.SubElement(lv_end, self._tag("PowerTransformerEnd.r")).text = f"{transformer['r_lv_ohm']:.12g}"
            ET.SubElement(lv_end, self._tag("PowerTransformerEnd.x")).text = f"{transformer['x_lv_ohm']:.12g}"
            ET.SubElement(lv_end, self._tag("PowerTransformerEnd.g")).text = "0"
            ET.SubElement(lv_end, self._tag("PowerTransformerEnd.b")).text = "0"
            ET.SubElement(lv_end, self._tag("TransformerEnd.BaseVoltage"), {self._rdf("resource"): f"#{transformer['base_voltage_lv_id']}"})
            ET.SubElement(lv_end, self._tag("PowerTransformerEnd.phaseAngleClock")).text = str(int(transformer["phase_angle_clock_lv"]))
            ET.SubElement(
                lv_end,
                self._tag("PowerTransformerEnd.connectionKind"),
                {self._rdf("resource"): f"http://iec.ch/TC57/2013/CIM-schema-cim16#WindingConnection.{transformer['connection_lv']}"},
            )

            tap_changer_id = transformer.get("tap_changer_id")
            if tap_changer_id:
                tap_end_id = transformer["end_hv_id"] if transformer.get("tap_side") == "hv" else transformer["end_lv_id"]
                tap = ET.SubElement(root, self._tag("RatioTapChanger"), {self._rdf("ID"): tap_changer_id})
                ET.SubElement(tap, self._tag("IdentifiedObject.name")).text = f"{transformer['name']}_tap"
                ET.SubElement(tap, self._tag("TapChanger.lowStep")).text = str(int(transformer["tap_min"]))
                ET.SubElement(tap, self._tag("TapChanger.highStep")).text = str(int(transformer["tap_max"]))
                ET.SubElement(tap, self._tag("TapChanger.neutralStep")).text = str(int(transformer["tap_neutral"]))
                ET.SubElement(tap, self._tag("TapChanger.normalStep")).text = str(int(transformer["tap_neutral"]))
                ET.SubElement(tap, self._tag("RatioTapChanger.stepVoltageIncrement")).text = f"{float(transformer['tap_step_percent']):.12g}"
                ET.SubElement(tap, self._tag("RatioTapChanger.TransformerEnd"), {self._rdf("resource"): f"#{tap_end_id}"})

    def _append_load_eq(self, root: ET.Element, assets: dict[str, object]) -> None:
        for load in assets["loads"]:
            element = ET.SubElement(root, self._tag("EnergyConsumer"), {self._rdf("ID"): load["rdf_id"]})
            ET.SubElement(element, self._tag("IdentifiedObject.name")).text = str(load["name"])
            ET.SubElement(element, self._tag("Equipment.aggregate")).text = "false"
            ET.SubElement(
                element,
                self._tag("Equipment.EquipmentContainer"),
                {self._rdf("resource"): f"#{assets['buses'][load['bus']]['voltage_level_id']}"},
            )
            ET.SubElement(element, self._tag("EnergyConsumer.LoadResponse"), {self._rdf("resource"): f"#{load['response_id']}"})

            response = ET.SubElement(root, self._tag("LoadResponseCharacteristic"), {self._rdf("ID"): load["response_id"]})
            ET.SubElement(response, self._tag("IdentifiedObject.name")).text = str(load["name"])
            ET.SubElement(response, self._tag("LoadResponseCharacteristic.exponentModel")).text = "false"
            for field, value in (
                ("pConstantCurrent", "0"),
                ("pConstantImpedance", "0"),
                ("pConstantPower", "1"),
                ("pVoltageExponent", "0"),
                ("qConstantCurrent", "0"),
                ("qConstantImpedance", "0"),
                ("qConstantPower", "1"),
                ("qVoltageExponent", "0"),
            ):
                ET.SubElement(response, self._tag(f"LoadResponseCharacteristic.{field}")).text = value

    def _append_sources_eq(self, root: ET.Element, assets: dict[str, object]) -> None:
        for source in assets["sources"]:
            if source["source_kind"] == "external_network":
                external = ET.SubElement(root, self._tag("ExternalNetworkInjection"), {self._rdf("ID"): source["external_network_id"]})
                ET.SubElement(external, self._tag("IdentifiedObject.name")).text = str(source["name"])
                ET.SubElement(external, self._tag("Equipment.aggregate")).text = "false"
                ET.SubElement(
                    external,
                    self._tag("Equipment.EquipmentContainer"),
                    {self._rdf("resource"): f"#{assets['buses'][source['bus']]['voltage_level_id']}"},
                )
                ET.SubElement(
                    external,
                    self._tag("ConductingEquipment.BaseVoltage"),
                    {self._rdf("resource"): f"#{assets['buses'][source['bus']]['base_voltage_id']}"},
                )
                ET.SubElement(external, self._tag("RegulatingCondEq.RegulatingControl"), {self._rdf("resource"): f"#{source['reg_control_id']}"})
                ET.SubElement(external, self._tag("ExternalNetworkInjection.minP")).text = f"{float(source['min_p_mw']):.12g}"
                ET.SubElement(external, self._tag("ExternalNetworkInjection.maxP")).text = f"{float(source['max_p_mw']):.12g}"
                ET.SubElement(external, self._tag("ExternalNetworkInjection.minQ")).text = f"{float(source['min_q_mvar']):.12g}"
                ET.SubElement(external, self._tag("ExternalNetworkInjection.maxQ")).text = f"{float(source['max_q_mvar']):.12g}"

                reg = ET.SubElement(root, self._tag("RegulatingControl"), {self._rdf("ID"): source["reg_control_id"]})
                ET.SubElement(reg, self._tag("IdentifiedObject.name")).text = str(source["name"])
                ET.SubElement(
                    reg,
                    self._tag("RegulatingControl.mode"),
                    {self._rdf("resource"): "http://iec.ch/TC57/2013/CIM-schema-cim16#RegulatingControlModeKind.voltage"},
                )
                ET.SubElement(reg, self._tag("RegulatingControl.Terminal"), {self._rdf("resource"): f"#{source['terminal_id']}"})
                continue

            generating_unit = ET.SubElement(root, self._tag("ThermalGeneratingUnit"), {self._rdf("ID"): source["generating_unit_id"]})
            ET.SubElement(generating_unit, self._tag("IdentifiedObject.name")).text = str(source["name"])
            ET.SubElement(generating_unit, self._tag("Equipment.aggregate")).text = "false"
            ET.SubElement(
                generating_unit,
                self._tag("Equipment.EquipmentContainer"),
                {self._rdf("resource"): f"#{assets['buses'][source['bus']]['voltage_level_id']}"},
            )
            ET.SubElement(generating_unit, self._tag("GeneratingUnit.initialP")).text = f"{max(float(source['p_mw']), 0.0):.12g}"
            ET.SubElement(generating_unit, self._tag("GeneratingUnit.maxOperatingP")).text = f"{float(source['max_p_mw']):.12g}"
            ET.SubElement(generating_unit, self._tag("GeneratingUnit.minOperatingP")).text = f"{float(source['min_p_mw']):.12g}"

            machine = ET.SubElement(root, self._tag("SynchronousMachine"), {self._rdf("ID"): source["machine_id"]})
            ET.SubElement(machine, self._tag("IdentifiedObject.name")).text = str(source["name"])
            ET.SubElement(
                machine,
                self._tag("Equipment.EquipmentContainer"),
                {self._rdf("resource"): f"#{assets['buses'][source['bus']]['voltage_level_id']}"},
            )
            ET.SubElement(machine, self._tag("RegulatingCondEq.RegulatingControl"), {self._rdf("resource"): f"#{source['reg_control_id']}"})
            ET.SubElement(machine, self._tag("RotatingMachine.ratedPowerFactor")).text = "1"
            ET.SubElement(machine, self._tag("RotatingMachine.ratedS")).text = f"{float(source['rated_s_mva']):.12g}"
            ET.SubElement(machine, self._tag("RotatingMachine.ratedU")).text = f"{float(source['rated_u_kv']):.12g}"
            ET.SubElement(machine, self._tag("RotatingMachine.GeneratingUnit"), {self._rdf("resource"): f"#{source['generating_unit_id']}"})
            ET.SubElement(machine, self._tag("SynchronousMachine.earthing")).text = "false"
            ET.SubElement(machine, self._tag("SynchronousMachine.maxQ")).text = f"{float(source['max_q_mvar']):.12g}"
            ET.SubElement(machine, self._tag("SynchronousMachine.minQ")).text = f"{float(source['min_q_mvar']):.12g}"
            ET.SubElement(machine, self._tag("SynchronousMachine.qPercent")).text = "100"
            for field in ("r0", "r2", "satDirectSubtransX", "r", "x0", "x2"):
                ET.SubElement(machine, self._tag(f"SynchronousMachine.{field}")).text = "0"
            ET.SubElement(
                machine,
                self._tag("SynchronousMachine.type"),
                {self._rdf("resource"): "http://iec.ch/TC57/2013/CIM-schema-cim16#SynchronousMachineKind.generator"},
            )

            reg = ET.SubElement(root, self._tag("RegulatingControl"), {self._rdf("ID"): source["reg_control_id"]})
            ET.SubElement(reg, self._tag("IdentifiedObject.name")).text = str(source["name"])
            ET.SubElement(
                reg,
                self._tag("RegulatingControl.mode"),
                {self._rdf("resource"): "http://iec.ch/TC57/2013/CIM-schema-cim16#RegulatingControlModeKind.voltage"},
            )
            ET.SubElement(reg, self._tag("RegulatingControl.Terminal"), {self._rdf("resource"): f"#{source['terminal_id']}"})

    def _append_limit_type_eq(self, root: ET.Element, assets: dict[str, object]) -> None:
        limit_type = ET.SubElement(root, self._tag("OperationalLimitType"), {self._rdf("ID"): assets["operational_limit_type_id"]})
        ET.SubElement(limit_type, self._tag("IdentifiedObject.name")).text = "PATL"
        ET.SubElement(limit_type, self._tag("OperationalLimitType.acceptableDuration")).text = "45000"
        ET.SubElement(limit_type, self._tag("OperationalLimitType.direction"), {self._rdf("resource"): "http://iec.ch/TC57/2013/CIM-schema-cim16#OperationalLimitDirectionKind.absoluteValue"})
        ET.SubElement(limit_type, self._tag("{http://entsoe.eu/CIM/SchemaExtension/3/1#}OperationalLimitType.limitType"), {self._rdf("resource"): "http://entsoe.eu/CIM/SchemaExtension/3/1#LimitTypeKind.patl"})

    def _append_terminal_states_ssh(self, root: ET.Element, assets: dict[str, object]) -> None:
        terminal_ids = [source["terminal_id"] for source in assets["sources"]]
        terminal_ids.extend(load["terminal_id"] for load in assets["loads"])
        for line in assets["lines"]:
            terminal_ids.extend([line["terminal_from_id"], line["terminal_to_id"]])
        for transformer in assets["transformers"]:
            terminal_ids.extend([transformer["terminal_hv_id"], transformer["terminal_lv_id"]])
        for terminal_id in terminal_ids:
            terminal = ET.SubElement(root, self._tag("Terminal"), {self._rdf("about"): f"#{terminal_id}"})
            ET.SubElement(terminal, self._tag("ACDCTerminal.connected")).text = "true"

    def _append_load_ssh(self, root: ET.Element, assets: dict[str, object]) -> None:
        for load in assets["loads"]:
            element = ET.SubElement(root, self._tag("EnergyConsumer"), {self._rdf("about"): f"#{load['rdf_id']}"})
            ET.SubElement(element, self._tag("EnergyConsumer.p")).text = f"{load['p_mw']:.12g}"
            ET.SubElement(element, self._tag("EnergyConsumer.q")).text = f"{load['q_mvar']:.12g}"

    def _append_sources_ssh(self, root: ET.Element, assets: dict[str, object]) -> None:
        for idx, source in enumerate(assets["sources"]):
            if source["source_kind"] == "external_network":
                external = ET.SubElement(root, self._tag("ExternalNetworkInjection"), {self._rdf("about"): f"#{source['external_network_id']}"})
                ET.SubElement(external, self._tag("RegulatingCondEq.controlEnabled")).text = "true"
                ET.SubElement(external, self._tag("ExternalNetworkInjection.p")).text = f"{float(source['p_mw']):.12g}"
                ET.SubElement(external, self._tag("ExternalNetworkInjection.q")).text = f"{float(source['q_mvar']):.12g}"
                ET.SubElement(external, self._tag("ExternalNetworkInjection.referencePriority")).text = "1" if bool(source["is_slack"]) else str(100 + idx)
                reg = ET.SubElement(root, self._tag("RegulatingControl"), {self._rdf("about"): f"#{source['reg_control_id']}"})
                ET.SubElement(reg, self._tag("RegulatingControl.discrete")).text = "false"
                ET.SubElement(reg, self._tag("RegulatingControl.enabled")).text = "true"
                ET.SubElement(reg, self._tag("RegulatingControl.targetDeadband")).text = "0"
                ET.SubElement(reg, self._tag("RegulatingControl.targetValue")).text = f"{float(source['target_vm_kv']):.12g}"
                ET.SubElement(reg, self._tag("RegulatingControl.targetValueUnitMultiplier"), {self._rdf("resource"): "http://iec.ch/TC57/2013/CIM-schema-cim16#UnitMultiplier.k"})
                continue

            unit = ET.SubElement(root, self._tag("ThermalGeneratingUnit"), {self._rdf("about"): f"#{source['generating_unit_id']}"})
            ET.SubElement(unit, self._tag("GeneratingUnit.normalPF")).text = "0"
            machine = ET.SubElement(root, self._tag("SynchronousMachine"), {self._rdf("about"): f"#{source['machine_id']}"})
            ET.SubElement(machine, self._tag("RegulatingCondEq.controlEnabled")).text = "true"
            ET.SubElement(machine, self._tag("RotatingMachine.p")).text = f"{float(source['p_mw']):.12g}"
            ET.SubElement(machine, self._tag("RotatingMachine.q")).text = f"{float(source['q_mvar']):.12g}"
            ET.SubElement(machine, self._tag("SynchronousMachine.referencePriority")).text = "1" if bool(source["is_slack"]) else str(100 + idx)
            ET.SubElement(machine, self._tag("SynchronousMachine.operatingMode"), {self._rdf("resource"): "http://iec.ch/TC57/2013/CIM-schema-cim16#SynchronousMachineOperatingMode.generator"})
            reg = ET.SubElement(root, self._tag("RegulatingControl"), {self._rdf("about"): f"#{source['reg_control_id']}"})
            ET.SubElement(reg, self._tag("RegulatingControl.discrete")).text = "false"
            ET.SubElement(reg, self._tag("RegulatingControl.enabled")).text = "true"
            ET.SubElement(reg, self._tag("RegulatingControl.targetDeadband")).text = "0"
            ET.SubElement(reg, self._tag("RegulatingControl.targetValue")).text = f"{float(source['target_vm_kv']):.12g}"
            ET.SubElement(reg, self._tag("RegulatingControl.targetValueUnitMultiplier"), {self._rdf("resource"): "http://iec.ch/TC57/2013/CIM-schema-cim16#UnitMultiplier.k"})

    def _append_transformer_ssh(self, root: ET.Element, assets: dict[str, object]) -> None:
        for transformer in assets["transformers"]:
            tap_changer_id = transformer.get("tap_changer_id")
            if not tap_changer_id:
                continue
            tap = ET.SubElement(root, self._tag("RatioTapChanger"), {self._rdf("about"): f"#{tap_changer_id}"})
            ET.SubElement(tap, self._tag("TapChanger.controlEnabled")).text = "false"
            tap_step = transformer.get("tap_pos")
            if tap_step is None:
                tap_step = transformer["tap_neutral"]
            ET.SubElement(tap, self._tag("TapChanger.step")).text = str(int(tap_step))

    def _append_line_ssh(self, root: ET.Element, assets: dict[str, object]) -> None:
        for line in assets["lines"]:
            if line["in_service"]:
                continue
            for terminal_id in (line["terminal_from_id"], line["terminal_to_id"]):
                terminal = ET.SubElement(root, self._tag("Terminal"), {self._rdf("about"): f"#{terminal_id}"})
                ET.SubElement(terminal, self._tag("ACDCTerminal.connected")).text = "false"
        for transformer in assets["transformers"]:
            if transformer["in_service"]:
                continue
            for terminal_id in (transformer["terminal_hv_id"], transformer["terminal_lv_id"]):
                terminal = ET.SubElement(root, self._tag("Terminal"), {self._rdf("about"): f"#{terminal_id}"})
                ET.SubElement(terminal, self._tag("ACDCTerminal.connected")).text = "false"
        for source in assets["sources"]:
            if source["in_service"]:
                continue
            terminal = ET.SubElement(root, self._tag("Terminal"), {self._rdf("about"): f"#{source['terminal_id']}"})
            ET.SubElement(terminal, self._tag("ACDCTerminal.connected")).text = "false"

    def _append_topology_tp(self, root: ET.Element, assets: dict[str, object]) -> None:
        for bus_index, bus in assets["buses"].items():
            node = ET.SubElement(root, self._tag("TopologicalNode"), {self._rdf("ID"): bus["rdf_id"]})
            ET.SubElement(node, self._tag("IdentifiedObject.name")).text = str(bus["name"])
            ET.SubElement(node, self._tag("TopologicalNode.ConnectivityNodeContainer"), {self._rdf("resource"): f"#{bus['voltage_level_id']}"})
            ET.SubElement(node, self._tag("TopologicalNode.BaseVoltage"), {self._rdf("resource"): f"#{bus['base_voltage_id']}"})

        for line in assets["lines"]:
            self._terminal_tp(root, line["terminal_from_id"], assets["buses"][line["from_bus"]]["rdf_id"])
            self._terminal_tp(root, line["terminal_to_id"], assets["buses"][line["to_bus"]]["rdf_id"])
        for transformer in assets["transformers"]:
            self._terminal_tp(root, transformer["terminal_hv_id"], assets["buses"][transformer["hv_bus"]]["rdf_id"])
            self._terminal_tp(root, transformer["terminal_lv_id"], assets["buses"][transformer["lv_bus"]]["rdf_id"])
        for load in assets["loads"]:
            self._terminal_tp(root, load["terminal_id"], assets["buses"][load["bus"]]["rdf_id"])
        for source in assets["sources"]:
            self._terminal_tp(root, source["terminal_id"], assets["buses"][source["bus"]]["rdf_id"])

    def _append_state_variables_sv(self, root: ET.Element, assets: dict[str, object]) -> None:
        for bus in assets["buses"].values():
            sv = ET.SubElement(root, self._tag("SvVoltage"), {self._rdf("ID"): self._rdf_id()})
            ET.SubElement(sv, self._tag("SvVoltage.angle")).text = f"{bus['va_degree']:.12g}"
            ET.SubElement(sv, self._tag("SvVoltage.v")).text = f"{bus['vm_pu'] * bus['vn_kv']:.12g}"
            ET.SubElement(sv, self._tag("SvVoltage.TopologicalNode"), {self._rdf("resource"): f"#{bus['rdf_id']}"})

    def _full_model(
        self,
        root: ET.Element,
        model_id: str,
        profile_uri: str,
        dependencies: list[str],
        assets: dict[str, object],
    ) -> None:
        full_model = ET.SubElement(root, self._tag("{http://iec.ch/TC57/61970-552/ModelDescription/1#}FullModel"), {self._rdf("about"): model_id})
        ET.SubElement(full_model, self._tag("{http://iec.ch/TC57/61970-552/ModelDescription/1#}Model.scenarioTime")).text = str(assets["scenario_time"])
        ET.SubElement(full_model, self._tag("{http://iec.ch/TC57/61970-552/ModelDescription/1#}Model.created")).text = "2026-03-26T00:00:00Z"
        ET.SubElement(full_model, self._tag("{http://iec.ch/TC57/61970-552/ModelDescription/1#}Model.description")).text = str(assets["description"])
        ET.SubElement(full_model, self._tag("{http://iec.ch/TC57/61970-552/ModelDescription/1#}Model.version")).text = "1"
        ET.SubElement(full_model, self._tag("{http://iec.ch/TC57/61970-552/ModelDescription/1#}Model.profile")).text = profile_uri
        ET.SubElement(full_model, self._tag("{http://iec.ch/TC57/61970-552/ModelDescription/1#}Model.modelingAuthoritySet")).text = str(assets["modeling_authority"])
        for dependency in dependencies:
            ET.SubElement(full_model, self._tag("{http://iec.ch/TC57/61970-552/ModelDescription/1#}Model.DependentOn"), {self._rdf("resource"): dependency})

    def _terminal_eq(self, root: ET.Element, terminal_id: str, name: str, equipment_id: str, sequence_number: int) -> None:
        terminal = ET.SubElement(root, self._tag("Terminal"), {self._rdf("ID"): terminal_id})
        ET.SubElement(terminal, self._tag("IdentifiedObject.name")).text = str(name)
        ET.SubElement(terminal, self._tag("ACDCTerminal.sequenceNumber")).text = str(sequence_number)
        ET.SubElement(terminal, self._tag("Terminal.phases"), {self._rdf("resource"): "http://iec.ch/TC57/2013/CIM-schema-cim16#PhaseCode.ABC"})
        ET.SubElement(terminal, self._tag("Terminal.ConductingEquipment"), {self._rdf("resource"): f"#{equipment_id}"})

    def _terminal_tp(self, root: ET.Element, terminal_id: str, topological_node_id: str) -> None:
        terminal = ET.SubElement(root, self._tag("Terminal"), {self._rdf("about"): f"#{terminal_id}"})
        ET.SubElement(terminal, self._tag("Terminal.TopologicalNode"), {self._rdf("resource"): f"#{topological_node_id}"})

    def _rdf_root(self) -> ET.Element:
        return ET.Element(self._tag("{http://www.w3.org/1999/02/22-rdf-syntax-ns#}RDF"))

    def _xml_bytes(self, root: ET.Element) -> bytes:
        return ET.tostring(root, encoding="utf-8", xml_declaration=True)

    def _line_param(self, row: object, field: str, fallback: float) -> float:
        value = row.get(field, fallback)
        if value is None:
            return fallback
        numeric = float(value)
        if math.isnan(numeric):
            return fallback
        return numeric

    def _float_or_none(self, value: object) -> float | None:
        if value is None:
            return None
        numeric = float(value)
        if math.isnan(numeric):
            return None
        return numeric

    def _winding_connection_symbol(self, vector_group: str, side: str) -> str:
        letters = "".join(ch for ch in vector_group if ch.isalpha())
        if not letters:
            return "Y"
        token = letters[0] if side == "hv" else letters[-1]
        mapped = token.upper()
        if mapped == "N":
            mapped = "Y"
        if mapped not in {"Y", "D", "Z"}:
            return "Y"
        return mapped

    def _tag(self, name: str) -> str:
        if name.startswith("{"):
            return name
        return f"{{{CIM_NS}}}{name}"

    def _rdf(self, name: str) -> str:
        return f"{{{RDF_NS}}}{name}"

    def _rdf_id(self) -> str:
        return f"_{uuid.uuid4()}"

    def _urn(self) -> str:
        return f"urn:uuid:{uuid.uuid4()}"

    def _name(self, value: object, fallback: str) -> str:
        text = "" if value is None else str(value).strip()
        return text or fallback

    def _bus_name(self, value: object, fallback_idx: int) -> str:
        return self._name(value, f"BUS_{fallback_idx}")
