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
        slack_gen_count = int(gen_table.get("slack", False).fillna(False).sum()) if len(gen_table) else 0
        if not ((ext_grid_count == 1 and len(gen_table) == 0) or (ext_grid_count == 0 and slack_gen_count == 1 and len(gen_table) == 1)):
            raise ValueError("CGMES export currently supports either one ext_grid or one slack generator.")
        unsupported_tables = {
            "sgen": "static generators",
            "trafo": "transformers",
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

        slack_source = self._slack_source(net, solved_net)
        slack_bus = slack_source["bus"]
        p_mw = slack_source["p_mw"]
        q_mvar = slack_source["q_mvar"]
        min_p = slack_source["min_p_mw"]
        max_p = slack_source["max_p_mw"]
        min_q = slack_source["min_q_mvar"]
        max_q = slack_source["max_q_mvar"]
        slack = {
            "generating_unit_id": self._rdf_id(),
            "machine_id": self._rdf_id(),
            "terminal_id": self._rdf_id(),
            "reg_control_id": self._rdf_id(),
            "name": slack_source["name"],
            "bus": slack_bus,
            "rated_u_kv": float(net.bus.at[slack_bus, "vn_kv"]),
            "target_vm_kv": float(net.bus.at[slack_bus, "vn_kv"]) * slack_source["target_vm_pu"],
            "p_mw": p_mw,
            "q_mvar": q_mvar,
            "min_p_mw": min_p,
            "max_p_mw": max_p,
            "min_q_mvar": min_q,
            "max_q_mvar": max_q,
            "rated_s_mva": max(abs(p_mw), abs(q_mvar), abs(max_p), abs(max_q), 1.0),
        }

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
            "slack": slack,
        }

    def _render_eq(self, case_id: str, profile_ids: dict[str, str], assets: dict[str, object]) -> bytes:
        root = self._rdf_root()
        self._full_model(root, profile_ids["eq"], "http://entsoe.eu/CIM/EquipmentCore/3/1", [], assets)
        self._append_base_structure(root, assets)
        self._append_terminals_eq(root, assets)
        self._append_line_eq(root, assets)
        self._append_load_eq(root, assets)
        self._append_slack_eq(root, assets)
        self._append_limit_type_eq(root, assets)
        return self._xml_bytes(root)

    def _render_ssh(self, case_id: str, profile_ids: dict[str, str], assets: dict[str, object]) -> bytes:
        root = self._rdf_root()
        self._full_model(root, profile_ids["ssh"], "http://entsoe.eu/CIM/SteadyStateHypothesis/1/1", [profile_ids["eq"]], assets)
        self._append_terminal_states_ssh(root, assets)
        self._append_load_ssh(root, assets)
        self._append_slack_ssh(root, assets)
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
        for load in assets["loads"]:
            self._terminal_eq(root, load["terminal_id"], load["name"], load["rdf_id"], 1)
        slack = assets["slack"]
        self._terminal_eq(root, slack["terminal_id"], slack["name"], slack["machine_id"], 1)

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

    def _append_slack_eq(self, root: ET.Element, assets: dict[str, object]) -> None:
        slack = assets["slack"]
        generating_unit = ET.SubElement(root, self._tag("ThermalGeneratingUnit"), {self._rdf("ID"): slack["generating_unit_id"]})
        ET.SubElement(generating_unit, self._tag("IdentifiedObject.name")).text = str(slack["name"])
        ET.SubElement(generating_unit, self._tag("Equipment.aggregate")).text = "false"
        ET.SubElement(
            generating_unit,
            self._tag("Equipment.EquipmentContainer"),
            {self._rdf("resource"): f"#{assets['buses'][slack['bus']]['voltage_level_id']}"},
        )
        ET.SubElement(generating_unit, self._tag("GeneratingUnit.initialP")).text = f"{max(slack['p_mw'], 0.0):.12g}"
        ET.SubElement(generating_unit, self._tag("GeneratingUnit.maxOperatingP")).text = f"{slack['max_p_mw']:.12g}"
        ET.SubElement(generating_unit, self._tag("GeneratingUnit.minOperatingP")).text = f"{slack['min_p_mw']:.12g}"

        machine = ET.SubElement(root, self._tag("SynchronousMachine"), {self._rdf("ID"): slack["machine_id"]})
        ET.SubElement(machine, self._tag("IdentifiedObject.name")).text = str(slack["name"])
        ET.SubElement(
            machine,
            self._tag("Equipment.EquipmentContainer"),
            {self._rdf("resource"): f"#{assets['buses'][slack['bus']]['voltage_level_id']}"},
        )
        ET.SubElement(machine, self._tag("RegulatingCondEq.RegulatingControl"), {self._rdf("resource"): f"#{slack['reg_control_id']}"})
        ET.SubElement(machine, self._tag("RotatingMachine.ratedPowerFactor")).text = "1"
        ET.SubElement(machine, self._tag("RotatingMachine.ratedS")).text = f"{slack['rated_s_mva']:.12g}"
        ET.SubElement(machine, self._tag("RotatingMachine.ratedU")).text = f"{slack['rated_u_kv']:.12g}"
        ET.SubElement(machine, self._tag("RotatingMachine.GeneratingUnit"), {self._rdf("resource"): f"#{slack['generating_unit_id']}"})
        ET.SubElement(machine, self._tag("SynchronousMachine.earthing")).text = "false"
        ET.SubElement(machine, self._tag("SynchronousMachine.maxQ")).text = f"{slack['max_q_mvar']:.12g}"
        ET.SubElement(machine, self._tag("SynchronousMachine.minQ")).text = f"{slack['min_q_mvar']:.12g}"
        ET.SubElement(machine, self._tag("SynchronousMachine.qPercent")).text = "100"
        for field in ("r0", "r2", "satDirectSubtransX", "r", "x0", "x2"):
            ET.SubElement(machine, self._tag(f"SynchronousMachine.{field}")).text = "0"
        ET.SubElement(
            machine,
            self._tag("SynchronousMachine.type"),
            {self._rdf("resource"): "http://iec.ch/TC57/2013/CIM-schema-cim16#SynchronousMachineKind.generator"},
        )

        reg = ET.SubElement(root, self._tag("RegulatingControl"), {self._rdf("ID"): slack["reg_control_id"]})
        ET.SubElement(reg, self._tag("IdentifiedObject.name")).text = str(slack["name"])
        ET.SubElement(
            reg,
            self._tag("RegulatingControl.mode"),
            {self._rdf("resource"): "http://iec.ch/TC57/2013/CIM-schema-cim16#RegulatingControlModeKind.voltage"},
        )
        ET.SubElement(reg, self._tag("RegulatingControl.Terminal"), {self._rdf("resource"): f"#{slack['terminal_id']}"})

    def _append_limit_type_eq(self, root: ET.Element, assets: dict[str, object]) -> None:
        limit_type = ET.SubElement(root, self._tag("OperationalLimitType"), {self._rdf("ID"): assets["operational_limit_type_id"]})
        ET.SubElement(limit_type, self._tag("IdentifiedObject.name")).text = "PATL"
        ET.SubElement(limit_type, self._tag("OperationalLimitType.acceptableDuration")).text = "45000"
        ET.SubElement(limit_type, self._tag("OperationalLimitType.direction"), {self._rdf("resource"): "http://iec.ch/TC57/2013/CIM-schema-cim16#OperationalLimitDirectionKind.absoluteValue"})
        ET.SubElement(limit_type, self._tag("{http://entsoe.eu/CIM/SchemaExtension/3/1#}OperationalLimitType.limitType"), {self._rdf("resource"): "http://entsoe.eu/CIM/SchemaExtension/3/1#LimitTypeKind.patl"})

    def _append_terminal_states_ssh(self, root: ET.Element, assets: dict[str, object]) -> None:
        terminal_ids = [assets["slack"]["terminal_id"]]
        terminal_ids.extend(load["terminal_id"] for load in assets["loads"])
        for line in assets["lines"]:
            terminal_ids.extend([line["terminal_from_id"], line["terminal_to_id"]])
        for terminal_id in terminal_ids:
            terminal = ET.SubElement(root, self._tag("Terminal"), {self._rdf("about"): f"#{terminal_id}"})
            ET.SubElement(terminal, self._tag("ACDCTerminal.connected")).text = "true"

    def _append_load_ssh(self, root: ET.Element, assets: dict[str, object]) -> None:
        for load in assets["loads"]:
            element = ET.SubElement(root, self._tag("EnergyConsumer"), {self._rdf("about"): f"#{load['rdf_id']}"})
            ET.SubElement(element, self._tag("EnergyConsumer.p")).text = f"{load['p_mw']:.12g}"
            ET.SubElement(element, self._tag("EnergyConsumer.q")).text = f"{load['q_mvar']:.12g}"

    def _append_slack_ssh(self, root: ET.Element, assets: dict[str, object]) -> None:
        slack = assets["slack"]
        unit = ET.SubElement(root, self._tag("ThermalGeneratingUnit"), {self._rdf("about"): f"#{slack['generating_unit_id']}"})
        ET.SubElement(unit, self._tag("GeneratingUnit.normalPF")).text = "0"
        machine = ET.SubElement(root, self._tag("SynchronousMachine"), {self._rdf("about"): f"#{slack['machine_id']}"})
        ET.SubElement(machine, self._tag("RegulatingCondEq.controlEnabled")).text = "true"
        ET.SubElement(machine, self._tag("RotatingMachine.p")).text = f"{slack['p_mw']:.12g}"
        ET.SubElement(machine, self._tag("RotatingMachine.q")).text = f"{slack['q_mvar']:.12g}"
        ET.SubElement(machine, self._tag("SynchronousMachine.referencePriority")).text = "1"
        ET.SubElement(machine, self._tag("SynchronousMachine.operatingMode"), {self._rdf("resource"): "http://iec.ch/TC57/2013/CIM-schema-cim16#SynchronousMachineOperatingMode.generator"})
        reg = ET.SubElement(root, self._tag("RegulatingControl"), {self._rdf("about"): f"#{slack['reg_control_id']}"})
        ET.SubElement(reg, self._tag("RegulatingControl.discrete")).text = "false"
        ET.SubElement(reg, self._tag("RegulatingControl.enabled")).text = "true"
        ET.SubElement(reg, self._tag("RegulatingControl.targetDeadband")).text = "0"
        ET.SubElement(reg, self._tag("RegulatingControl.targetValue")).text = f"{slack['target_vm_kv']:.12g}"
        ET.SubElement(reg, self._tag("RegulatingControl.targetValueUnitMultiplier"), {self._rdf("resource"): "http://iec.ch/TC57/2013/CIM-schema-cim16#UnitMultiplier.k"})

    def _append_line_ssh(self, root: ET.Element, assets: dict[str, object]) -> None:
        for line in assets["lines"]:
            if line["in_service"]:
                continue
            for terminal_id in (line["terminal_from_id"], line["terminal_to_id"]):
                terminal = ET.SubElement(root, self._tag("Terminal"), {self._rdf("about"): f"#{terminal_id}"})
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
        for load in assets["loads"]:
            self._terminal_tp(root, load["terminal_id"], assets["buses"][load["bus"]]["rdf_id"])
        self._terminal_tp(root, assets["slack"]["terminal_id"], assets["buses"][assets["slack"]["bus"]]["rdf_id"])

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

    def _slack_source(self, net: object, solved_net: object) -> dict[str, object]:
        if len(net.ext_grid):
            source = net.ext_grid.iloc[0]
            slack_res = solved_net.res_ext_grid.iloc[0]
            p_mw = -float(slack_res.p_mw)
            q_mvar = -float(slack_res.q_mvar)
            return {
                "name": self._name(source.get("name"), "Slack"),
                "bus": int(source.bus),
                "target_vm_pu": float(source.vm_pu),
                "p_mw": p_mw,
                "q_mvar": q_mvar,
                "min_p_mw": float(source.get("min_p_mw", min(0.0, p_mw))),
                "max_p_mw": float(source.get("max_p_mw", max(abs(p_mw) * 1.5, 1.0))),
                "min_q_mvar": float(source.get("min_q_mvar", min(-abs(q_mvar) * 2.0, q_mvar, -1.0))),
                "max_q_mvar": float(source.get("max_q_mvar", max(abs(q_mvar) * 2.0, q_mvar, 1.0))),
            }

        source = net.gen.loc[net.gen["slack"].fillna(False)].iloc[0]
        bus_idx = int(source.bus)
        p_mw = float(source.get("p_mw", 0.0))
        q_mvar = float(solved_net.res_gen.loc[source.name, "q_mvar"])
        return {
            "name": self._name(source.get("name"), "Slack"),
            "bus": bus_idx,
            "target_vm_pu": float(source.get("vm_pu", solved_net.res_bus.at[bus_idx, "vm_pu"])),
            "p_mw": p_mw,
            "q_mvar": q_mvar,
            "min_p_mw": float(source.get("min_p_mw", min(0.0, p_mw))),
            "max_p_mw": float(source.get("max_p_mw", max(abs(p_mw) * 1.5, 1.0))),
            "min_q_mvar": float(source.get("min_q_mvar", min(-abs(q_mvar) * 2.0, q_mvar, -1.0))),
            "max_q_mvar": float(source.get("max_q_mvar", max(abs(q_mvar) * 2.0, q_mvar, 1.0))),
        }

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
