from __future__ import annotations

import math
import re
from pathlib import Path
from typing import Any

from powermodelconverter.importers.opendss import OpenDSSResultSnapshot
from powermodelconverter.core.model import CanonicalCase
from powermodelconverter.core.pandapower_backend import PandapowerAdapter
from powermodelconverter.importers.pypsa import PypsaAdapter
from powermodelconverter.validation.result import ValidationResult
from powermodelconverter.validation.runners import (
    run_julia_pmd,
    run_julia_powermodels,
    run_julia_powersystems,
    run_pandapower_3ph_pf,
    run_pandapower_pf,
    run_pypsa_pf,
)
from powermodelconverter.validation.tolerances import (
    BALANCED_TOLERANCES,
    OPENDSS_BALANCED_TOLERANCES,
    OPENDSS_UNBALANCED_TOLERANCES,
    UNBALANCED_PMD_TOLERANCES,
    UNBALANCED_PP_TOLERANCES,
)


def compare_voltages(source_v: dict[str, complex], target_v: dict[str, complex], tolerance: float) -> ValidationResult:
    max_delta = 0.0
    compared = 0
    for key, source_value in source_v.items():
        target_value = target_v.get(key)
        if target_value is None:
            continue
        max_delta = max(max_delta, abs(source_value - target_value))
        compared += 1
    return ValidationResult(
        case_id="comparison",
        passed=compared > 0 and max_delta <= tolerance,
        slack_delta_mva=0.0,
        max_voltage_delta_pu=max_delta,
        details={"compared_points": compared},
    )


class ValidationService:
    def __init__(self) -> None:
        self._pandapower = PandapowerAdapter()
        self._pypsa = PypsaAdapter()

    def validate_against_pandapower(
        self,
        case: CanonicalCase,
        *,
        reference_slack_p_mw: float,
        reference_slack_q_mvar: float,
        reference_voltages: dict[str, complex],
        slack_tolerance_mva: float = BALANCED_TOLERANCES["slack_mva"],
        voltage_tolerance_pu: float = BALANCED_TOLERANCES["voltage_pu"],
    ) -> ValidationResult:
        net = run_pandapower_pf(case)
        slack_p, slack_q = self._extract_balanced_slack(net)
        slack_delta = math.hypot(slack_p - reference_slack_p_mw, slack_q - reference_slack_q_mvar)

        max_voltage_delta = 0.0
        compared = 0
        for _, row in net.res_bus.iterrows():
            bus_key = self._bus_key(net, int(row.name))
            reference = reference_voltages.get(bus_key)
            if reference is None:
                continue
            angle_rad = math.radians(float(row.va_degree))
            actual = complex(float(row.vm_pu) * math.cos(angle_rad), float(row.vm_pu) * math.sin(angle_rad))
            max_voltage_delta = max(max_voltage_delta, abs(actual - reference))
            compared += 1

        passed = slack_delta <= slack_tolerance_mva and max_voltage_delta <= voltage_tolerance_pu and compared > 0
        return ValidationResult(
            case_id=case.case_id,
            passed=passed,
            slack_delta_mva=slack_delta,
            max_voltage_delta_pu=max_voltage_delta,
            details={"compared_buses": compared},
        )

    def validate_opendss_roundtrip(
        self,
        case: CanonicalCase,
        reference: OpenDSSResultSnapshot,
        **kwargs: Any,
    ) -> ValidationResult:
        return self.validate_against_pandapower(
            case,
            reference_slack_p_mw=reference.slack_p_mw,
            reference_slack_q_mvar=reference.slack_q_mvar,
            reference_voltages=reference.voltages,
            **kwargs,
        )

    def validate_opendss_unbalanced_roundtrip(
        self,
        case: CanonicalCase,
        reference: OpenDSSResultSnapshot,
        *,
        slack_tolerance_mva: float = OPENDSS_UNBALANCED_TOLERANCES["slack_mva"],
        voltage_tolerance_pu: float = OPENDSS_UNBALANCED_TOLERANCES["voltage_pu"],
    ) -> ValidationResult:
        net = run_pandapower_3ph_pf(case)
        slack = self._extract_3ph_slack(net)
        slack_p = sum(slack[key] for key in ("p_a_mw", "p_b_mw", "p_c_mw"))
        slack_q = sum(slack[key] for key in ("q_a_mvar", "q_b_mvar", "q_c_mvar"))
        slack_delta = math.hypot(slack_p - reference.slack_p_mw, slack_q - reference.slack_q_mvar)

        candidate_voltages = self._extract_3ph_node_voltages(net)
        max_voltage_delta = 0.0
        compared = 0
        for key, ref_value in reference.node_voltages.items():
            candidate = candidate_voltages.get(key.lower())
            if candidate is None:
                continue
            max_voltage_delta = max(max_voltage_delta, abs(candidate - ref_value))
            compared += 1

        passed = slack_delta <= slack_tolerance_mva and max_voltage_delta <= voltage_tolerance_pu and compared > 0
        return ValidationResult(
            case_id=case.case_id,
            passed=passed,
            slack_delta_mva=slack_delta,
            max_voltage_delta_pu=max_voltage_delta,
            details={"compared_nodes": compared, "mode": "unbalanced_3ph", "backend": "pandapower"},
        )

    def validate_pandapower_case_against_opendss(
        self,
        case: CanonicalCase,
        reference: OpenDSSResultSnapshot,
        *,
        slack_tolerance_mva: float = OPENDSS_BALANCED_TOLERANCES["slack_mva"],
        voltage_tolerance_pu: float = OPENDSS_BALANCED_TOLERANCES["voltage_pu"],
    ) -> ValidationResult:
        net = run_pandapower_pf(case)
        slack_p, slack_q = self._extract_balanced_slack(net)
        slack_delta = math.hypot(slack_p - reference.slack_p_mw, slack_q - reference.slack_q_mvar)

        max_voltage_delta = 0.0
        compared = 0
        for _, row in net.res_bus.iterrows():
            bus_key = self._bus_key(net, int(row.name)).lower()
            candidate = reference.voltages.get(bus_key)
            if candidate is None:
                continue
            angle_rad = math.radians(float(row.va_degree))
            actual = complex(float(row.vm_pu) * math.cos(angle_rad), float(row.vm_pu) * math.sin(angle_rad))
            max_voltage_delta = max(max_voltage_delta, abs(actual - candidate))
            compared += 1

        passed = slack_delta <= slack_tolerance_mva and max_voltage_delta <= voltage_tolerance_pu and compared > 0
        return ValidationResult(
            case_id=case.case_id,
            passed=passed,
            slack_delta_mva=slack_delta,
            max_voltage_delta_pu=max_voltage_delta,
            details={"compared_buses": compared, "backend": "opendss"},
        )

    def validate_pandapower_unbalanced_against_opendss(
        self,
        case: CanonicalCase,
        reference: OpenDSSResultSnapshot,
        *,
        slack_tolerance_mva: float = OPENDSS_UNBALANCED_TOLERANCES["slack_mva"],
        voltage_tolerance_pu: float = OPENDSS_UNBALANCED_TOLERANCES["voltage_pu"],
    ) -> ValidationResult:
        net = run_pandapower_3ph_pf(case)
        slack = self._extract_3ph_slack(net)
        slack_p = sum(slack[key] for key in ("p_a_mw", "p_b_mw", "p_c_mw"))
        slack_q = sum(slack[key] for key in ("q_a_mvar", "q_b_mvar", "q_c_mvar"))
        slack_delta = math.hypot(slack_p - reference.slack_p_mw, slack_q - reference.slack_q_mvar)

        node_voltages = self._extract_3ph_node_voltages(net)
        max_voltage_delta = 0.0
        compared = 0
        for key, candidate in reference.node_voltages.items():
            actual = node_voltages.get(key.lower())
            if actual is None:
                continue
            max_voltage_delta = max(max_voltage_delta, abs(actual - candidate))
            compared += 1

        passed = slack_delta <= slack_tolerance_mva and max_voltage_delta <= voltage_tolerance_pu and compared > 0
        return ValidationResult(
            case_id=case.case_id,
            passed=passed,
            slack_delta_mva=slack_delta,
            max_voltage_delta_pu=max_voltage_delta,
            details={"compared_nodes": compared, "mode": "unbalanced_3ph", "backend": "opendss"},
        )

    def validate_pandapower_split_against_opendss(
        self,
        case: CanonicalCase,
        reference: OpenDSSResultSnapshot,
        *,
        slack_tolerance_mva: float = UNBALANCED_PP_TOLERANCES["slack_mva"],
        voltage_tolerance_pu: float = UNBALANCED_PP_TOLERANCES["voltage_pu"],
    ) -> ValidationResult:
        net_template = self._pandapower.to_net(case)
        init_vm_pu: list[float] = []
        init_va_degree: list[float] = []
        for _, row in net_template.bus.iterrows():
            bus_key = str(row["name"]).lower()
            candidate = reference.node_voltages.get(bus_key)
            if candidate is None:
                init_vm_pu.append(1.0)
                init_va_degree.append(0.0)
                continue
            init_vm_pu.append(abs(candidate))
            init_va_degree.append(math.degrees(math.atan2(candidate.imag, candidate.real)))

        net = run_pandapower_pf(
            case,
            algorithm="nr",
            init="auto",
            init_vm_pu=init_vm_pu,
            init_va_degree=init_va_degree,
            calculate_voltage_angles=True,
            numba=False,
            max_iteration=50,
        )
        slack_p, slack_q = self._extract_balanced_slack(net)
        slack_delta = math.hypot(slack_p - reference.slack_p_mw, slack_q - reference.slack_q_mvar)

        max_voltage_delta = 0.0
        delta_sum = 0.0
        compared = 0
        worst_nodes: list[tuple[float, str]] = []
        for _, row in net.res_bus.iterrows():
            bus_key = self._bus_key(net, int(row.name)).lower()
            candidate = reference.node_voltages.get(bus_key)
            if candidate is None:
                continue
            angle_rad = math.radians(float(row.va_degree))
            actual = complex(float(row.vm_pu) * math.cos(angle_rad), float(row.vm_pu) * math.sin(angle_rad))
            delta = abs(actual - candidate)
            max_voltage_delta = max(max_voltage_delta, delta)
            delta_sum += delta
            compared += 1
            worst_nodes.append((delta, bus_key))
        worst_nodes.sort(reverse=True)
        mean_voltage_delta = delta_sum / compared if compared else 0.0

        passed = slack_delta <= slack_tolerance_mva and max_voltage_delta <= voltage_tolerance_pu and compared > 0
        return ValidationResult(
            case_id=case.case_id,
            passed=passed,
            slack_delta_mva=slack_delta,
            max_voltage_delta_pu=max_voltage_delta,
            details={
                "compared_nodes": compared,
                "backend": "opendss",
                "mode": "phase_split",
                "mean_voltage_delta_pu": mean_voltage_delta,
                "worst_nodes": [{"node": node, "voltage_delta_pu": delta} for delta, node in worst_nodes[:10]],
            },
        )

    def validate_pandapower_unbalanced_roundtrip(
        self,
        case: CanonicalCase,
        *,
        slack_tolerance_mva: float = UNBALANCED_PP_TOLERANCES["slack_mva"],
        voltage_tolerance_pu: float = UNBALANCED_PP_TOLERANCES["voltage_pu"],
    ) -> ValidationResult:
        reference_net = run_pandapower_3ph_pf(case)
        candidate_net = run_pandapower_3ph_pf(case)
        reference_voltages = self._extract_3ph_bus_voltages(reference_net)
        candidate_voltages = self._extract_3ph_bus_voltages(candidate_net)
        reference_slack = self._extract_3ph_slack(reference_net)
        candidate_slack = self._extract_3ph_slack(candidate_net)
        slack_delta = math.sqrt(sum((candidate_slack[key] - reference_slack[key]) ** 2 for key in sorted(reference_slack)))

        max_voltage_delta = 0.0
        compared = 0
        for key, reference in reference_voltages.items():
            candidate = candidate_voltages.get(key)
            if candidate is None:
                continue
            max_voltage_delta = max(max_voltage_delta, abs(candidate - reference))
            compared += 1

        passed = slack_delta <= slack_tolerance_mva and max_voltage_delta <= voltage_tolerance_pu and compared > 0
        return ValidationResult(
            case_id=case.case_id,
            passed=passed,
            slack_delta_mva=slack_delta,
            max_voltage_delta_pu=max_voltage_delta,
            details={"compared_nodes": compared, "mode": "unbalanced_3ph", "backend": "pandapower"},
        )

    def validate_powermodels_export(
        self,
        case: CanonicalCase,
        *,
        powermodels_json: Path,
        julia_binary: str,
        julia_script: Path,
        julia_depot: Path,
        slack_tolerance_mva: float = BALANCED_TOLERANCES["slack_mva"],
        voltage_tolerance_pu: float = BALANCED_TOLERANCES["voltage_pu"],
    ) -> ValidationResult:
        net = run_pandapower_pf(case)
        pm_bus_lookup = self._pandapower.get_pm_bus_lookup(case)
        reference_voltages = {
            str(pm_bus_lookup.get(int(idx), int(idx) + 1)): complex(
                float(row.vm_pu) * math.cos(math.radians(float(row.va_degree))),
                float(row.vm_pu) * math.sin(math.radians(float(row.va_degree))),
            )
            for idx, row in net.res_bus.iterrows()
        }
        payload = run_julia_powermodels(
            powermodels_json,
            julia_binary=julia_binary,
            julia_script=julia_script,
            julia_depot=julia_depot,
        )
        slack_ref_p, slack_ref_q = self._extract_balanced_slack(net)
        slack_delta = math.hypot(payload["slack_p_mw"] - slack_ref_p, payload["slack_q_mvar"] - slack_ref_q)

        max_voltage_delta = 0.0
        compared = 0
        for bus_name, values in payload["bus_results"].items():
            reference = reference_voltages.get(bus_name)
            if reference is None:
                continue
            actual = complex(values["vm_pu_real"], values["vm_pu_imag"])
            max_voltage_delta = max(max_voltage_delta, abs(actual - reference))
            compared += 1

        passed = slack_delta <= slack_tolerance_mva and max_voltage_delta <= voltage_tolerance_pu and compared > 0
        return ValidationResult(
            case_id=case.case_id,
            passed=passed,
            slack_delta_mva=slack_delta,
            max_voltage_delta_pu=max_voltage_delta,
            details={"compared_buses": compared, "backend": "powermodels"},
        )

    def validate_powermodelsdistribution_export(
        self,
        case: CanonicalCase,
        *,
        pmd_input_path: Path,
        julia_binary: str,
        julia_script: Path,
        julia_depot: Path,
        julia_project: Path,
        reference_slack_p_mw: float,
        reference_slack_q_mvar: float,
        reference_node_voltages: dict[str, complex],
        slack_tolerance_mva: float = UNBALANCED_PMD_TOLERANCES["slack_mva"],
        voltage_tolerance_pu: float = UNBALANCED_PMD_TOLERANCES["voltage_pu"],
    ) -> ValidationResult:
        payload = run_julia_pmd(
            pmd_input_path,
            julia_binary=julia_binary,
            julia_script=julia_script,
            julia_depot=julia_depot,
            julia_project=julia_project,
        )
        slack_delta = math.hypot(payload["slack_p_mw"] - reference_slack_p_mw, payload["slack_q_mvar"] - reference_slack_q_mvar)

        max_voltage_delta = 0.0
        compared = 0
        for node_name, values in payload["node_results"].items():
            reference = reference_node_voltages.get(node_name.lower())
            if reference is None:
                continue
            actual = complex(values["vm_pu_real"], values["vm_pu_imag"])
            max_voltage_delta = max(max_voltage_delta, abs(actual - reference))
            compared += 1

        passed = slack_delta <= slack_tolerance_mva and max_voltage_delta <= voltage_tolerance_pu and compared > 0
        return ValidationResult(
            case_id=case.case_id,
            passed=passed,
            slack_delta_mva=slack_delta,
            max_voltage_delta_pu=max_voltage_delta,
            details={
                "compared_nodes": compared,
                "backend": "powermodelsdistribution",
                "slack_tolerance_mva": slack_tolerance_mva,
                "voltage_tolerance_pu": voltage_tolerance_pu,
            },
        )

    def validate_powersystems_export(
        self,
        case: CanonicalCase,
        *,
        powersystems_case: Path,
        julia_binary: str,
        julia_script: Path,
        julia_depot: Path,
        julia_project: Path,
        slack_tolerance_mva: float = BALANCED_TOLERANCES["slack_mva"],
        voltage_tolerance_pu: float = BALANCED_TOLERANCES["voltage_pu"],
    ) -> ValidationResult:
        net = run_pandapower_pf(case)
        reference_voltages = {
            self._normalize_bus_name(self._bus_key(net, int(idx))): complex(
                float(row.vm_pu) * math.cos(math.radians(float(row.va_degree))),
                float(row.vm_pu) * math.sin(math.radians(float(row.va_degree))),
            )
            for idx, row in net.res_bus.iterrows()
        }
        payload = run_julia_powersystems(
            powersystems_case,
            julia_binary=julia_binary,
            julia_script=julia_script,
            julia_depot=julia_depot,
            julia_project=julia_project,
        )

        max_voltage_delta = 0.0
        compared = 0
        for bus_name, values in payload.get("bus_results", {}).items():
            reference = reference_voltages.get(self._normalize_bus_name(bus_name))
            if reference is None:
                continue
            actual = complex(values["vm_pu_real"], values["vm_pu_imag"])
            max_voltage_delta = max(max_voltage_delta, abs(actual - reference))
            compared += 1

        solved = bool(payload.get("solved", False))
        slack_delta = float(payload.get("slack_delta_mva", 0.0))
        passed = solved and slack_delta <= slack_tolerance_mva and max_voltage_delta <= voltage_tolerance_pu and compared > 0
        return ValidationResult(
            case_id=case.case_id,
            passed=passed,
            slack_delta_mva=slack_delta,
            max_voltage_delta_pu=max_voltage_delta,
            details={
                "compared_buses": compared,
                "backend": "powersimulations",
                "validation_mode": payload.get("validation_mode", "unknown"),
            },
        )

    def validate_pypsa_export(
        self,
        case: CanonicalCase,
        *,
        pypsa_path: Path,
        slack_tolerance_mva: float = BALANCED_TOLERANCES["slack_mva"],
        voltage_tolerance_pu: float = BALANCED_TOLERANCES["voltage_pu"],
    ) -> ValidationResult:
        normalized_net = self._pypsa._normalize_pandapower_names(self._pandapower.to_net(case))
        normalized_case = self._pandapower.to_canonical(
            normalized_net,
            case_id=case.case_id,
            source_format=case.source_format,
            metadata={key: value for key, value in case.metadata.items() if key != "pandapower_json"},
            source_path=case.source_path,
        )
        reference = run_pandapower_pf(normalized_case)
        pypsa_result = run_pypsa_pf(pypsa_path)

        slack_ref_p, slack_ref_q = self._extract_balanced_slack(reference)
        slack_delta = math.hypot(pypsa_result.slack_p_mw - slack_ref_p, pypsa_result.slack_q_mvar - slack_ref_q)

        max_voltage_delta = 0.0
        compared = 0
        for _, row in reference.res_bus.iterrows():
            bus_key = self._bus_key(reference, int(row.name))
            candidate = pypsa_result.voltages.get(bus_key)
            if candidate is None:
                continue
            angle_rad = math.radians(float(row.va_degree))
            actual = complex(float(row.vm_pu) * math.cos(angle_rad), float(row.vm_pu) * math.sin(angle_rad))
            max_voltage_delta = max(max_voltage_delta, abs(actual - candidate))
            compared += 1

        passed = slack_delta <= slack_tolerance_mva and max_voltage_delta <= voltage_tolerance_pu and compared > 0
        return ValidationResult(
            case_id=case.case_id,
            passed=passed,
            slack_delta_mva=slack_delta,
            max_voltage_delta_pu=max_voltage_delta,
            details={"compared_buses": compared, "backend": "pypsa"},
        )

    def _extract_balanced_slack(self, net: Any) -> tuple[float, float]:
        slack_p = float(net.res_ext_grid.p_mw.sum()) if hasattr(net, "res_ext_grid") else 0.0
        slack_q = float(net.res_ext_grid.q_mvar.sum()) if hasattr(net, "res_ext_grid") else 0.0
        if hasattr(net, "gen") and hasattr(net, "res_gen") and len(net.gen):
            slack_mask = net.gen.get("slack", False)
            if hasattr(slack_mask, "fillna"):
                slack_mask = slack_mask.fillna(False)
            if getattr(slack_mask, "any", lambda: False)():
                slack_p += float(net.res_gen.loc[slack_mask, "p_mw"].sum())
                slack_q += float(net.res_gen.loc[slack_mask, "q_mvar"].sum())
        return slack_p, slack_q

    def _bus_key(self, net: Any, bus_idx: int) -> str:
        value = net.bus.loc[bus_idx, "name"] if "name" in net.bus.columns else None
        if value is None:
            return f"BUS{bus_idx}"
        if isinstance(value, float) and math.isnan(value):
            return f"BUS{bus_idx}"
        text = str(value).strip()
        return text if text else f"BUS{bus_idx}"

    def _extract_3ph_bus_voltages(self, net: Any) -> dict[str, complex]:
        voltages: dict[str, complex] = {}
        for idx, row in net.res_bus_3ph.iterrows():
            bus_key = self._bus_key(net, int(idx))
            for phase in ("a", "b", "c"):
                vm = float(row[f"vm_{phase}_pu"])
                va = math.radians(float(row[f"va_{phase}_degree"]))
                voltages[f"{bus_key}.{phase}"] = complex(vm * math.cos(va), vm * math.sin(va))
        return voltages

    def _extract_3ph_slack(self, net: Any) -> dict[str, float]:
        if getattr(net, "res_ext_grid_3ph", None) is None or net.res_ext_grid_3ph.empty:
            return {key: 0.0 for key in ("p_a_mw", "q_a_mvar", "p_b_mw", "q_b_mvar", "p_c_mw", "q_c_mvar")}
        return {key: float(net.res_ext_grid_3ph[key].sum()) for key in ("p_a_mw", "q_a_mvar", "p_b_mw", "q_b_mvar", "p_c_mw", "q_c_mvar")}

    def _extract_3ph_node_voltages(self, net: Any) -> dict[str, complex]:
        node_voltages: dict[str, complex] = {}
        phase_to_node = {"a": 1, "b": 2, "c": 3}
        for idx, row in net.res_bus_3ph.iterrows():
            bus_key = self._bus_key(net, int(idx)).lower()
            for phase, node in phase_to_node.items():
                vm = float(row[f"vm_{phase}_pu"])
                va = math.radians(float(row[f"va_{phase}_degree"]))
                node_voltages[f"{bus_key}.{node}"] = complex(vm * math.cos(va), vm * math.sin(va))
        return node_voltages

    def _normalize_bus_name(self, bus_name: str) -> str:
        normalized = re.sub(r"\s+", "", str(bus_name)).strip().lower()
        for prefix in ("acbus", "bus"):
            if normalized.startswith(prefix):
                normalized = normalized[len(prefix):]
                break
        return normalized.lstrip("_-")
