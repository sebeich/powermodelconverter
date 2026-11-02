"""Native anchor for CGMES routes: the SV profile shipped inside the package.

CGMES is an exchange standard, not a tool, so there is no executable reference
implementation to solve a case and compare against -- the arrangement used for
MATPOWER, OpenDSS, PyPSA and the Julia backends is not available here.

A conformity package does, however, carry the originating TSO's own solved state
in its StateVariables profile. That is a native reference in the sense that
matters: it was produced by the tool that authored the model, independently of
any converter. Where a package ships one, it is the anchor a CGMES import should
be judged against, rather than pandapower re-solving the converted model.

Note what this does *not* cover: an exported package written by pmc has no SV
profile of its own, so export routes still rest on an independent reader
(`pandapower.converter.cim.from_cim`) rather than an independent solver. A
lenient reader is weaker evidence than an executing tool -- pmc emitted
syntactically invalid MATPOWER files for months while a third-party parser
accepted them.
"""

from __future__ import annotations

import math
from pathlib import Path
from typing import Any
import zipfile

from powermodelconverter.core.model import CanonicalCase
from powermodelconverter.validation.result import ValidationResult

# The SV profile is a solved state from another tool, not a re-solve of the same
# model, so it carries genuine numerical difference. UC3 measures 1.68e-4 pu on
# the ENTSO-E SmallGrid package; 1e-3 is the signed-off balanced tier.
SV_TOLERANCE_PU = 1e-3

_SV_ID_COLUMNS = ("origin_id", "cim_topnode")


def _local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def parse_sv_profile(package: str | Path) -> dict[str, dict[str, float]]:
    """Read SvVoltage records (kV magnitude, degree angle) keyed by TopologicalNode."""
    import xml.etree.ElementTree as ET

    records: dict[str, dict[str, float]] = {}
    package_path = Path(package)
    members: list[bytes] = []
    if package_path.is_dir():
        for child in sorted(package_path.iterdir()):
            if child.suffix.lower() == ".zip":
                with zipfile.ZipFile(child) as archive:
                    members.extend(archive.read(name) for name in archive.namelist()
                                   if name.lower().endswith(".xml"))
            elif child.suffix.lower() == ".xml":
                members.append(child.read_bytes())
    else:
        with zipfile.ZipFile(package_path) as archive:
            members.extend(archive.read(name) for name in archive.namelist()
                           if name.lower().endswith(".xml"))

    for payload in members:
        try:
            root = ET.fromstring(payload)
        except ET.ParseError:
            continue
        for element in root.iter():
            if _local_name(element.tag) != "SvVoltage":
                continue
            entry: dict[str, float] = {}
            node: str | None = None
            for child in element:
                name = _local_name(child.tag)
                if name == "SvVoltage.v":
                    entry["v_kv"] = float(child.text or 0.0)
                elif name == "SvVoltage.angle":
                    entry["angle_degree"] = float(child.text or 0.0)
                elif name == "SvVoltage.TopologicalNode":
                    node = (child.attrib.get(
                        "{http://www.w3.org/1999/02/22-rdf-syntax-ns#}resource") or "").lstrip("#")
            if node and "v_kv" in entry:
                entry.setdefault("angle_degree", 0.0)
                records[node] = entry
    return records


def validate_against_sv_profile(
    case: CanonicalCase,
    package: str | Path,
    solved_net: Any,
    *,
    voltage_tolerance_pu: float = SV_TOLERANCE_PU,
) -> ValidationResult:
    """Compare a solved converted model against the package's published SV state."""
    try:
        sv_records = parse_sv_profile(package)
    except Exception as exc:  # noqa: BLE001 - record, never abort the report
        return ValidationResult(
            case_id=case.case_id, passed=False, slack_delta_mva=math.inf,
            max_voltage_delta_pu=math.inf,
            details={"compared_buses": 0, "backend": "cgmes_sv", "error": str(exc)[:500]},
            status="pending", notes=f"SV profile unreadable: {type(exc).__name__}.")

    buses = solved_net.bus
    id_column = next((c for c in _SV_ID_COLUMNS if c in buses.columns), None)
    if id_column is None or not sv_records:
        return ValidationResult(
            case_id=case.case_id, passed=False, slack_delta_mva=math.inf,
            max_voltage_delta_pu=math.inf,
            details={"compared_buses": 0, "backend": "cgmes_sv"},
            status="pending",
            notes="No SV profile or no CGMES node identifiers on the converted buses.")

    max_delta = 0.0
    compared = 0
    for idx, bus in buses.iterrows():
        record = sv_records.get(str(bus[id_column]))
        if record is None:
            continue
        vn_kv = float(bus["vn_kv"])
        reference = complex(
            (record["v_kv"] / vn_kv) * math.cos(math.radians(record["angle_degree"])),
            (record["v_kv"] / vn_kv) * math.sin(math.radians(record["angle_degree"])),
        )
        vm = float(solved_net.res_bus.at[idx, "vm_pu"])
        va = math.radians(float(solved_net.res_bus.at[idx, "va_degree"]))
        max_delta = max(max_delta, abs(complex(vm * math.cos(va), vm * math.sin(va)) - reference))
        compared += 1

    return ValidationResult(
        case_id=case.case_id,
        passed=compared > 0 and max_delta <= voltage_tolerance_pu,
        # The SV profile publishes voltages, not a slack injection, so there is
        # nothing to compare on the power side; reported as zero rather than
        # invented.
        slack_delta_mva=0.0,
        max_voltage_delta_pu=max_delta,
        details={"compared_buses": compared, "backend": "cgmes_sv",
                 "reference": "StateVariables profile (originating TSO solved state)"},
    )
