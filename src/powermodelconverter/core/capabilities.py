from __future__ import annotations

from dataclasses import asdict, dataclass


@dataclass(frozen=True, slots=True)
class ToolCapability:
    tool: str
    balanced_import: bool
    balanced_export: bool
    balanced_validation: bool
    unbalanced_import: bool
    unbalanced_export: bool
    unbalanced_validation: bool
    notes: str


CAPABILITY_MATRIX: dict[str, ToolCapability] = {
    "matpower": ToolCapability(
        tool="matpower",
        balanced_import=True,
        balanced_export=True,
        balanced_validation=True,
        unbalanced_import=False,
        unbalanced_export=False,
        unbalanced_validation=False,
        notes="Balanced MATPOWER cases are validated. Unbalanced semantics are outside the MATPOWER exchange contract.",
    ),
    "opendss": ToolCapability(
        tool="opendss",
        balanced_import=True,
        balanced_export=False,
        balanced_validation=True,
        unbalanced_import=True,
        unbalanced_export=True,
        unbalanced_validation=True,
        notes="OpenDSS import is validated for a supported subset. Unbalanced routes are signed off for pandapower and PowerModelsDistribution on the starter three-phase feeder.",
    ),
    "pandapower": ToolCapability(
        tool="pandapower",
        balanced_import=True,
        balanced_export=True,
        balanced_validation=True,
        unbalanced_import=True,
        unbalanced_export=True,
        unbalanced_validation=True,
        notes="Balanced JSON and native pandapower 3ph models are supported. Unbalanced validation currently targets native pandapower roundtrips.",
    ),
    "powermodels": ToolCapability(
        tool="powermodels",
        balanced_import=False,
        balanced_export=True,
        balanced_validation=True,
        unbalanced_import=False,
        unbalanced_export=False,
        unbalanced_validation=False,
        notes="Balanced PowerModels JSON export is validated with Julia AC power flow.",
    ),
    "powermodelsdistribution": ToolCapability(
        tool="powermodelsdistribution",
        balanced_import=False,
        balanced_export=False,
        balanced_validation=False,
        unbalanced_import=False,
        unbalanced_export=True,
        unbalanced_validation=True,
        notes="Validated as an unbalanced backend for the OpenDSS-source starter feeder. Canonical import into this repo is not implemented yet.",
    ),
    "pypsa": ToolCapability(
        tool="pypsa",
        balanced_import=True,
        balanced_export=True,
        balanced_validation=True,
        unbalanced_import=False,
        unbalanced_export=False,
        unbalanced_validation=False,
        notes="Balanced AC PyPSA import/export is validated for the current line-based transmission subset against pandapower and PyPSA power flow. Current scope excludes unbalanced networks and does not yet sign off transformer-, shunt-, link-, store-, or storage-unit-heavy PyPSA models.",
    ),
    "pypower": ToolCapability(
        tool="pypower",
        balanced_import=False,
        balanced_export=False,
        balanced_validation=False,
        unbalanced_import=False,
        unbalanced_export=False,
        unbalanced_validation=False,
        notes="Planned via the MATPOWER-style balanced exchange contract.",
    ),
}


def capability_rows() -> list[dict[str, object]]:
    return [asdict(CAPABILITY_MATRIX[name]) for name in sorted(CAPABILITY_MATRIX)]
