from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


@dataclass(slots=True)
class ValidationResult:
    case_id: str
    passed: bool
    slack_delta_mva: float
    max_voltage_delta_pu: float
    details: dict[str, Any] = field(default_factory=dict)
    status: str = "validated"
    source_tool: str | None = None
    export_tool: str | None = None
    model_type: str | None = None
    notes: str = ""
    timestamp: str = field(default_factory=_utc_now_iso)
    duration_seconds: float = 0.0
