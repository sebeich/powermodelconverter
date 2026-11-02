"""Plausibility pass over a converted hub model, using pandapower's own diagnostic.

Validation answers "do two independent tools agree on this model?". That is a
statement about the conversion, not about the model: a network can round-trip to
1e-11 pu and still be implausible. This module adds the second question -- "does
the converted model make sense?" -- by running `pandapower.diagnostic` on the hub
net and condensing its result into something small enough for the certificate.

Only pandapower's checks are run. No pmc-authored check lives here, so a finding
is always attributable to pandapower and carries its documented semantics.

Two properties matter for the caller:

* it never raises -- a diagnostic failure is recorded, never propagated, because
  a plausibility note must not be able to fail a validated route;
* it is advisory -- nothing here feeds `ValidationResult.passed`. Converted
  models legitimately trip several of these checks (deliberately merged voltage
  levels, typed line defaults, loads carrying embedded generation as negative
  `p_mw`), and a certificate whose verdict flips on those would be ignored.
"""

from __future__ import annotations

from collections.abc import Callable
from copy import deepcopy
from typing import Any

import pandapower as pp

from powermodelconverter.core.model import CanonicalCase

MAX_SAMPLE_IDS = 5

# pp.diagnostic solves with runpp, and `invalid_values` inspects bus/line/trafo/
# trafo3w/load/sgen/gen/ext_grid/switch -- not asymmetric_load or asymmetric_sgen.
# On a three-phase hub model the static checks still apply to the shared tables,
# but the convergence-forensics family sees a net whose load is invisible to
# runpp and passes trivially. Say so rather than implying full coverage.
BALANCED_COVERAGE = "full"
UNBALANCED_COVERAGE = (
    "partial: pp.diagnostic solves with runpp, so the convergence checks do not "
    "see the asymmetric_load/asymmetric_sgen tables; static data checks apply"
)


def _json_safe(value: Any) -> Any:
    """Coerce a diagnostic payload into something json.dumps accepts.

    pandapower hands back numpy scalars and pandas indices inside nested dicts
    and lists; defaultdicts also appear.
    """
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(item) for item in value]
    if isinstance(value, bool):
        return value
    if hasattr(value, "item"):  # numpy scalar
        try:
            return value.item()
        except (AttributeError, ValueError):
            return str(value)
    if isinstance(value, (int, float, str)) or value is None:
        return value
    return str(value)


def _is_container(value: Any) -> bool:
    return isinstance(value, (dict, list, tuple, set))


def _count_findings(value: Any) -> int:
    """Count the offending element references a single check reported.

    Result shapes differ per check, so the recursion descends only through
    containers that group elements and stops at the ones that describe a single
    one:

    * ``{'line': [0, 1, 2]}``    -> element tables mapping to index lists: 3
    * ``{'line': {0: {...}}}``   -> table, then element id, then detail: 1
    * ``[(5, 'vn_kv', -1, '>0')]`` -> one tuple per offending value: 1
    * ``{'param': 'x', 'ok': False}`` -> a detail record about one element: 1

    The rule that separates them: a dict or tuple whose members are all scalars
    describes one element, while a list of scalars is a list of element ids.
    pp.diagnostic reports a check only when it found something, so a present but
    all-false payload still counts as one finding rather than none.
    """
    if isinstance(value, dict):
        if not value:
            return 0
        if all(_is_container(item) for item in value.values()):
            return sum(_count_findings(item) for item in value.values())
        return 1
    if isinstance(value, (list, set)):
        if not value:
            return 0
        if all(_is_container(item) for item in value):
            return sum(_count_findings(item) for item in value)
        return len(value)
    if isinstance(value, tuple):
        if value and all(_is_container(item) for item in value):
            return sum(_count_findings(item) for item in value)
        return 1
    return 1


def _truncate(value: Any, limit: int = MAX_SAMPLE_IDS) -> Any:
    """Keep a check's shape but cap the element lists.

    A 907-bus feeder reports hundreds of line indices per check; the certificate
    needs the shape and a handful of ids, not the full listing.
    """
    if isinstance(value, dict):
        return {str(key): _truncate(item, limit) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        items = list(value)
        kept = [_truncate(item, limit) for item in items[:limit]]
        if len(items) > limit:
            kept.append(f"... {len(items) - limit} more")
        return kept
    return _json_safe(value)


def diagnose_net(
    net: Any,
    *,
    unbalanced: bool = False,
    run: Callable[..., Any] | None = None,
    **diagnostic_kwargs: Any,
) -> dict[str, Any]:
    """Run pandapower's diagnostic on `net` and condense the result.

    `run` is passed straight through to pp.diagnostic, which uses it in place of
    its default runpp call for the convergence-forensics checks.
    """
    record: dict[str, Any] = {
        "tool": "pandapower.diagnostic",
        "pandapower_version": getattr(pp, "__version__", "unknown"),
        "coverage": UNBALANCED_COVERAGE if unbalanced else BALANCED_COVERAGE,
    }
    kwargs: dict[str, Any] = dict(diagnostic_kwargs)
    if run is not None:
        kwargs["run"] = run
    try:
        # On a copy: pp.diagnostic's convergence-forensics checks solve the net
        # they are handed and leave the res_* tables behind, and they perturb it
        # (scaled loads, zeroed injections, closed switches) along the way. The
        # caller's model must come back exactly as it went in.
        result = pp.diagnostic(
            deepcopy(net),
            report_style=None,
            warnings_only=True,
            return_result_dict=True,
            **kwargs,
        )
    except Exception as exc:  # noqa: BLE001 - advisory pass, never fatal
        record["error"] = f"{type(exc).__name__}: {exc}"
        record["clean"] = None
        record["finding_count"] = None
        record["checks_with_findings"] = []
        return record

    result = result or {}
    findings = {
        str(key): {"count": _count_findings(value), "sample": _truncate(value)}
        for key, value in result.items()
    }
    # pp.diagnostic reports only the checks that found something, so an empty
    # result is the clean case rather than a check that failed to run.
    record["checks_with_findings"] = sorted(findings)
    record["finding_count"] = sum(entry["count"] for entry in findings.values())
    record["clean"] = not findings
    record["findings"] = findings
    return record


def diagnose_case(
    case: CanonicalCase,
    *,
    run: Callable[..., Any] | None = None,
    **diagnostic_kwargs: Any,
) -> dict[str, Any]:
    """Diagnose the pandapower net behind a canonical case."""
    try:
        net = case.net
        unbalanced = bool(case.is_unbalanced)
    except Exception as exc:  # noqa: BLE001 - advisory pass, never fatal
        return {
            "tool": "pandapower.diagnostic",
            "pandapower_version": getattr(pp, "__version__", "unknown"),
            "error": f"{type(exc).__name__}: {exc}",
            "clean": None,
            "finding_count": None,
            "checks_with_findings": [],
        }
    return diagnose_net(net, unbalanced=unbalanced, run=run, **diagnostic_kwargs)


def summarize_diagnostic(record: dict[str, Any] | None) -> str:
    """One-cell rendering of a diagnostic record for the certificate tables."""
    if not record:
        return "not run"
    if record.get("error"):
        return "error"
    if record.get("clean"):
        return "clean"
    checks = record.get("checks_with_findings") or []
    count = record.get("finding_count")
    return f"{len(checks)} check(s), {count} element(s)"

