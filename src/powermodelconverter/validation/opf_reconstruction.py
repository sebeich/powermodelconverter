"""Rebuild the optimisation problem a planning tool actually solved.

A PyPSA-Eur network is a *planning* model: it is optimised with linearised
flows, and the result carries no voltage-angle reference, because a linear
program does not need one. To run an AC power flow on that dispatch the staging
pipeline injects one slack generator per island (`SLACK_NAME_PREFIX`).

Those two uses pull in opposite directions, and the same model cannot serve
both:

* the **power flow** needs the injected slack -- without it there is no machine
  free to take up losses, and the AC power flow has no reference to converge on;
* the **optimisation** must not have it -- PyPSA optimised before that generator
  existed. Exported into an OPF it becomes an ordinary unit, and with no cost
  and no capacity bound the LP dispatches it ahead of everything else. A DC OPF
  on the converted model pushed 113 GW through it and came out 45 % cheaper than
  PyPSA's own answer: a different problem, not a conversion error.

So the PF model keeps the slack and the OPF model has it stripped. The AC OPF
needs a third arrangement again -- it has to source real losses, which the
lossless LP never priced -- and gets an explicit, deliberately expensive
supply-only unit instead (`add_priced_loss_slack`).
"""

from __future__ import annotations

from typing import Any

import pandapower as pp

SLACK_NAME_PREFIX = "pmc_val_slack_"

LOSS_SLACK_NAME = "pmc_acopf_loss_slack"


def strip_powerflow_slacks(net: Any) -> int:
    """Remove the injected power-flow slacks, restoring the original problem.

    Pricing them instead would leave an extra unit in the problem and change the
    answer; the reconstruction has to be the set of machines the source tool
    optimised over, no more.
    """
    removed = 0
    for table in ("ext_grid", "gen", "sgen"):
        frame = getattr(net, table, None)
        if frame is None or not len(frame) or "name" not in frame.columns:
            continue
        mask = frame.name.astype(str).str.startswith(SLACK_NAME_PREFIX)
        if not bool(mask.any()):
            continue
        victims = list(frame.index[mask])
        if hasattr(net, "poly_cost") and len(net.poly_cost):
            net.poly_cost.drop(
                index=net.poly_cost.index[(net.poly_cost.et == table)
                                          & (net.poly_cost.element.isin(victims))],
                inplace=True)
        frame.drop(index=victims, inplace=True)
        removed += len(victims)
    return removed


def _dearest_unit_cost(net: Any) -> float:
    if not hasattr(net, "poly_cost") or not len(net.poly_cost):
        return 0.0
    if "cp1_eur_per_mw" not in net.poly_cost.columns:
        return 0.0
    costs = net.poly_cost.cp1_eur_per_mw.astype(float)
    return float(costs.max()) if len(costs) else 0.0


def add_priced_loss_slack(
    net: Any,
    *,
    price_multiple: float = 1000.0,
    capacity_mw: float = 5e4,
    fallback_price_eur_per_mw: float = 1000.0,
) -> dict[str, Any]:
    """Add one supply-only unit at the reference bus to cover AC losses.

    A DC OPF is lossless, so its dispatch balances generation against load
    exactly. The AC OPF built on the same machine set has to find real losses
    somewhere, and if every unit is already at its optimised bound there is no
    feasible point at all.

    The unit is priced at 1000x the dearest machine in the dispatch, far above
    the merit order, so the optimiser reaches for it only where physics leaves
    no alternative and never to undercut the dispatch. Note what this costs in
    evidential value: at this price the unit is suppressed so far below the
    resolution of the objective that finding it undispatched is close to
    guaranteed by construction, and is therefore weak evidence that the real
    machines had the headroom. Pricing it at the dearest machine instead
    (price_multiple=1.0) puts it last in the merit order and makes that a real
    test.

    It is bounded at zero below: left free to absorb, a positive price turns
    consumption into revenue and the objective runs away negative (-389 399.91
    on the PyPSA-Eur transmission island, where the unit "earned" 15 EUR/MWh for taking 50 GW).
    """
    if not len(net.ext_grid):
        raise ValueError("No reference bus: cannot place a loss slack.")
    dearest = _dearest_unit_cost(net)
    price = dearest * price_multiple if dearest > 0.0 else fallback_price_eur_per_mw
    reference_bus = int(net.ext_grid.bus.iloc[0])
    index = pp.create_gen(
        net,
        bus=reference_bus,
        p_mw=0.0,
        vm_pu=float(net.ext_grid.vm_pu.iloc[0]),
        min_p_mw=0.0,
        max_p_mw=float(capacity_mw),
        # Same reactive convention as the machines it stands in for, rather
        # than a bound wide enough to wreck the solver's conditioning.
        min_q_mvar=-0.3 * float(capacity_mw),
        max_q_mvar=0.3 * float(capacity_mw),
        name=LOSS_SLACK_NAME,
        controllable=True,
    )
    pp.create_poly_cost(net, element=index, et="gen", cp1_eur_per_mw=price)
    return {
        "element": int(index),
        "bus": reference_bus,
        "price_eur_per_mw": price,
        "capacity_mw": float(capacity_mw),
        "dearest_real_unit_eur_per_mw": dearest,
        "price_multiple": float(price_multiple) if dearest > 0.0 else None,
        "pricing": "1000x the marginal cost of the dearest dispatched machine",
    }


__all__ = [
    "SLACK_NAME_PREFIX",
    "LOSS_SLACK_NAME",
    "strip_powerflow_slacks",
    "add_priced_loss_slack",
]
