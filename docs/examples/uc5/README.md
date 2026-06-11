# UC5 — Unbalanced Feeder to Three-Way Cross-Validation and Unbalanced AC-OPF

Scenario: the IEEE European Low Voltage Test Feeder (asymmetric, 907 buses, 55 single-phase loads) is imported from its pandapower staging artifact, cross-validated phase-by-phase across three independent backends — pandapower `runpp_3ph`, OpenDSS, and PowerModelsDistribution — and then dispatched as an unbalanced AC-OPF in PowerModelsDistribution with single-phase rooftop PV and a per-phase voltage cap. The feeder's native ecosystems (OpenDSS, pandapower 3-phase) are simulation-only; per-phase optimal dispatch with curtailment is a capability that only becomes reachable through the conversion hub.

Inputs: `validation_cases/native/pandapower/ieee_european_lv_asymmetric.json` (signed-off staging of the IEEE benchmark). The PV scenario is generated deterministically from the validated PMD export: a 5 kW single-phase generator at every load bus, on the load's own phase and voltage base.

Expected residuals: pandapower 3-phase self-roundtrip below `1e-3 pu`; pandapower vs. OpenDSS phase voltages below `1.6e-3 pu`; pandapower vs. PowerModelsDistribution phase voltages below `5e-3 pu` (signed-off unbalanced tolerance tiers).

Downstream result: `solve_mc_opf` (AC polar unbalanced, Ipopt; IVR fallback documented in `summary.json` if used) with zero-cost PV against a unit-cost substation source and a `1.10 pu` per-phase voltage cap (the EN 50160 +10% supply band; the feeder's source already operates at `1.05 pu`, so a cap at the source setpoint would leave no headroom and renders the model infeasible). `curtailment_table.csv` reports per-generator, per-phase dispatch against capacity; the summary aggregates total PV capacity, dispatch, and curtailment.

Citations:
- IEEE PES Distribution Test Feeder Working Group. IEEE European Low Voltage Test Feeder benchmark package.
- Fobes, D. M.; Claeys, S.; Geth, F.; Coffrin, C. "PowerModelsDistribution.jl: An open-source framework for exploring distribution power flow formulations." Electric Power Systems Research, 2020.
- Thurner, L. et al. "pandapower — An Open-Source Python Tool for Convenient Modeling, Analysis, and Optimization of Electric Power Systems." IEEE Transactions on Power Systems, 2018.
- Dugan, R. C.; McDermott, T. E. "An open source platform for collaborating on smart grid research." IEEE PES General Meeting, 2011.
