# UC3 — CGMES Exchange Model to Security Analysis and Loss-Minimization OPF

Scenario: the ENTSO-E SmallGrid conformity package (CGMES 2.4.15, BaseCase + Boundary) is imported through pmc, screened for N-1 line and transformer contingencies in pandapower, and dispatched with a loss-minimization AC-OPF in PowerModels.jl. CGMES is the regulator-mandated European exchange format but is purely a data standard — neither pandapower's analysis stack nor PowerModels can be reached from it without the conversion hub.

Inputs: `validation_cases/native/cgmes/CGMES_v2.4.15_SmallGridTestConfiguration_BaseCase_Complete_v3.0.0.zip` and the matching Boundary package. The source-side validation anchor is the SV (StateVariables) profile shipped inside the BaseCase package, i.e. the originating TSO's own solved bus voltages.

Expected residuals: pmc power flow vs. the published SV solved state below `1e-3 pu`; CGMES roundtrip (export → re-import) and `pandapower -> PowerModels` legs inside the balanced `1e-3` envelope. The roundtrip runs on the full SmallGrid topology with shunts and wards frozen at their solved-state PQ injections, because those equipment classes are not yet carried by the CGMES exporter (documented in `summary.json`).

Downstream result: `contingency_table.csv` with per-outage convergence, peak loading, and violations newly introduced relative to the base case (the package carries no usable thermal ratings through cim2pp — line ratings fall back to a 1 kA placeholder, so loading is a relative indicator while the voltage band and convergence are absolute), plus a loss-minimization AC-OPF (uniform marginal cost on every generator — the natural cost-free objective, since CGMES carries no economic data) reporting dispatched losses against the base-case power flow losses.

Citations:
- ENTSO-E. CGMES Conformity Assessment Scheme Test Configurations, v2.4.15.
- IEC 61970-600-1/600-2, Common Grid Model Exchange Standard (CGMES).
- Thurner, L. et al. "pandapower — An Open-Source Python Tool for Convenient Modeling, Analysis, and Optimization of Electric Power Systems." IEEE Transactions on Power Systems, 2018.
- Coffrin, C.; Bent, R.; Sundar, K.; Ng, Y.; Lubin, M. "PowerModels.jl: An Open-Source Framework for Exploring Power Flow Formulations." PSCC, 2018.
