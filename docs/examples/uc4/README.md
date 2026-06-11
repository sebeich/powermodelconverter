# UC4 — Composing PyPSA-Eur EHV with the OSM-Derived German Sub-Transmission Grid

Scenario: the Europe-wide PyPSA-Eur electricity network (`base_s_all_elec.nc`) is composed with the OpenStreetMap-derived German grid from 60 kV upward (4,543 buses, 5,381 lines, 696 transformers; 671 matched EHV↔HV substation couplings), imported through pmc into the pandapower hub, solved as a full AC power flow, and cross-validated against PowerModels.jl at scale. Three sources, none individually sufficient: PyPSA-Eur stops at the EHV level and solves linearized flows, the raw OSM extract is topology data rather than a solvable electrical model, and neither ecosystem provides an independently cross-checked AC solution.

Inputs: the staged main island `validation_cases/pypsa-eur/hv_all_de_mv/outputs/pypsa_eur_hv_all_de_mv.pandapower_all_islands/pypsa_eur_hv_all_de_mv.island_00.pandapower.json` (7,824 buses spanning 60–380 kV). The overlay build and the PyPSA→pandapower import are regenerated with `scripts/build_pypsa_eur_hv_de_mv_overlay.py` and `scripts/import_pypsa_eur_to_pandapower.py`; their staging-time validation (slack Δ ≈ 4e-8 MVA, voltage Δ ≈ 9e-11 pu, Ybus relative Frobenius norm ≈ 2e-17) is carried into `summary.json` as provenance.

Expected residuals: pandapower self-check and `pandapower -> PowerModels` cross-validation inside the balanced `1e-3` envelope on all 7,824 buses — the scale demonstration for the conversion hub.

Downstream result: `subtransmission_loading.csv` (loading distribution of the 60–155 kV layer), `boundary_transformer_flows.csv` (power exchange at the 671 EHV↔HV coupling transformers), and `voltage_levels.csv` (per-level voltage statistics).

Honest disclosures (also in `summary.json`): OSM line parameters are typed per-voltage-class defaults, not measured impedances; the operating point is the synthetic degree-weighted load and distributed-PV allocation recorded at staging time (≈0.1 MW per island), so flow magnitudes demonstrate methodology and model solvability, not a metered system state; substation matching distances (mean 0.034°, max 0.44°) are reported as a data-quality metric rather than hidden.

Citations:
- Hörsch, J.; Hofmann, F.; Schlachtberger, D.; Brown, T. "PyPSA-Eur: An open optimisation model of the European transmission system." Energy Strategy Reviews, 2018.
- Brown, T.; Hörsch, J.; Schlachtberger, D. "PyPSA: Python for Power System Analysis." Journal of Open Research Software, 2018.
- OpenStreetMap contributors, Open Database License (ODbL).
- Coffrin, C.; Bent, R.; Sundar, K.; Ng, Y.; Lubin, M. "PowerModels.jl: An Open-Source Framework for Exploring Power Flow Formulations." PSCC, 2018.
