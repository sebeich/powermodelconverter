# UC2 — OpenDSS Feeder to PyPSA Capacity Expansion

Scenario: the IEEE European Low Voltage Test Feeder is imported from the public IEEE PES benchmark package, aggregated to a balanced pandapower representation, exported to PyPSA, and solved as a 24-hour sector-coupled investment problem with PV, home batteries, and decentral air-source heat pumps.

Inputs: `input/ieee_european_lv/European_LV_Test_Feeder_v2.zip`, `input/technology_data/costs_2030_64c3e6a8.csv`, and [`inputs/pv_profile_summer_day.csv`](inputs/pv_profile_summer_day.csv). The source-side validation anchor is the published workbook `Solutions/OpenDSS/Snapshots/Snapshot_1min_Initialization_Off Peak.xlsx` shipped in the IEEE package.

Expected residuals: source-side positive-sequence voltage residual below `1e-3 pu` against the published OpenDSS snapshot; `pandapower -> PyPSA` residuals below `1e-3 pu` on slack and voltage.

Downstream result: `network.optimize(solver_name="highs")` returns an optimal annualized objective and writes bus-level expansion in `expansion.csv` plus hourly dispatch totals in `dispatch.csv`.

Citations:
- Schneider, M.; Stetz, T.; Kuehn, M. et al. IEEE European Low Voltage Test Feeder, IEEE PES Distribution Test Feeder Working Group benchmark package, v2.
- Brown, T.; Hoersch, J.; Schlachtberger, D. “PyPSA: Python for Power System Analysis.” Journal of Open Research Software, 2018.
- PyPSA technology-data repository, commit `64c3e6a872ccc86f6fca7d74640f1bc7979e89a4`.
- UC2 diagnosis note: [diagnosis.md](diagnosis.md).
