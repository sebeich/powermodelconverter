# UC2 Inputs

`pv_profile_summer_day.csv` is a fixed 24-hour summer-day rooftop PV availability profile used as an exogenous planning signal in the PyPSA capacity-expansion stage.

It is intentionally separated from the benchmark feeder inputs:
- topology and demand benchmark: IEEE European Low Voltage Test Feeder package
- technology costs: PyPSA `technology-data`, commit `64c3e6a872ccc86f6fca7d74640f1bc7979e89a4`
- PV forcing signal: fixed in-repo CSV with deterministic values for reproducible runs

This CSV is not part of the IEEE feeder benchmark itself; it is the additional forcing series required by the sector-coupled planning experiment.
