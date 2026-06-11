# Research Use Cases

The paper-grade use cases live in per-UC folders under [docs/examples](examples): each has a `README.md` (scenario, inputs, expected residuals, downstream result, citations), a `run.py` reproducer, and — where real debugging happened — a `diagnosis.md` root-cause note. Outputs are written to `docs/generated/ucN/`.

Run them through compose so the repo's Python and Julia dependencies are available:

```bash
docker compose run --rm worker-python python docs/examples/uc1/run.py
docker compose run --rm worker-python python docs/examples/uc2/run.py
docker compose run --rm worker-python python docs/examples/uc3/run.py
docker compose run --rm worker-python python docs/examples/uc4/run.py
docker compose run --rm worker-python python docs/examples/uc5/run.py
```

## Latest Results

All five reproducers were run in the compose runtime on June 11, 2026:

| UC | Status | Key result | Artifact |
| --- | --- | --- | --- |
| `UC1` | Pass | all three PGLib cases pass both conversion legs (worst `max_voltage_delta_pu = 2.4e-11`); QC and SOC relaxation gaps within 10% relative of the published PGLib baseline | [uc1 summary](generated/uc1/summary.json) |
| `UC2` | Pass | 907 buses vs. published OpenDSS snapshot workbook: `max_voltage_delta_pu = 1.26e-4`; capacity expansion optimal (objective `1.65e4`) | [uc2 summary](generated/uc2/summary.json) |
| `UC3` | Pass | SV-anchor vs. TSO solved state: `1.68e-4 pu` on 118/118 buses; full-topology roundtrip `2.9e-11 MVA / 4.5e-14 pu`; PowerModels leg `7.6e-8 MVA / 1.9e-10 pu`; 186/186 N-1 cases converged; loss-min OPF `LOCALLY_SOLVED` (134.3 → 55.8 MW losses) | [uc3 summary](generated/uc3/summary.json) |
| `UC4` | Pass | 7,824-bus composite (PyPSA-Eur EHV + OSM German ≥60 kV) cross-validated against PowerModels: `slack_delta_mva = 3.9e-8`, `max_voltage_delta_pu = 9.0e-11` on all 7,824 buses | [uc4 summary](generated/uc4/summary.json) |
| `UC5` | Pass | three-way per-phase validation on 2,721 nodes (OpenDSS `6.4e-4 pu`, PMD `4.8e-3 pu`); unbalanced AC-OPF `LOCALLY_SOLVED`: 246.4 of 275 kW PV dispatched, 28.6 kW curtailed per-phase where the `1.10 pu` cap binds at the feeder ends | [uc5 summary](generated/uc5/summary.json) |

## Use-Case Catalog

### UC1 — PGLib convex OPF relaxation gaps

- folder: [examples/uc1](examples/uc1) (`README.md`, `run.py`, `opf_relaxations.jl`, `diagnosis.md`)
- chain: pinned PGLib MATPOWER cases → pandapower → PowerModels.jl AC/SOC/QC/SDP OPF
- scientific output: relaxation gaps compared against the published PGLib baseline table

### UC2 — OpenDSS feeder to PyPSA capacity expansion

- folder: [examples/uc2](examples/uc2) (`README.md`, `run.py`, `diagnosis.md`, `inputs/`)
- chain: IEEE European LV Test Feeder (OpenDSS CSV package) → pandapower (balanced phase-sum) → PyPSA 24-hour sector-coupled investment problem
- validation anchor: the published OpenDSS snapshot workbook shipped in the IEEE package

### UC3 — CGMES exchange model to security analysis and loss-minimization OPF

- folder: [examples/uc3](examples/uc3) (`README.md`, `run.py`, `loss_min_opf.jl`, `diagnosis.md`)
- chain: ENTSO-E SmallGrid CGMES package → pandapower N-1 screening → PowerModels loss-minimization AC-OPF
- validation anchor: the SV (StateVariables) profile inside the package — the originating TSO's own solved state
- the diagnosis note documents two real exporter bugs caught by upgrading the roundtrip from a smoke net to the full conformity topology (CIM load-sign convention for machines, dropped transformer magnetizing branch)

### UC4 — Composing PyPSA-Eur EHV with the OSM-derived German sub-transmission grid

- folder: [examples/uc4](examples/uc4) (`README.md`, `run.py`)
- chain: staged PyPSA-Eur Europe + OSM Germany ≥60 kV overlay island (7,824 buses, 60–380 kV) → pandapower → PowerModels cross-validation → sub-transmission loading, boundary-transformer flows, and per-level voltage statistics
- this is the scale demonstration; overlay provenance and synthetic-operating-point disclosures are carried into `summary.json`

### UC5 — Unbalanced feeder to three-way cross-validation and unbalanced AC-OPF

- folder: [examples/uc5](examples/uc5) (`README.md`, `run.py`, `mc_opf.jl`)
- chain: IEEE European LV asymmetric feeder → three-way per-phase cross-validation (pandapower `runpp_3ph` / OpenDSS / PowerModelsDistribution) → PMD unbalanced AC-OPF with single-phase PV and a per-phase voltage cap
- scientific output: per-phase PV curtailment emerging from the voltage constraint

## Legacy sketches

The earlier single-file scripts are superseded by the folders above but remain runnable as smoke checks: `uc1_matpower_to_powermodels.py`, `uc2_opendss_to_pypsa.py`, `uc3_cgmes_roundtrip.py`, `uc4_merge_sketch.py`, `uc5_pypsa_cross_check.py`, `ding0_import_smoke.py`, `hpk11_opendss_to_pandapower.py`. The DING0 and HPK11 scripts retain their role as importer coverage for real-world datasets.

## Notes

- UC4 composes models at the PyPSA layer with `scripts/build_pypsa_eur_hv_de_mv_overlay.py`; the converter's role is making the composite solvable and cross-verifiable.
- N-1 loading in UC3 is reported relative to the base case because cim2pp does not map CGMES CurrentLimits into pandapower thermal ratings (1 kA placeholder).
- The UC3 contingency loop, the UC4 analysis layer, and both Julia OPF drivers are deterministic; re-running regenerates identical tables up to solver tolerances.
