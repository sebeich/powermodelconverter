# Validation Cases

This directory is the canonical home for model data used by the validation suite.

The hub-and-spoke structure is:

- `src/powermodelconverter/`: converter code only. Importers build the canonical pandapower-backed case; exporters and validators read from that hub.
- `validation_cases/native/`: small native fixture models and checked derived exports for unit tests, examples, and report generation.
- `validation_cases/pypsa-eur/`: PyPSA-Eur data-only validation cases, including the Germany OSM raw 60 kV+ source and the combined Europe HV plus Germany MV overlay.
- `validation_cases/ding0/`: DING0-origin network snapshots and hub/spoke exports.
- `validation_cases/hpk11/`: Team-Nando HPK11 source workbook and OpenDSS/pandapower validation artifacts.
- `validation_cases/pglib/`: selected PGLib MATPOWER source cases used by use-case examples.
- `validation_cases/use_cases/`: generated use-case and paper-example artifacts promoted out of `docs/generated`.
- `docs/validation_report.*`: generated human-readable and machine-readable validation overview.
- `artifacts/exports/`: ignored scratch output from ad hoc CLI runs.
- `input/`: ignored scratch/download/build area. A local PyPSA-Eur checkout may live there while building data, but it is not part of the validation-case collection.

## Current Inventory

The structured inventory is stored in [manifest.json](manifest.json). The maintained groups are:

- `native/matpower`: `case9.m`
- `native/cgmes`: official CGMES small-grid base and boundary packages
- `native/opendss`: minimal balanced, chain, unbalanced starter feeders, and IEEE 13 source files
- `native/pandapower`: `ieee_european_lv_asymmetric.json`
- `native/outputs`: checked derived exports from native cases
- `pypsa-eur/osm_raw_de_60kv`: Germany OSM raw topology down to 60 kV with validation artifacts
- `pypsa-eur/hv_all_de_mv`: Europe HV PyPSA-Eur network combined with Germany MV/110 kV OSM overlay, with all pandapower islands exported separately
- `ding0/oep_hub_spoke_952_998_1021_1062`: OEP/DING0 network snapshot and pandapower/PyPSA hub-spoke outputs
- `hpk11`: Team-Nando HPK11 workbook, OpenDSS master, validated pandapower JSON, and plot
- `pglib/source`: selected PGLib MATPOWER cases used by UC1
- `use_cases/uc1`: PGLIB OPF cases 5, 14, and 30 in pandapower/PowerModels form plus relaxation results
- `use_cases/uc2`: OpenDSS-to-pandapower-to-PyPSA planning example artifacts
- `use_cases/uc2_chain`: OpenDSS chain feeder planning example artifacts
- `use_cases/uc3`: CGMES roundtrip use-case artifacts
- `use_cases/uc4`: MATPOWER plus OpenDSS merge sketch artifacts
- `use_cases/uc5`: synthetic pandapower/PowerModels/PyPSA cross-check artifacts

Large binary data such as NetCDF sources is expected to be stored via Git LFS.
