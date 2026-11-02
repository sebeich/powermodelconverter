# Validation Case Inventory

The validation cases now live under `validation_cases/`. The Python package under `src/powermodelconverter/` contains converter code only; generated or large network artifacts are kept out of the package tree.

## Canonical Layout

| Path | Role |
| --- | --- |
| `src/powermodelconverter/` | Hub-and-spoke converter code. The hub is the canonical pandapower-backed case. |
| `validation_cases/native/` | Small source fixtures grouped by native source tool. |
| `validation_cases/native/outputs/` | Checked derived exports from native fixtures. |
| `validation_cases/pypsa-eur/` | PyPSA-Eur data-only validation cases and derived pandapower outputs. |
| `validation_cases/ding0/` | DING0-origin validation snapshots and hub-spoke exports. |
| `validation_cases/pglib/` | Selected PGLib MATPOWER source cases for MATPOWER validation. |
| `docs/validation_report.*` | Generated validation overview used for publication/review. |
| `artifacts/exports/` | Ignored scratch output from CLI runs. |
| `input/` | Ignored scratch/download/build area. Full upstream repositories may live here locally, but are not validation cases. |

## Current Cases

| Case | Source | Type | Main files |
| --- | --- | --- | --- |
| `case9` | MATPOWER | balanced | `validation_cases/native/matpower/case9.m` |
| `cgmes_small_grid` | CGMES | balanced | `validation_cases/native/cgmes/*.zip` |
| `minimal_radial` | OpenDSS | balanced | `validation_cases/native/opendss/minimal_radial.dss` |
| `minimal_chain` | OpenDSS | balanced | `validation_cases/native/opendss/minimal_chain.dss` |
| `minimal_unbalanced_3ph` | OpenDSS | unbalanced | `validation_cases/native/opendss/minimal_unbalanced_3ph.dss` |
| `minimal_unbalanced_branch` | OpenDSS | unbalanced | `validation_cases/native/opendss/minimal_unbalanced_branch.dss` |
| `ieee13_opendss` | OpenDSS | unbalanced | `validation_cases/native/opendss/IEEE13Nodeckt.dss` plus bus XY and line-code files |
| `ieee_european_lv_asymmetric` | pandapower | unbalanced | `validation_cases/native/pandapower/ieee_european_lv_asymmetric.json` |
| `pglib_opf_case5_pjm` | MATPOWER/PGLib | balanced | `validation_cases/pglib/source/pglib_opf_case5_pjm.m` |
| `pglib_opf_case14_ieee` | MATPOWER/PGLib | balanced | `validation_cases/pglib/source/pglib_opf_case14_ieee.m` |
| `pglib_opf_case30_ieee__api` | MATPOWER/PGLib | balanced | `validation_cases/pglib/source/pglib_opf_case30_ieee__api.m` |
| `ding0_oep_hub_spoke_952_998_1021_1062` | DING0/OEP | balanced | `validation_cases/ding0/oep_hub_spoke_952_998_1021_1062/outputs/*.json` and `*.pypsa.nc` |
| `pypsa_eur_osm_raw_de_60kv` | PyPSA-Eur | balanced | `validation_cases/pypsa-eur/osm_raw_de_60kv/source/base.nc` |
| `pypsa_eur_hv_all_de_mv` | PyPSA-Eur | balanced | `validation_cases/pypsa-eur/hv_all_de_mv/source/base_s_all_elec_de_mv_overlay.nc` |

## PyPSA-Eur Cases

`pypsa_eur_osm_raw_de_60kv` is the Germany OSM raw topology down to 60 kV. It is kept as data only, not as a vendored PyPSA-Eur checkout. The output folder contains the converted pandapower JSON, diagnostic JSON, static HTML plot, PNG, and validation JSON.

`pypsa_eur_hv_all_de_mv` combines the normal Europe HV PyPSA-Eur model with the Germany MV/110 kV OSM overlay. The source summary records 8660 buses, 12257 lines, 696 transformers, and 7 sub-networks. Its outputs include the full pandapower JSON and every pandapower island as a separate JSON under `validation_cases/pypsa-eur/hv_all_de_mv/outputs/pypsa_eur_hv_all_de_mv.pandapower_all_islands/`.

The full PyPSA-Eur repository is not part of the validation collection. Local clones and Snakemake work directories belong under ignored `input/`; only the resulting NetCDF source data and converter outputs are retained here.
