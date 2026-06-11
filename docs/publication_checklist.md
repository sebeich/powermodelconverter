# Publication Checklist

This checklist records the repository safety review before pushing the hub-and-spoke validation branch publicly.

## Checked

- Local credential file `.env.ding0` is ignored by Git and Docker.
- Local scratch/build data under `input/`, `resources/`, `artifacts/`, `out_*`, and Julia depots is ignored.
- The full local PyPSA-Eur checkout remains under ignored `input/pypsa-eur`; it is not part of the validation-case collection.
- HPK11 backend output is stored as `validation_cases/hpk11/outputs/hpk11.backend.pandapower.json`, not in `_backend_hpk11`.
- Validation source data and generated artifacts live under `validation_cases/`, outside the Python package tree.
- Large validation artifacts are marked for Git LFS in `.gitattributes`.

## Dataset License Checks Before Public Release

The following validation cases are intended to come from public sources, but their redistribution terms should be checked before publishing binaries in the public repository or release assets:

- `validation_cases/pypsa-eur/`: PyPSA-Eur-derived NetCDF and OSM-derived topology data.
- `validation_cases/ding0/`: DING0/OEP-derived network snapshot.
- `validation_cases/hpk11/`: Team-Nando/MV-LV-Networks HPK11 workbook and derived OpenDSS/pandapower artifacts.
- `validation_cases/pglib/`: selected PGLib OPF MATPOWER cases.
- `validation_cases/native/cgmes/`: official CGMES small-grid sample packages.
- `validation_cases/native/opendss/IEEE13Node*` and IEEE European LV artifacts in use-case outputs: IEEE PES test feeder materials.
- `validation_cases/native/pandapower/ieee_european_lv_asymmetric.json`: pandapower bundled test network.

## Do Not Commit

- `.env.ding0`
- `input/pypsa-eur/`
- `input/hpk11/`
- `input/pglib/`
- `input/technology_data/`
- `input/ieee_european_lv/`
- any regenerated scratch output outside `validation_cases/` unless it is intentionally promoted and documented in `validation_cases/manifest.json`
