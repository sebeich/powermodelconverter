# PowerModelConverter

Validation-first conversion platform for power system models.

The purpose of this repository is to become a common, verifiable interchange layer between grid-modeling tools. Instead of trusting that an imported or exported file is correct because it parsed successfully, this project treats load-flow agreement as the acceptance criterion.

This repository accompanies an academic paper submission on validation-first power-system model conversion. The paper citation and DOI will be added here once they are available.

For every supported route, the target is:

1. Quick validation via slack power
2. Full validation via complex bus voltages
3. For three-phase models, full validation via phase voltages per bus

## What The Repo Does

Today the repo can:

- import balanced MATPOWER `.m` cases
- import OpenDSS `.dss` cases for a first supported subset
- import pandapower JSON, including native three-phase pandapower models
- export balanced cases to pandapower JSON
- export balanced cases to PowerModels JSON
- export unbalanced OpenDSS starter feeders to PowerModelsDistribution input
- validate balanced routes against pandapower and Julia `PowerModels`
- validate native pandapower three-phase roundtrips with `runpp_3ph`
- validate unbalanced OpenDSS starter feeders against both pandapower and Julia `PowerModelsDistribution`

The full route inventory is tracked in:

- [validation_report.html](/home/seb/powermodelconverter/docs/validation_report.html)
- [validation_report.md](/home/seb/powermodelconverter/docs/validation_report.md)
- [validation_report.json](/home/seb/powermodelconverter/docs/validation_report.json)
- [research_methodology_llm_input.md](/home/seb/powermodelconverter/docs/research_methodology_llm_input.md)

## Current Validation Status

Validated routes right now:

- `matpower -> pandapower` on `case9`
- `matpower -> powermodels` on `case9`
- `opendss -> pandapower` on `minimal_radial`
- `opendss -> powermodels` on `minimal_radial`
- `opendss -> pandapower` on `minimal_unbalanced_3ph`
- `opendss -> powermodelsdistribution` on `minimal_unbalanced_3ph`
- `pandapower -> pandapower` on a balanced `case9` JSON roundtrip
- `pandapower -> powermodels` on that balanced `case9` JSON roundtrip
- `pandapower -> pandapower` on `ieee_european_lv_asymmetric` with three-phase validation
- `pandapower -> powermodelsdistribution` on `ieee_european_lv_asymmetric`

Not yet validated:

- broader native unbalanced pandapower topologies outside the current supported subset

Representative measured precisions from the generated report:

- `matpower -> powermodels` on `case9`:
  slack delta `6.7856210496e-09 MVA`, max voltage delta `9.65387848049e-12 pu`
- `opendss -> powermodels` on `minimal_radial`:
  slack delta `6.86639507574e-14 MVA`, max voltage delta `6.67036704689e-16 pu`
- `opendss -> powermodelsdistribution` on `minimal_unbalanced_3ph`:
  slack delta `7.08868116922e-06 MVA`, max voltage delta `0.000936091367299 pu`
- `pandapower -> powermodelsdistribution` on `ieee_european_lv_asymmetric`:
  slack delta approximately `2.32e-04 MVA`, max phase-voltage delta approximately `4.78e-03 pu`
- `pandapower -> pandapower` on `ieee_european_lv_asymmetric`:
  slack delta `0.0 MVA`, max phase-voltage delta `0.0 pu`, `2721` compared phase points

## Repository Layout

- [pyproject.toml](/home/seb/powermodelconverter/pyproject.toml)
  Python package metadata, dependencies, and CLI entrypoint.
- [src/powermodelconverter/core](/home/seb/powermodelconverter/src/powermodelconverter/core)
  Canonical case model, capability registry, and shared exceptions.
- [src/powermodelconverter/adapters](/home/seb/powermodelconverter/src/powermodelconverter/adapters)
  Import and export logic for MATPOWER, OpenDSS, pandapower, and auxiliary native importers.
- [src/powermodelconverter/validation](/home/seb/powermodelconverter/src/powermodelconverter/validation)
  Balanced and unbalanced validation services.
- [src/powermodelconverter/cli](/home/seb/powermodelconverter/src/powermodelconverter/cli)
  Command-line interface exposed as `pmc`.
- [src/powermodelconverter/julia](/home/seb/powermodelconverter/src/powermodelconverter/julia)
  Local Julia project used for `PowerModels` validation.
- [src/powermodelconverter/julia_pmd](/home/seb/powermodelconverter/src/powermodelconverter/julia_pmd)
  Local Julia project used for `PowerModelsDistribution` validation.
- [src/powermodelconverter/data/samples](/home/seb/powermodelconverter/src/powermodelconverter/data/samples)
  Starter model files used for validation and examples.
- [docs](/home/seb/powermodelconverter/docs)
  Generated validation inventory and future documentation.
- [tests](/home/seb/powermodelconverter/tests)
  Smoke tests and route-validation tests.
- [scripts](/home/seb/powermodelconverter/scripts)
  Environment bootstrap and report-generation scripts.

## Canonical Model

The canonical representation is intentionally pragmatic at this stage:

- normalized element tables are stored in the `CanonicalCase`
- a lossless pandapower serialization is stored in metadata
- balanced versus unbalanced mode is tracked in metadata
- phase count is tracked so validation and export logic can branch cleanly

That means the repo is already organized around adapters and validation contracts, while still keeping the implementation compact enough to evolve quickly.

## Supported Tools

Current tool-level support is exposed by:

```bash
./.venv/bin/python -m powermodelconverter.cli.main capabilities
```

At a high level:

- `matpower`
  balanced import, balanced export, balanced validation
- `pandapower`
  balanced import/export/validation and native unbalanced three-phase import/export/validation
- `opendss`
  balanced import and validation for the supported subset, plus a validated unbalanced starter feeder route
- `powermodels`
  balanced export and balanced validation
- `powermodelsdistribution`
  validated as an unbalanced backend for the OpenDSS starter feeder route and the native pandapower `ieee_european_lv_asymmetric` feeder
- `pypower`
  planned

## Installation

### Python environment

```bash
python3 -m venv .venv
./.venv/bin/pip install --upgrade pip setuptools wheel
./.venv/bin/pip install -e .
```

### Julia validation environment

```bash
bash scripts/bootstrap_julia_env.sh
```

That installs the local Julia dependencies used by the validation scripts in [src/powermodelconverter/julia](/home/seb/powermodelconverter/src/powermodelconverter/julia) and [src/powermodelconverter/julia_pmd](/home/seb/powermodelconverter/src/powermodelconverter/julia_pmd).

## License

This project is released under the BSD 3-Clause License. See [LICENSE](/home/seb/powermodelconverter/LICENSE).

## Sample Cases

Included sample files:

- MATPOWER: [case9.m](/home/seb/powermodelconverter/src/powermodelconverter/data/samples/matpower/case9.m)
- OpenDSS starter case: [minimal_radial.dss](/home/seb/powermodelconverter/src/powermodelconverter/data/samples/opendss/minimal_radial.dss)
- OpenDSS unbalanced starter feeder: [minimal_unbalanced_3ph.dss](/home/seb/powermodelconverter/src/powermodelconverter/data/samples/opendss/minimal_unbalanced_3ph.dss)
- OpenDSS future target: [IEEE13Nodeckt.dss](/home/seb/powermodelconverter/src/powermodelconverter/data/samples/opendss/IEEE13Nodeckt.dss)
- pandapower 3-phase: [ieee_european_lv_asymmetric.json](/home/seb/powermodelconverter/src/powermodelconverter/data/samples/pandapower/ieee_european_lv_asymmetric.json)

Notes:

- `minimal_radial.dss` is the current validated OpenDSS reference case
- `minimal_unbalanced_3ph.dss` is the current validated OpenDSS unbalanced starter feeder for both pandapower and PowerModelsDistribution
- `IEEE13Nodeckt.dss` is present as a future expansion target, but not yet covered by the supported OpenDSS subset
- SimBench remains available only as a native pandapower-family import helper, not as a cross-tool exchange route

## How To Use

### 1. Show the supported route matrix

```bash
./.venv/bin/python -m powermodelconverter.cli.main capabilities
```

### 2. Validate a MATPOWER case

```bash
./.venv/bin/python -m powermodelconverter.cli.main validate \
  --source-format matpower \
  --source src/powermodelconverter/data/samples/matpower/case9.m
```

### 3. Validate the current OpenDSS starter route

```bash
./.venv/bin/python -m powermodelconverter.cli.main validate \
  --source-format opendss \
  --source src/powermodelconverter/data/samples/opendss/minimal_radial.dss
```

### 4. Validate an OpenDSS unbalanced three-phase feeder

```bash
./.venv/bin/python -m powermodelconverter.cli.main validate \
  --source-format opendss \
  --source src/powermodelconverter/data/samples/opendss/minimal_unbalanced_3ph.dss
```

### 5. Validate a native pandapower three-phase model

```bash
./.venv/bin/python -m powermodelconverter.cli.main validate \
  --source-format pandapower \
  --source src/powermodelconverter/data/samples/pandapower/ieee_european_lv_asymmetric.json
```

### 6. Import a native SimBench case into the canonical layer

```bash
./.venv/bin/python - <<'PY'
from powermodelconverter.adapters.simbench_adapter import SimbenchImportAdapter
case = SimbenchImportAdapter().import_case("1-HV-mixed--0-no_sw")
print(case.case_id, case.source_format, case.is_unbalanced, case.phase_count)
PY
```

### 7. Regenerate the full validation inventory

```bash
./.venv/bin/python scripts/generate_validation_report.py
```

This rewrites:

- [validation_report.html](/home/seb/powermodelconverter/docs/validation_report.html)
- [validation_report.md](/home/seb/powermodelconverter/docs/validation_report.md)
- [validation_report.json](/home/seb/powermodelconverter/docs/validation_report.json)

## Paper

The repository is prepared for a companion paper submission. The bibliographic reference and DOI are intentionally left blank until the publication record is available.

Until then:

- use [CITATION.cff](/home/seb/powermodelconverter/CITATION.cff) for repository citation metadata
- use [docs/research_methodology_llm_input.md](/home/seb/powermodelconverter/docs/research_methodology_llm_input.md) for the current research-methodology summary

### 8. Run the test suite

```bash
./.venv/bin/python -m pytest -q
```

## CLI Output

The `validate` command prints JSON with:

- `case_id`
- `source_format`
- `is_unbalanced`
- export artifact paths
- initial validation result
- PowerModels validation result when applicable
- PowerModelsDistribution validation result for supported unbalanced OpenDSS routes

For unbalanced pandapower cases:

- `powermodels_export` is `null`
- `powermodels_validation` is `null`

That is intentional for balanced `PowerModels`. Unbalanced pandapower cases can export to `PowerModelsDistribution` when they stay inside the currently supported 3-phase subset.

## Validation Rules

Balanced routes use:

- slack mismatch tolerance: `1e-3 MVA`
- max voltage mismatch tolerance: `1e-3 pu`

Three-phase pandapower routes use:

- the same slack tolerance
- the same voltage tolerance
- comparison over all phase voltages at all buses

Three-phase PowerModelsDistribution routes use:

- slack mismatch tolerance: `1e-3 MVA`
- max phase-voltage mismatch tolerance: `5e-3 pu`
- comparison over all compared phase nodes

## Known Limits

- SimBench is treated as a native pandapower-family import convenience, not as a separate exchange backend.
- OpenDSS import currently targets a conservative subset:
  `Vsource`, `bus`, `line`, `transformer`, and `load`.
- `PowerModelsDistribution` is currently validated for the OpenDSS starter feeder and the native pandapower `ieee_european_lv_asymmetric` feeder, not yet for every possible unbalanced topology.
- `pypower`, PowerFactory, PSS/E, and PSCAD adapters are not implemented yet.
- The canonical schema is still pandapower-backed rather than a fully neutral multi-phase network model.

## Development Workflow

Recommended loop:

1. install the Python and Julia environments
2. run one or more `validate` commands on sample cases
3. run `./.venv/bin/python scripts/generate_validation_report.py`
4. run `./.venv/bin/python -m pytest -q`
5. inspect [validation_report.html](/home/seb/powermodelconverter/docs/validation_report.html) before claiming a new route is validated

## Roadmap

1. decide whether SimBench should remain a native import convenience or move into a separate utility layer
2. expand OpenDSS support to regulators, capacitors, switches, and line-code heavy feeders such as IEEE 13-bus
3. extend the `PowerModelsDistribution` route beyond the OpenDSS starter feeder into broader canonical unbalanced exports
4. add broader explicit export adapters for MATPOWER and pandapower round-trips
5. add commercial-tool adapters behind optional environment-specific integrations
