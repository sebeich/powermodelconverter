# PowerModelConverter

Validation-first conversion platform for power system models.

The purpose of this repository is to become a common, verifiable interchange layer between grid-modeling tools. Instead of trusting that an imported or exported file is correct because it parsed successfully, this project treats load-flow agreement as the acceptance criterion.

This repository accompanies an academic paper submission on validation-first power-system model conversion. The paper citation and DOI will be added here once they are available.

For every supported route, the target is:

1. Quick validation via slack power
2. Full validation via complex bus voltages
3. For three-phase models, full validation via phase voltages per bus

## Validation Methodology

The repo uses two complementary evidence layers:

- Native-origin cases are the primary ground-truth layer. They start from models that were authored natively in a source ecosystem and therefore preserve that tool's own modeling assumptions as far as possible.
- Canonical common-subset cases are the interoperability layer. They intentionally restrict the model to a shared subset so that each tool-to-tool path can be tested on the same electrical problem without hidden unsupported semantics.

The design goal is not to replace native-origin validation with canonical cases. It is to use canonical cases to prove broad interchangeability, and native-origin cases to prove tool-native fidelity. Balanced and unbalanced routes are therefore tracked separately in the generated reports.

## What The Repo Does

Today the repo can:

- import balanced MATPOWER `.m` cases
- import balanced CGMES/CIM packages through pandapower's native CGMES loader
- import OpenDSS `.dss` cases for a first supported subset
- import pandapower JSON, including native three-phase pandapower models
- import balanced PyPSA networks for the current supported subset
- export balanced cases to pandapower JSON
- export a balanced supported subset to CGMES/CIM and validate it by native pandapower re-import
- export balanced cases to OpenDSS for the current supported subset
- export balanced cases to PowerModels JSON
- export balanced transmission-style cases to PyPSA NetCDF
- export unbalanced OpenDSS starter feeders to PowerModelsDistribution input
- validate balanced routes against pandapower and Julia `PowerModels`
- validate balanced routes against PyPSA AC power flow for the current supported subset
- validate native pandapower three-phase roundtrips with `runpp_3ph`
- validate unbalanced OpenDSS starter feeders against both pandapower and Julia `PowerModelsDistribution`
- validate native pandapower three-phase exports to OpenDSS and PowerModelsDistribution

The full route inventory is tracked in:

- [validation_report.html](/home/seb/powermodelconverter-1/docs/validation_report.html)
- [validation_report.md](/home/seb/powermodelconverter-1/docs/validation_report.md)
- [validation_report.json](/home/seb/powermodelconverter-1/docs/validation_report.json)

## Validation Status

This repository is meant to be used as a verified converter, not just a parser.

The detailed route inventory, tested cases, and measured numerical precision are generated into:

- [validation_report.html](/home/seb/powermodelconverter-1/docs/validation_report.html)
- [validation_report.md](/home/seb/powermodelconverter-1/docs/validation_report.md)
- [validation_report.json](/home/seb/powermodelconverter-1/docs/validation_report.json)

Use those reports when you need the exact signed-off route matrix. The README stays focused on operating the repository with your own networks.

## Repository Layout

- [pyproject.toml](/home/seb/powermodelconverter-1/pyproject.toml)
  Python package metadata, dependencies, and CLI entrypoint.
- [src/powermodelconverter/core](/home/seb/powermodelconverter-1/src/powermodelconverter/core)
  Canonical case model, capability registry, and shared exceptions.
- [src/powermodelconverter/adapters](/home/seb/powermodelconverter-1/src/powermodelconverter/adapters)
  Import and export logic for MATPOWER, CGMES/CIM, OpenDSS, pandapower, PyPSA, and auxiliary native importers.
- [src/powermodelconverter/validation](/home/seb/powermodelconverter-1/src/powermodelconverter/validation)
  Balanced and unbalanced validation services.
- [src/powermodelconverter/cli](/home/seb/powermodelconverter-1/src/powermodelconverter/cli)
  Command-line interface exposed as `pmc`.
- [src/powermodelconverter/julia](/home/seb/powermodelconverter-1/src/powermodelconverter/julia)
  Local Julia project used for `PowerModels` validation.
- [src/powermodelconverter/julia_pmd](/home/seb/powermodelconverter-1/src/powermodelconverter/julia_pmd)
  Local Julia project used for `PowerModelsDistribution` validation.
- [src/powermodelconverter/data/samples](/home/seb/powermodelconverter-1/src/powermodelconverter/data/samples)
  Starter model files used for validation and examples.
- [docs](/home/seb/powermodelconverter-1/docs)
  Generated validation inventory and future documentation.
- [tests](/home/seb/powermodelconverter-1/tests)
  Smoke tests and route-validation tests.
- [scripts](/home/seb/powermodelconverter-1/scripts)
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
- `cgmes`
  balanced import, balanced export for the current supported subset, balanced validation through native pandapower CGMES loading
- `pandapower`
  balanced import/export/validation and native unbalanced three-phase import/export/validation
- `opendss`
  balanced and unbalanced import/export/validation for the currently signed-off subsets
- `pypsa`
  balanced import/export/validation for the current transmission-style AC subset
- `powermodels`
  balanced export and balanced validation
- `powermodelsdistribution`
  validated as an unbalanced backend for the OpenDSS starter feeder routes and the native pandapower `ieee_european_lv_asymmetric` feeder
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

That installs the local Julia dependencies used by the validation scripts in [src/powermodelconverter/julia](/home/seb/powermodelconverter-1/src/powermodelconverter/julia) and [src/powermodelconverter/julia_pmd](/home/seb/powermodelconverter-1/src/powermodelconverter/julia_pmd).

## License

This project is released under the BSD 3-Clause License. See [LICENSE](/home/seb/powermodelconverter-1/LICENSE).

## Sample Cases

Included sample files:

- CGMES base case: [CGMES_v2.4.15_SmallGridTestConfiguration_BaseCase_Complete_v3.0.0.zip](/home/seb/powermodelconverter-1/src/powermodelconverter/data/samples/cgmes/CGMES_v2.4.15_SmallGridTestConfiguration_BaseCase_Complete_v3.0.0.zip)
- CGMES boundary case: [CGMES_v2.4.15_SmallGridTestConfiguration_Boundary_v3.0.0.zip](/home/seb/powermodelconverter-1/src/powermodelconverter/data/samples/cgmes/CGMES_v2.4.15_SmallGridTestConfiguration_Boundary_v3.0.0.zip)
- MATPOWER: [case9.m](/home/seb/powermodelconverter-1/src/powermodelconverter/data/samples/matpower/case9.m)
- OpenDSS starter case: [minimal_radial.dss](/home/seb/powermodelconverter-1/src/powermodelconverter/data/samples/opendss/minimal_radial.dss)
- OpenDSS balanced chained feeder: [minimal_chain.dss](/home/seb/powermodelconverter-1/src/powermodelconverter/data/samples/opendss/minimal_chain.dss)
- OpenDSS unbalanced starter feeder: [minimal_unbalanced_3ph.dss](/home/seb/powermodelconverter-1/src/powermodelconverter/data/samples/opendss/minimal_unbalanced_3ph.dss)
- OpenDSS unbalanced branched feeder: [minimal_unbalanced_branch.dss](/home/seb/powermodelconverter-1/src/powermodelconverter/data/samples/opendss/minimal_unbalanced_branch.dss)
- OpenDSS future target: [IEEE13Nodeckt.dss](/home/seb/powermodelconverter-1/src/powermodelconverter/data/samples/opendss/IEEE13Nodeckt.dss)
- pandapower 3-phase: [ieee_european_lv_asymmetric.json](/home/seb/powermodelconverter-1/src/powermodelconverter/data/samples/pandapower/ieee_european_lv_asymmetric.json)

These samples serve two purposes:

- they are regression cases for the validated routes in the generated reports
- they are templates for how to structure your own input files when trying a new conversion

## How To Use

### 1. Install the environments

```bash
python3 -m venv .venv
./.venv/bin/pip install --upgrade pip setuptools wheel
./.venv/bin/pip install -e .
bash scripts/bootstrap_julia_env.sh
```

The Python environment runs the adapters and CLI. The Julia environment is required for the `PowerModels` and `PowerModelsDistribution` validation backends.

### 2. Check what the current repo claims to support

```bash
./.venv/bin/python -m powermodelconverter.cli.main capabilities
```

Use this before attempting a new route. It is the quick way to see which source and target formats are implemented in this branch.

### 3. Run a verified conversion from your own network

The main entrypoint is `validate`. You provide a source model, the CLI imports it, exports it through the signed-off backends for that source, and compares load-flow results.

Typical pattern:

```bash
./.venv/bin/python -m powermodelconverter.cli.main validate \
  --source-format <format> \
  --source <path-to-your-model>
```

Supported source-format values in the current repo include `matpower`, `opendss`, `pandapower`, `cgmes`, and `pypsa` for the currently signed-off subsets.

### 4. Example: validate a MATPOWER network

```bash
./.venv/bin/python -m powermodelconverter.cli.main validate \
  --source-format matpower \
  --source src/powermodelconverter/data/samples/matpower/case9.m
```

### 5. Example: validate a balanced OpenDSS feeder

```bash
./.venv/bin/python -m powermodelconverter.cli.main validate \
  --source-format opendss \
  --source src/powermodelconverter/data/samples/opendss/minimal_radial.dss
```

### 6. Example: validate an unbalanced OpenDSS feeder

```bash
./.venv/bin/python -m powermodelconverter.cli.main validate \
  --source-format opendss \
  --source src/powermodelconverter/data/samples/opendss/minimal_unbalanced_3ph.dss
```

### 7. Example: validate a native pandapower three-phase model

```bash
./.venv/bin/python -m powermodelconverter.cli.main validate \
  --source-format pandapower \
  --source src/powermodelconverter/data/samples/pandapower/ieee_european_lv_asymmetric.json
```

### 8. Example: validate a CGMES/CIM package

```bash
./.venv/bin/python -m powermodelconverter.cli.main validate \
  --source-format cgmes \
  --source src/powermodelconverter/data/samples/cgmes
```

### 9. Regenerate the validation inventory

```bash
./.venv/bin/python scripts/generate_validation_report.py
```

Do this after adding a new route, changing an adapter, or adding a new test case. It rewrites:

- [validation_report.html](/home/seb/powermodelconverter-1/docs/validation_report.html)
- [validation_report.md](/home/seb/powermodelconverter-1/docs/validation_report.md)
- [validation_report.json](/home/seb/powermodelconverter-1/docs/validation_report.json)

### 10. Run the test suite

```bash
./.venv/bin/python -m pytest -q
```

### 11. Recommended workflow for your own networks

1. Start with the source tool that authored your network natively.
2. Run `capabilities` to confirm the route exists in this branch.
3. Run `validate` on your network file or package.
4. Inspect the reported slack mismatch and voltage mismatch.
5. Open [validation_report.html](/home/seb/powermodelconverter-1/docs/validation_report.html) to compare your route against the signed-off reference cases.
6. If your model uses advanced components, check the limitations section below before trusting the result.

## CLI Output

The `validate` command prints JSON with:

- `case_id`
- `source_format`
- `is_unbalanced`
- export artifact paths
- initial validation result against the first backend roundtrip
- PowerModels validation result when applicable
- PowerModelsDistribution validation result for supported unbalanced OpenDSS routes

In practice, treat the JSON as the machine-readable certificate for a conversion run: it tells you which exports were produced and how closely the solved electrical state matched after conversion.

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

Quick interpretation:

- small slack mismatch means the overall power balance stayed consistent
- small voltage mismatch means the converted model preserved the electrical state bus by bus, or phase by phase for unbalanced cases
- a route should only be treated as signed off if it is both implemented and listed in the generated report

## Supported Abilities And Current Limits

Use this section as the practical trust boundary for your own models.

### Balanced routes

Current strengths:

- bus-branch transmission and distribution style AC cases with one clear slack source
- lines and constant-power loads across the validated subsets
- MATPOWER, pandapower, OpenDSS, CGMES/CIM, PyPSA, and PowerModels on their currently signed-off balanced subsets

Current limits:

- CGMES export is currently conservative and centered on a bus-branch subset
- PyPSA support is aimed at line-based AC models, not the full PyPSA component space
- broader OpenDSS control semantics are not yet universally signed off

### Unbalanced routes

Current strengths:

- native pandapower 3-phase models in the signed-off subset
- unbalanced OpenDSS feeders in the signed-off subset
- export and validation against `PowerModelsDistribution`

Current limits:

- not every unbalanced topology or component combination is covered yet
- regulator-heavy, switch-heavy, and broader feeder-library semantics are still outside the validated boundary
- `PowerModelsDistribution` is used as a validation/export backend, not yet as a full general import backend

## Known Limits

- SimBench is treated as a native pandapower-family import convenience, not as a separate exchange backend.
- CGMES import relies on pandapower's native CIM/CGMES loader, so fidelity is anchored to pandapower's supported CGMES semantics.
- CGMES export is currently limited to balanced transformer-free bus-branch models with one slack source, lines, and constant-power loads.
- The OpenDSS paths are signed off for the current balanced and three-phase subsets in this repo, not yet for arbitrary OpenDSS models with broader controls, switching, and equipment semantics.
- The PyPSA path is validated on the current line-based balanced subset and does not yet sign off transformer-, shunt-, link-, store-, or storage-unit-heavy PyPSA models.
- `PowerModelsDistribution` is validated as an export/solver backend only; import back into the canonical layer is not implemented.
- OpenDSS import currently targets a conservative subset:
  `Vsource`, `bus`, `line`, `transformer`, and `load`.
- `PowerModelsDistribution` is currently validated for the OpenDSS starter feeders and the native pandapower `ieee_european_lv_asymmetric` feeder, not yet for every possible unbalanced topology.
- The additional OpenDSS branched unbalanced feeder is validated through `PowerModelsDistribution`, but broader feeder libraries and regulator-heavy models are still outside the signed-off scope.
- PyPSA validation is currently limited to the transmission-style balanced AC subset represented in the generated report. Transformer-, shunt-, link-, store-, and storage-unit-heavy PyPSA models are not claimed as validated.
- `PowerModels.jl` is used here as a validation backend and package-native reference source, not yet as a general import backend into the canonical layer.
- `pypower`, PowerFactory, PSS/E, and PSCAD adapters are not implemented yet.
- The canonical schema is still pandapower-backed rather than a fully neutral multi-phase network model.
- The generated validation report is the source of truth for what is actually signed off in this branch. If a route is not listed there, it should not be treated as validated.

## Roadmap

1. decide whether SimBench should remain a native import convenience or move into a separate utility layer
2. expand OpenDSS support to regulators, capacitors, switches, and line-code heavy feeders such as IEEE 13-bus
3. extend the `PowerModelsDistribution` route beyond the OpenDSS starter feeder into broader canonical unbalanced exports
4. add broader explicit export adapters for MATPOWER and pandapower round-trips
5. add commercial-tool adapters behind optional environment-specific integrations
