# PowerModelConverter

Validation-first conversion platform for power system models.

The purpose of this repository is to become a common, verifiable interchange layer between grid-modeling tools. Instead of trusting that an imported or exported file is correct because it parsed successfully, this project treats load-flow agreement as the acceptance criterion.

This repository accompanies an academic paper submission on validation-first power-system model conversion. The paper citation and DOI will be added here once they are available.

For every supported route, the target is:

1. Quick validation via slack power
2. Full validation via complex bus voltages
3. For three-phase models, full validation via phase voltages per bus

## Quickstart

If you only want to translate one model and check whether that route is currently in scope:

```bash
./scripts/pmc-docker.sh build
./scripts/pmc-docker.sh precheck --source path/to/model --target-format pandapower
./scripts/pmc-docker.sh translate --source path/to/model --target-format pandapower
```

That gives you:

- a pinned runtime without local Julia or Python setup
- a route-level support check before export
- one exported target artifact
- one machine-readable validation result for that requested route

If you want the maintained full route inventory and the measured validation precision across the repo, open [validation_report.html](docs/validation_report.html).

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
- export balanced pandapower cases to CGMES/CIM including lines, transformers, constant-power loads, and one slack source, then validate by native pandapower re-import
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

- [validation_report.html](docs/validation_report.html)
- [validation_report.md](docs/validation_report.md)
- [validation_report.json](docs/validation_report.json)

## Validation Status

This repository is meant to be used as a verified converter, not just a parser.

The detailed route inventory, tested cases, and measured numerical precision are generated into:

- [validation_report.html](docs/validation_report.html)
- [validation_report.md](docs/validation_report.md)
- [validation_report.json](docs/validation_report.json)

Use those reports when you need the exact signed-off route matrix. The README stays focused on the researcher workflow: taking a model from one tool, converting it into another, and checking whether that conversion is within the validated scope of this repository.

If your goal is simpler and more operational, use the `Quickstart` and `User Workflow` sections first. Those are the shortest path from one source file to one validated exported artifact.

When you want the actual result numbers:

- open [validation_report.html](docs/validation_report.html) and read the `Balanced Precision`, `Unbalanced Precision`, and `Route Details` tables
- open [validation_report.md](docs/validation_report.md) for the same route tables in plain text
- open [validation_report.json](docs/validation_report.json) for machine-readable `slack_delta_mva`, `max_voltage_delta_pu`, and `compared_points` values per route

## Repository Layout

- [pyproject.toml](pyproject.toml)
  Python package metadata, dependencies, and CLI entrypoint.
- [src/powermodelconverter/core](src/powermodelconverter/core)
  Canonical case model, capability registry, and shared exceptions.
- [src/powermodelconverter/importers](src/powermodelconverter/importers)
  Source-format import logic that produces `CanonicalCase` instances backed by pandapower.
- [src/powermodelconverter/exporters](src/powermodelconverter/exporters)
  Target-format export logic that reads only from the canonical pandapower-backed case.
- [src/powermodelconverter/validation](src/powermodelconverter/validation)
  Solver runners, comparison logic, and balanced/unbalanced validation services.
- [src/powermodelconverter/report](src/powermodelconverter/report)
  Report loading, merge, and artifact generation helpers.
- [src/powermodelconverter/cli](src/powermodelconverter/cli)
  Command-line interface exposed as `pmc`.
- [src/powermodelconverter/julia](src/powermodelconverter/julia)
  Local Julia project used for `PowerModels` validation.
- [src/powermodelconverter/julia_pmd](src/powermodelconverter/julia_pmd)
  Local Julia project used for `PowerModelsDistribution` validation.
- [src/powermodelconverter/data/samples](src/powermodelconverter/data/samples)
  Starter model files used for validation and examples.
- [docs](docs)
  Generated validation inventory and future documentation.
- [tests](tests)
  Smoke tests and route-validation tests.
- [scripts](scripts)
  Environment bootstrap and report-generation scripts.

## Canonical Model

The canonical representation is intentionally pragmatic at this stage:

- `CanonicalCase.net` is a pandapower network and is the canonical representation
- importers translate source models into pandapower tables plus explicit case metadata
- exporters read from that canonical pandapower net rather than carrying tool-specific side data
- balanced versus unbalanced mode, phase count, provenance, and reference power-flow metadata are tracked on the case object

That means the repo is organized around a pragmatic pandapower-backed hub-and-spoke architecture: import into a canonical pandapower net, then export outward and validate against the target solver.

## Supported Tools

Current tool-level support is exposed by:

```bash
./.venv/bin/python -m powermodelconverter.cli.main capabilities
```

At a high level:

- `matpower`
  balanced import, balanced export, balanced validation
- `cgmes`
  balanced import, balanced export for the current supported subset (including lines, transformers, constant-power loads, and one slack source), balanced validation through native pandapower CGMES loading
- `pandapower`
  balanced import/export/validation and native unbalanced three-phase import/export/validation
- `opendss`
  balanced and unbalanced import/export/validation for the currently signed-off subsets
- `pypsa`
  balanced import/export/validation for the current transmission-style AC subset, with bus geodata carried into pandapower exports when available
- `powermodels`
  balanced export and balanced validation
- `powersystems`
  balanced MATPOWER-compatible advanced import/export path exists in the CLI, but it is not currently part of the maintained solver-validated route inventory
- `powermodelsdistribution`
  validated as an unbalanced backend for the OpenDSS starter feeder routes and the native pandapower `ieee_european_lv_asymmetric` feeder
- `pandapower_split`
  experimental unbalanced OpenDSS-focused phase-split pandapower export path for advanced workflows
- `pypower`
  balanced import and validation for the current signed-off static network subset

## Conversion Overview

The detailed signed-off route inventory lives in the generated validation reports. For day-to-day use, the tables below provide the quick visual overview.

### Balanced Tool Support

| Tool | Import | Export | Validation | Notes |
| --- | --- | --- | --- | --- |
| `pandapower` | Yes | Yes | Yes | Core balanced exchange backend and reference runtime in this repo |
| `matpower` | Yes | Yes | Yes | Balanced MATPOWER `.m` workflows are signed off |
| `opendss` | Yes | Yes | Yes | Balanced subset is signed off, not arbitrary OpenDSS semantics |
| `cgmes` | Yes | Yes | Yes | Export supports lines, transformers, constant-power loads, and one slack source; strict deterministic re-import validation may still show non-zero drift on very large cases |
| `pypsa` | Yes | Yes | Yes | Signed off for the current line-based AC subset; bus geodata is preserved into pandapower when present |
| `powermodels` | No | Yes | Yes | Validation/export backend for balanced AC models |
| `pypower` | Yes | No | Yes | Balanced Python-case import is supported for the signed-off static network subset |

### Balanced Conversion Matrix

`Yes` means the route is available in the current validated balanced toolchain. `Subset` means the route exists but only for the signed-off balanced component subset documented below.

| From \ To | `pandapower` | `matpower` | `opendss` | `cgmes` | `pypsa` | `powermodels` |
| --- | --- | --- | --- | --- | --- | --- |
| `pandapower` | Yes | Yes | Subset | Subset | Subset | Yes |
| `matpower` | Yes | Yes | Subset | Subset | Subset | Yes |
| `opendss` | Yes | Yes | Subset | Subset | Subset | Yes |
| `cgmes` | Yes | Yes | Subset | Subset | Subset | Yes |
| `pypsa` | Yes | Yes | Subset | Subset | Subset | Yes |

### Unbalanced Tool Support

| Tool | Import | Export | Validation | Notes |
| --- | --- | --- | --- | --- |
| `pandapower` | Yes | Yes | Yes | Native 3-phase pandapower subset is signed off |
| `opendss` | Yes | Yes | Yes | Starter three-phase feeder subset is signed off |
| `powermodelsdistribution` | Yes | Yes | Yes | Signed off for the current DSS-compatible unbalanced subset |

### Unbalanced Conversion Matrix

`Yes` means the route is available in the current validated unbalanced toolchain for the signed-off 3-phase subset.

| From \ To | `pandapower` | `opendss` | `powermodelsdistribution` |
| --- | --- | --- | --- |
| `pandapower` | Yes | Yes | Yes |
| `opendss` | Yes | Yes | Yes |
| `powermodelsdistribution` | Yes | Yes | Yes |

### Practical Meaning Of `Subset`

| Area | Currently Signed Off | Not Yet Claimed As Generally Validated |
| --- | --- | --- |
| Balanced CGMES | Bus-branch style balanced models with lines, transformers, constant-power loads, and one slack source | Broader CIM semantics such as switched topology/control-rich assets, unbalanced CIM, and strict zero-drift guarantees on very large imported cases |
| Balanced OpenDSS | Conservative AC subset used in the signed-off balanced routes | Broader control, switch, regulator, capacitor, and line-code-heavy semantics |
| Balanced PyPSA | Line-based AC transmission-style models in the validated set | Full PyPSA component space such as links, stores, storage units, and broader transformer/shunt-heavy cases |
| Unbalanced OpenDSS / PMD / pandapower | Native 3-phase feeder subset exercised by the validated starter and native asymmetric cases | Arbitrary feeder libraries, regulator-heavy cases, and wider advanced component combinations |

## Installation

### Recommended: Docker-first runtime

The repository now ships with a pinned container runtime so end users do not need local Python, Julia, MATPOWER, or PowerModels installations.

Build the image once:

```bash
./scripts/pmc-docker.sh build
```

Then use the same wrapper for the normal workflow:

```bash
./scripts/pmc-docker.sh capabilities
./scripts/pmc-docker.sh precheck --source path/to/model --target-format pypsa
./scripts/pmc-docker.sh translate --source path/to/model --target-format pandapower
```

This image pins:

- the Python runtime from the Docker image
- the Python dependency set declared in [pyproject.toml](pyproject.toml)
- Julia `1.12.3`
- the local Julia environments in [src/powermodelconverter/julia](src/powermodelconverter/julia)
- the local Julia environments in [src/powermodelconverter/julia_pmd](src/powermodelconverter/julia_pmd)

Because the current repository is mounted into the container, users can point at their own model files and the translated outputs are still written next to those source files on the host filesystem.

The Docker wrapper also bind-mounts the host directories passed via `--source` and `--output`, and it runs the container as your current host user so translated files come back on disk with normal ownership. Your input model does not need to live inside this repository.

The Docker wrapper also keeps the paper-validation path available without requiring local tooling:

```bash
./scripts/pmc-docker.sh test
./scripts/pmc-docker.sh report
./scripts/pmc-docker.sh shell
```

### Native local environment

Native local installation is still possible for development, but Docker is now the preferred path for reproducible end-user execution and version compatibility.

```bash
python3 -m venv .venv
./.venv/bin/pip install --upgrade pip setuptools wheel
./.venv/bin/pip install -e .
```

### Julia validation environment

```bash
bash scripts/bootstrap_julia_env.sh
```

That installs the local Julia dependencies used by the validation scripts in [src/powermodelconverter/julia](src/powermodelconverter/julia) and [src/powermodelconverter/julia_pmd](src/powermodelconverter/julia_pmd).

## License

This project is released under the BSD 3-Clause License. See [LICENSE](LICENSE).

## Sample Cases And Provenance

Included sample files:

- CGMES base case: [CGMES_v2.4.15_SmallGridTestConfiguration_BaseCase_Complete_v3.0.0.zip](src/powermodelconverter/data/samples/cgmes/CGMES_v2.4.15_SmallGridTestConfiguration_BaseCase_Complete_v3.0.0.zip)
- CGMES boundary case: [CGMES_v2.4.15_SmallGridTestConfiguration_Boundary_v3.0.0.zip](src/powermodelconverter/data/samples/cgmes/CGMES_v2.4.15_SmallGridTestConfiguration_Boundary_v3.0.0.zip)
- MATPOWER: [case9.m](src/powermodelconverter/data/samples/matpower/case9.m)
- OpenDSS starter case: [minimal_radial.dss](src/powermodelconverter/data/samples/opendss/minimal_radial.dss)
- OpenDSS balanced chained feeder: [minimal_chain.dss](src/powermodelconverter/data/samples/opendss/minimal_chain.dss)
- OpenDSS unbalanced starter feeder: [minimal_unbalanced_3ph.dss](src/powermodelconverter/data/samples/opendss/minimal_unbalanced_3ph.dss)
- OpenDSS unbalanced branched feeder: [minimal_unbalanced_branch.dss](src/powermodelconverter/data/samples/opendss/minimal_unbalanced_branch.dss)
- OpenDSS IEEE benchmark feeder: [IEEE13Nodeckt.dss](src/powermodelconverter/data/samples/opendss/IEEE13Nodeckt.dss)
- pandapower 3-phase: [ieee_european_lv_asymmetric.json](src/powermodelconverter/data/samples/pandapower/ieee_european_lv_asymmetric.json)

These samples serve as reference cases for the validated conversion routes and as minimal examples of the supported input structure.

The practical provenance split is:

- the bundled CGMES ZIPs are official SmallGrid sample files used for pandapower CGMES workflows in the validation report
- `case9.m` is the standard MATPOWER `case9` reference case
- `ieee_european_lv_asymmetric.json` is a bundled pandapower-native asymmetric reference case used for the signed-off three-phase route
- the `minimal_*` OpenDSS cases are small bundled repo examples used to exercise the currently signed-off balanced and unbalanced OpenDSS subsets
- `IEEE13Nodeckt.dss` is the bundled IEEE 13-node OpenDSS feeder kept for heavier unbalanced importer smoke coverage and route-development work

This repository is intended to include only sample cases that are either standard reference examples, tool-native examples, or small bundled validation fixtures. When in doubt, use your own source files outside the repo and keep generated outputs outside version control.

## User Workflow

### 1. Build the pinned runtime once

```bash
./scripts/pmc-docker.sh build
```

That container includes the Python runtime, the package dependencies, Julia `1.12.3`, and the local Julia validation environments used by `PowerModels` and `PowerModelsDistribution`.

### 2. Check whether your source and target tools are in scope

```bash
./scripts/pmc-docker.sh capabilities
```

Use this first. It is the quick way to see whether the source and target ecosystem you care about are implemented in this branch, and whether the support is balanced, unbalanced, or limited to a subset. The maintained report in this branch currently contains `174` validated route records and no pending entries.

### 3. Precheck one source-to-target route before exporting

```bash
./scripts/pmc-docker.sh precheck \
  --source src/powermodelconverter/data/samples/matpower/case9.m \
  --target-format pypsa
```

This is the lightweight route check for end users. It auto-detects the source format in the common file-based cases, summarizes the populated components, dry-runs the requested export, and reports support blockers before you commit to a conversion.

### 4. Translate to one chosen target and validate that route

The main end-user entrypoint is now `translate`. You provide one source model and one destination format, and the CLI exports only that target and runs the full deterministic validation for that path.

Typical pattern:

```bash
./scripts/pmc-docker.sh translate \
  --source <path-to-your-model> \
  --target-format <target-format>
```

Supported `target-format` values in the current repo are `pandapower`, `powermodels`, `powermodelsdistribution`, `pypsa`, `opendss`, and `cgmes` for their currently signed-off subsets.

Advanced target formats also exist:

- `powersystems` for the balanced MATPOWER-compatible PowerSystems.jl route
- `pandapower_split` for the experimental OpenDSS-focused phase-split export path

Those advanced targets are real CLI options, but the main user workflow in this README stays focused on the more established end-user paths first.

By default, the exported file is written next to the source model with the same base name and the target suffix. In the common case, users only need to provide:

- the source path
- the target format

Absolute host paths outside the repository are supported. The Docker wrapper automatically mounts the source directory and, if needed, the explicit output directory into the container.

Use `--source-format` only when auto-detection is ambiguous, and `--output` only when you want the artifact somewhere else.

### 5. Example: MATPOWER to PyPSA

```bash
./scripts/pmc-docker.sh translate \
  --source src/powermodelconverter/data/samples/matpower/case9.m \
  --target-format pypsa
```

### 6. Example: OpenDSS to pandapower

```bash
./scripts/pmc-docker.sh translate \
  --source src/powermodelconverter/data/samples/opendss/minimal_radial.dss \
  --target-format pandapower
```

### 7. Example: native pandapower to PowerModelsDistribution

```bash
./scripts/pmc-docker.sh translate \
  --source src/powermodelconverter/data/samples/pandapower/ieee_european_lv_asymmetric.json \
  --target-format powermodelsdistribution
```

### 8. Run the full maintained route checks after larger internal changes

The maintainer-oriented command is still `validate`. It fans out into the currently supported exports for the selected source family and should still be rerun whenever converter logic changes beyond the outer interface.

```bash
./scripts/pmc-docker.sh validate \
  --source-format matpower \
  --source src/powermodelconverter/data/samples/matpower/case9.m
```

For paper-grade regression checks inside the pinned runtime, the same container can also run:

```bash
./scripts/pmc-docker.sh report
./scripts/pmc-docker.sh test
```

### 9. Interpret the conversion result

Treat the `translate` output as the conversion certificate for one route.

For day-to-day use, the practical reading is:

1. confirm that the precheck reported the route as supported
2. confirm that the requested target artifact was created
3. check the slack mismatch to see whether the overall power balance stayed consistent
4. check the voltage mismatch to see whether the solved state was preserved bus by bus, or phase by phase for unbalanced cases
5. consult [validation_report.html](docs/validation_report.html) when you need the maintained signed-off routes and measured precision

## CLI Output

The `precheck` command prints JSON with:

- `case_id`
- `source_format`
- `target_format`
- `component_counts`
- whether the route is currently supported
- any precheck issues found during dry-run export
- the validation mode that will be used for the requested target

In the common file-based cases, `source_format` is auto-detected from the input path and file contents.

The `translate` command prints JSON with:

- `case_id`
- `source_format`
- `target_format`
- `precheck`
- source-side validation result
- target export path
- target validation result

If `--output` is not provided, `target_export` defaults to the same directory as the source model with the same base name and the target file extension.

The `validate` command prints JSON with:

- `case_id`
- `source_format`
- `is_unbalanced`
- export artifact paths
- initial validation result against the first backend roundtrip
- PowerModels validation result when applicable
- PowerModelsDistribution validation result for supported unbalanced OpenDSS routes

In practice:

- use `translate` as the machine-readable certificate for one requested route
- use `validate` as the maintainer-side regression command after significant converter changes
- use the generated validation report when you want the maintained full-route inventory and the per-route numeric result values

## Docker Assets

The repository ships with:

- [Dockerfile](Dockerfile)
- [docker-compose.yml](docker-compose.yml)
- [pmc-docker.sh](scripts/pmc-docker.sh)

Together they provide the pinned end-user runtime and keep the full validation pipeline available for maintainers.

The helper script supports:

- `./scripts/pmc-docker.sh build`
- `./scripts/pmc-docker.sh capabilities`
- `./scripts/pmc-docker.sh precheck ...`
- `./scripts/pmc-docker.sh translate ...`
- `./scripts/pmc-docker.sh validate ...`
- `./scripts/pmc-docker.sh test`
- `./scripts/pmc-docker.sh report`
- `./scripts/pmc-docker.sh shell`

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

Quick interpretation for researchers:

- small slack mismatch means the overall power balance stayed consistent
- small voltage mismatch means the converted model preserved the electrical state bus by bus, or phase by phase for unbalanced cases
- a route should only be treated as signed off if it is both implemented and listed in the generated report

## Supported Abilities And Current Limits

Use this section as the practical trust boundary when deciding whether a published or in-house model is inside the validated conversion scope.

### Balanced routes

Current strengths:

- bus-branch transmission and distribution style AC cases
- lines, transformers, and constant-power loads across the validated subsets
- MATPOWER, pandapower, OpenDSS, CGMES/CIM, PyPSA, and PowerModels on their currently signed-off balanced subsets

Current limits:

- CGMES export/import on very large cases can still show small non-zero deterministic drift after re-import validation
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
- CGMES export currently supports balanced lines, transformers, constant-power loads, and one slack source; strict zero-drift guarantees are not claimed for very large roundtripped cases.
- The OpenDSS paths are signed off for the current balanced and three-phase subsets in this repo, not yet for arbitrary OpenDSS models with broader controls, switching, and equipment semantics.
- The PyPSA path is validated on the current line-based balanced subset and does not yet sign off transformer-, shunt-, link-, store-, or storage-unit-heavy PyPSA models.
- `PowerModelsDistribution` support is validated for the current DSS-compatible unbalanced subset, but not yet for arbitrary feeder libraries or every possible PMD-authored workflow.
- OpenDSS import currently targets a conservative subset:
  `Vsource`, `bus`, `line`, `transformer`, and `load`.
- `PowerModelsDistribution` is currently validated for the OpenDSS starter feeders and the native pandapower `ieee_european_lv_asymmetric` feeder, not yet for every possible unbalanced topology.
- The additional OpenDSS branched unbalanced feeder is validated through `PowerModelsDistribution`, but broader feeder libraries and regulator-heavy models are still outside the signed-off scope.
- PyPSA validation is currently limited to the transmission-style balanced AC subset represented in the generated report. Transformer-, shunt-, link-, store-, and storage-unit-heavy PyPSA models are not claimed as validated.
- `PowerModels.jl` is used here as a validation backend and package-native reference source, not yet as a general import backend into the canonical layer.
- `PowerSystems.jl` support currently exists as an advanced CLI path, but it is not claimed as part of the maintained solver-validated route inventory in the generated report.
- `PowerFactory`, `PSS/E`, and `PSCAD` adapters are not implemented yet. `pypower` now has balanced import plus validation coverage for the current static network subset.
- The canonical schema is still pandapower-backed rather than a fully neutral multi-phase network model.
- The generated validation report is the source of truth for what is actually signed off in this branch. If a route is not listed there, it should not be treated as validated.

## Roadmap

1. decide whether SimBench should remain a native import convenience or move into a separate utility layer
2. expand OpenDSS support to regulators, capacitors, switches, and line-code heavy feeders such as IEEE 13-bus
3. extend the `PowerModelsDistribution` route beyond the OpenDSS starter feeder into broader canonical unbalanced exports
4. add broader explicit export adapters for MATPOWER and pandapower round-trips
5. add commercial-tool adapters behind optional environment-specific integrations
