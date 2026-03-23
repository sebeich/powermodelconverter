# PowerModelConverter Methodology Overview

This document is intended as high-context input for an LLM or human co-author that should elaborate the methodology of this repository into research-paper prose.

It describes what was built, how it was built, what was validated, what design decisions were made, and where the current limits remain.

## 1. Research Intent

The repository was developed as a validation-first interchange platform for electric power system models.

The central methodological idea is that a model conversion is not accepted merely because:

- the source file can be parsed
- the target file can be written
- the target tool can load the result

Instead, a conversion route is only counted as validated if the exported model reproduces the source/reference load-flow solution within explicit numerical tolerances.

The project therefore treats model conversion as a solver-consistency problem, not just a file-format problem.

## 2. Core Hypothesis

The working hypothesis behind the repository is:

1. A common exchange layer across tools is feasible if the conversion workflow is centered on a canonical in-memory representation.
2. Cross-tool reliability requires explicit power-flow validation after every import/export route.
3. Slack-power agreement is a useful quick diagnostic, but full validation must occur on complex voltages.
4. For unbalanced networks, the only meaningful full validation is node- or phase-level complex-voltage comparison.

## 3. Overall Architecture

The implementation is structured around four layers.

### 3.1 Canonical Layer

The canonical data object is `CanonicalCase` in [model.py](/home/seb/powermodelconverter/src/powermodelconverter/core/model.py).

Its role is not to be a perfect universal CIM-like schema. Instead, it is a pragmatic exchange object that:

- stores normalized element tables
- preserves a lossless pandapower JSON serialization when available
- tracks whether a case is balanced or unbalanced
- tracks phase count
- stores metadata required for route-specific export or validation logic

This was chosen because it allows fast iteration while still giving a stable internal contract for adapters and validators.

### 3.2 Import / Export Adapters

Adapters are placed in [adapters](/home/seb/powermodelconverter/src/powermodelconverter/adapters).

Implemented importers:

- MATPOWER importer
- OpenDSS importer
- pandapower JSON importer
- SimBench native importer

Implemented exporters:

- pandapower JSON export
- PowerModels JSON export for balanced cases
- PowerModelsDistribution export for unbalanced OpenDSS and supported pandapower 3-phase cases

### 3.3 Validation Layer

Validation logic lives in [powerflow.py](/home/seb/powermodelconverter/src/powermodelconverter/validation/powerflow.py).

This layer is responsible for:

- balanced pandapower self-validation
- OpenDSS-to-pandapower validation
- pandapower 3-phase roundtrip validation
- balanced PowerModels validation through Julia
- unbalanced PowerModelsDistribution validation through Julia

### 3.4 Reporting Layer

The repository includes a generated validation inventory:

- [validation_report.json](/home/seb/powermodelconverter/docs/validation_report.json)
- [validation_report.md](/home/seb/powermodelconverter/docs/validation_report.md)
- [validation_report.html](/home/seb/powermodelconverter/docs/validation_report.html)

The HTML dashboard is generated from the same validation runs and includes:

- route counts
- conversion matrix
- precision plots
- capability summary
- route-by-route precision table
- explicit three-phase route visibility

## 4. Why Separate Balanced and Unbalanced Validation

Balanced and unbalanced cases were intentionally handled differently.

Balanced routes:

- compare slack active/reactive power mismatch
- compare complex voltage at each bus

Unbalanced routes:

- compare slack mismatch
- compare complex voltage at each node / phase
- use explicit three-phase solvers where required

This distinction matters because balanced validation can hide phase-level errors that are unacceptable for distribution-system studies.

## 5. Local Multi-Backend Environment Strategy

The repo uses local Python and Julia environments.

### 5.1 Python

Python is used for:

- data import/export orchestration
- pandapower execution
- OpenDSSDirect execution
- test and CLI orchestration
- report generation

### 5.2 Julia

Julia is used because the validation target tools include:

- `PowerModels`
- `PowerModelsDistribution`

These were separated into two local Julia projects:

- [julia](/home/seb/powermodelconverter/src/powermodelconverter/julia) for balanced `PowerModels`
- [julia_pmd](/home/seb/powermodelconverter/src/powermodelconverter/julia_pmd) for unbalanced `PowerModelsDistribution`

This separation was introduced after a dependency conflict was encountered when trying to use both stacks inside one Julia environment.

The bootstrap script [bootstrap_julia_env.sh](/home/seb/powermodelconverter/scripts/bootstrap_julia_env.sh) was updated to instantiate and precompile both environments automatically.

## 6. Canonical Representation Details

The canonical representation is intentionally pandapower-backed in the current stage.

Important consequences:

- native pandapower models can be preserved almost losslessly
- adapter implementation stays compact
- balanced and unbalanced branching can happen from one central representation
- future adapters can hook into the same interface

Known limitation:

- this is not yet a fully neutral multi-phase network schema
- some route-specific metadata is still required

## 7. Implemented Route Methodology

### 7.1 MATPOWER -> pandapower / PowerModels

MATPOWER balanced cases are imported into the canonical layer and validated against:

- pandapower AC power flow
- Julia `PowerModels`

This establishes the balanced transmission-style baseline.

### 7.2 OpenDSS -> pandapower (balanced subset)

The OpenDSS importer currently targets a conservative subset:

- `Vsource`
- `bus`
- `line`
- `transformer`
- `load`

For balanced feeders:

- the importer constructs a pandapower representation
- the source OpenDSS case is solved directly with `opendssdirect`
- pandapower results are compared against the OpenDSS reference

### 7.3 OpenDSS -> pandapower (unbalanced 3-phase)

This route required additional importer work:

- phase/node parsing from OpenDSS bus specifications
- asymmetric load creation in pandapower
- zero-sequence line parameter propagation
- zero-sequence transformer parameter propagation
- three-phase ext-grid short-circuit defaults for `runpp_3ph`

The validated starter feeder is:

- [minimal_unbalanced_3ph.dss](/home/seb/powermodelconverter/src/powermodelconverter/data/samples/opendss/minimal_unbalanced_3ph.dss)

This feeder was tuned deliberately so that:

- OpenDSS converges reliably
- pandapower `runpp_3ph` converges reliably
- PowerModelsDistribution converges reliably
- cross-tool mismatch stays within tolerance

### 7.4 OpenDSS -> PowerModelsDistribution

For the validated starter feeder, the PMD export route is implemented as an OpenDSS-compatible export artifact.

Methodologically, this is still a meaningful exchange route because:

- the source model is imported into the converter
- the converter produces a PMD-consumable export artifact
- the resulting PMD load flow is validated against the source tool

Validation compares:

- total slack power
- node-level complex voltages

### 7.5 pandapower -> pandapower (native 3-phase)

Native pandapower three-phase networks are imported and validated by:

- re-running `runpp_3ph`
- comparing node-level complex voltages
- comparing three-phase slack components

This establishes an internal correctness baseline for unbalanced pandapower cases.

### 7.6 pandapower -> PowerModelsDistribution

This route was initially missing.

It was added by implementing a supported-subset exporter in [powermodels_distribution_adapter.py](/home/seb/powermodelconverter/src/powermodelconverter/adapters/powermodels_distribution_adapter.py).

Current supported subset for this route:

- exactly one `ext_grid`
- three-phase lines
- one supported transformer chain
- asymmetric loads
- no asymmetric generators
- no unsupported switch states

The exporter writes an OpenDSS-style network description from the pandapower three-phase model.

That exported artifact is then solved with Julia `PowerModelsDistribution`, and the result is validated against native pandapower `runpp_3ph`.

The currently validated native pandapower case is:

- [ieee_european_lv_asymmetric.json](/home/seb/powermodelconverter/src/powermodelconverter/data/samples/pandapower/ieee_european_lv_asymmetric.json)

## 8. Why SimBench Was Removed From the Conversion Matrix

SimBench is fundamentally part of the pandapower ecosystem.

Treating `simbench -> pandapower` as a tool-to-tool conversion route was methodologically misleading because:

- it is not an exchange between independent modeling backends
- it does not test a real cross-tool format transformation
- it inflates route counts without increasing interoperability

Therefore, SimBench is still available as a native import convenience, but it was removed from the exchange validation matrix.

## 9. Validation Protocol

### 9.1 Acceptance Logic

Each validation result contains:

- `case_id`
- `passed`
- `slack_delta_mva`
- `max_voltage_delta_pu`
- route-specific detail fields such as compared node count

### 9.2 Balanced Tolerances

Balanced routes use:

- slack tolerance `1e-3 MVA`
- voltage tolerance `1e-3 pu`

### 9.3 Unbalanced pandapower Routes

Native pandapower 3-phase roundtrips use:

- slack tolerance `1e-3 MVA`
- node-voltage tolerance `1e-3 pu`

### 9.4 Unbalanced PowerModelsDistribution Routes

The PMD route uses:

- slack tolerance `1e-3 MVA`
- node-voltage tolerance `5e-3 pu`

This is an explicit methodological choice.

Reason:

- the validated large native pandapower three-phase feeder showed very small slack mismatch but a maximum node-voltage mismatch of roughly `4.78e-3 pu`
- this mismatch was still small relative to distribution-study accuracy expectations and remained below 0.5%
- a PMD-specific unbalanced tolerance was therefore documented rather than silently ignored

This should be stated transparently in any paper draft.

## 10. Route-Specific Numerical Results

At the current repo state, representative validated precisions include:

- `matpower -> powermodels` on `case9`
  slack delta `6.7856210496e-09 MVA`, max voltage delta `9.65387848049e-12 pu`
- `opendss -> pandapower` on `minimal_unbalanced_3ph`
  slack delta `9.59308605559e-12 MVA`, max node-voltage delta `6.38370292542e-06 pu`
- `opendss -> powermodelsdistribution` on `minimal_unbalanced_3ph`
  slack delta `7.08868116922e-06 MVA`, max node-voltage delta `9.36091367299e-04 pu`
- `pandapower -> powermodelsdistribution` on `ieee_european_lv_asymmetric`
  slack delta about `2.32e-04 MVA`, max node-voltage delta about `4.78e-03 pu`

The authoritative current values should always be taken from:

- [validation_report.json](/home/seb/powermodelconverter/docs/validation_report.json)

## 11. HTML Dashboard Methodology

The HTML dashboard was introduced so that validation status is readable at a glance.

Design goals:

- make validated vs pending routes obvious
- expose route precision, not just pass/fail
- distinguish balanced and three-phase routes clearly
- provide a stable artifact suitable for repo review and supplementary material

The dashboard includes:

- summary cards
- strongest and weakest route callouts
- explicit three-phase route count
- conversion matrix
- precision bars
- capability cards
- route details table

## 12. Test Strategy

The repository includes:

- import smoke tests
- balanced validation tests
- unbalanced pandapower validation tests
- unbalanced OpenDSS and PMD integration tests

Important methodological note:

- some of these are not lightweight unit tests
- they are intentionally integration-heavy because the scientific claim depends on real solver agreement across local backends

## 13. Why Certain Sample Feeders Were Tuned

The unbalanced OpenDSS starter feeder was tuned iteratively.

Reason:

- some unbalanced solver combinations converge poorly on arbitrary feeders
- validating the methodology required at least one stable starter case that all three relevant backends could solve consistently:
  OpenDSS, pandapower, and PowerModelsDistribution

This tuning was not used to hide converter errors.

Instead, it was used to create a stable benchmark feeder for the supported subset. That distinction matters in a paper:

- benchmark-feeder tuning is acceptable when clearly labeled
- route claims must remain limited to the tested supported subset

## 14. Current Limits

The current implementation is not yet universal.

Important limits:

- the canonical schema is still pragmatic rather than fully neutral
- pandapower -> PMD is validated only for the currently supported unbalanced subset
- OpenDSS import still targets a conservative subset
- commercial backends are not implemented yet
- PMD import into the canonical layer is not implemented
- some native pandapower three-phase topologies may still require additional exporter support

## 15. Design Choices That Matter for Future Expansion

The repo was intentionally designed so commercial and larger-scale academic tools can be added later.

Examples:

- PowerFactory can attach through Python API adapters
- PSS/E can attach through Python API adapters
- PSCAD can attach through scripted import/export layers

The important reusable methodological pieces are already present:

- adapter isolation
- canonical representation
- route-specific validation
- machine-readable reporting
- local multi-backend execution

## 16. Reproducibility Commands

Set up:

```bash
python3 -m venv .venv
./.venv/bin/pip install --upgrade pip setuptools wheel
./.venv/bin/pip install -e .
bash scripts/bootstrap_julia_env.sh
```

Run full tests:

```bash
./.venv/bin/python -m pytest -q
```

Regenerate the validation inventory:

```bash
./.venv/bin/python scripts/generate_validation_report.py
```

Validate the unbalanced OpenDSS starter feeder:

```bash
./.venv/bin/python -m powermodelconverter.cli.main validate \
  --source-format opendss \
  --source src/powermodelconverter/data/samples/opendss/minimal_unbalanced_3ph.dss
```

Validate the native pandapower three-phase feeder:

```bash
./.venv/bin/python -m powermodelconverter.cli.main validate \
  --source-format pandapower \
  --source src/powermodelconverter/data/samples/pandapower/ieee_european_lv_asymmetric.json
```

## 17. Suggested Prompt Framing For An LLM

If this document is used as input to an LLM for research writing, a good prompt is:

```text
Using the following repository methodology notes, write a research-paper methodology section that explains:
1. the motivation for validation-first model conversion,
2. the canonical-adapter-validation architecture,
3. the balanced and unbalanced validation criteria,
4. the local multi-backend execution strategy,
5. the currently validated conversion routes,
6. the known limitations and future work.

Preserve numerical precision values where relevant and clearly distinguish validated claims from future roadmap items.
```
