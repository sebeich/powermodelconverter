# UC1 Diagnosis Notes

## Scope

This note records the UC1 debugging work for the failing `pandapower -> powermodels` validation leg on `pglib_opf_case14_ieee` and `pglib_opf_case30_ieee__api`.

## Candidate A: generator cost curve transcription

Tested first as requested, but this candidate could not explain the observed failure mode:

- the failing metric was the AC power-flow validation leg, not the OPF objective
- PowerModels AC PF on the original PGLib MATPOWER files matched pandapower bus voltages for all three cases to numerical precision
- therefore the QC/SOC baseline divergence was downstream of a data-model mismatch before OPF, not caused by cost curves directly

Result:

- not the root cause of the PF validation failure
- cost scaling still matters for OPF reproducibility and was corrected together with the exporter normalization step

## Candidate B: transformer per-unit rescaling

Tested by comparing three representations for the same case:

1. original PGLib MATPOWER branch rows
2. pandapower's intermediate `ppci`
3. the emitted PowerModels JSON

Findings:

- the imported pandapower `ppci` matched the original MATPOWER branch rows exactly, including the case30 transformer rows
- the drift appeared only in the final `ppc -> PowerModels JSON` serialization
- pandapower's PM serializer was emitting branch terms on an effective 1 MVA base:
  - `br_r`, `br_x`, `g_fr`, `g_to` were divided by `baseMVA`
  - `b_fr`, `b_to` were multiplied by `baseMVA`
  - branch ratings were left in MVA instead of being normalized
- this distorted the PowerModels case enough to break the larger PGLib cases

Result:

- confirmed root cause for the branch-side mismatch
- fixed in `PandapowerAdapter.export_powermodels_json()` by post-normalizing the emitted PowerModels JSON to the same conventions produced by `PowerModels.parse_file()` on the original MATPOWER cases

## Candidate C: shunt handling

Tested by removing shunts from the exported case and re-running validation.

Findings:

- for `case14`, removing shunts made the broken export pass before the serializer fix
- for `case30`, removing shunts did not resolve the failure
- direct comparison against `PowerModels.parse_file()` showed that the shunt sign in the exported JSON was already correct
- the apparent `case14` "shunt bug" was actually a symptom of the broader base-normalization bug: the shunts were already in the expected per-unit convention while loads, generators, and branch terms were not

Result:

- not an independent sign bug
- no separate shunt sign flip was required once the serializer normalization was fixed

## Root Cause Summary

The failure was in the final pandapower-to-PowerModels JSON serialization, not in the MATPOWER import.

The emitted JSON mixed conventions:

- loads and generators remained in MW / MVAr
- branch series terms were additionally scaled by `1 / baseMVA`
- branch charging terms were scaled by `baseMVA`
- branch ratings stayed in MVA
- the slack-bus voltage bounds were clamped to the fixed setpoint instead of preserving the benchmark bounds

This mixed-unit export made the validation leg fail on `case14` and `case30`, and it also explained the downstream relaxation-gap discrepancy.

## Follow-up

The Julia validation harness was also updated to report slack injection back in MW / MVAr, because after the exporter fix the solve results correctly returned per-unit generator outputs.
