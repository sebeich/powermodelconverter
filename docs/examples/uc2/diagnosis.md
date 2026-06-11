# UC2 Diagnosis

## Scope

This note records the root-cause analysis for the failing `OpenDSS -> pandapower -> PyPSA`
route that originally blocked the research-grade UC2 example.

The target route is the balanced phase-sum workflow for the IEEE European Low Voltage Test
Feeder. The same failure was first reproduced on the smaller `minimal_radial.dss` case to
isolate the seam before touching the benchmark feeder.

## 1. Minimal reproduction

Initial `minimal_radial.dss` validation on the live branch:

- `OpenDSS -> pandapower`: passed
  - `slack_delta_mva = 2.3977e-4`
  - `max_voltage_delta_pu = 9.3157e-4`
- `pandapower -> PyPSA`: failed
  - `slack_delta_mva = 1.3721e-2`
  - `max_voltage_delta_pu = 8.6003e-3`

This immediately localized the red state to the `pandapower -> PyPSA` seam, not the
OpenDSS importer.

## 2. Element-class ablation on `minimal_radial`

The chain was rebuilt incrementally:

- lines only: green
- lines + loads: green
- full case with transformer: red

The transformer class was the first element that flipped the route from green to red.

## 3. Transformer export diagnosis

Comparison against PyPSA's own `import_from_pandapower_net` behavior showed that the
manual exporter was correct in intent, but the previous path used the upstream PyPSA
importer directly, and on this branch/runtime that importer produced transformer
impedances that were off by a factor of roughly ten on the minimal case.

Observed on `minimal_radial`:

- expected transformer series terms: about `r = 0.01`, `x = 0.05916`
- exported-through-upstream-PyPSA path: about `r = 0.001`, `x = 0.005916`

That impedance shrinkage explains the non-physical voltage and slack mismatch in the
PyPSA validation leg.

### Fix

`src/powermodelconverter/importers/pypsa.py`

- replaced the direct `network.import_from_pandapower_net(...)` path with an explicit
  `pandapower_to_pypsa()` mapping
- mapped buses, lines, transformers, shunts, loads, ext-grids, gens, and sgens directly
- preserved transformer `phase_shift`, `tap_side`, and impedance terms explicitly

### Result

Post-fix `minimal_radial.dss`:

- `pandapower -> PyPSA`: passed
  - `slack_delta_mva = 8.3167e-14`
  - `max_voltage_delta_pu = 6.1934e-16`

Regression check on `minimal_chain.dss` also passed:

- `slack_delta_mva = 6.2353e-7`
- `max_voltage_delta_pu = 3.1803e-9`

## 4. IEEE European LV benchmark: second bug

After the PyPSA exporter fix, the official IEEE European LV feeder still showed a severe
per-unit mismatch on the aggregated balanced case.

Inspection of the imported pandapower net showed:

- HV source bus nominal voltage: `11.0 kV` (correct)
- LV buses `1`, `2`, `34`, ... nominal voltage: also `11.0 kV` (wrong)

The physical secondary voltage magnitude was correct in absolute terms, but pandapower
was expressing it on the wrong nominal base, so LV buses appeared around `0.04 pu`
instead of about `1.05 pu`.

This was traced to the OpenDSS importer's bus-base inference:

- the LV side is single-phase at many buses
- `Bus.kVBase()` was not reliable as a final bus nominal-voltage oracle on this feeder
- all buses downstream of the transformer inherited the primary-side base unless corrected

### Transformer phase-shift sub-fix

Before fixing bus bases, the IEEE feeder also exposed a missing delta-wye transformer
phase shift in the OpenDSS importer:

- official feeder transformer connections: `delta -> wye`
- correct imported shift: `+30 degrees`

Without that shift, the balanced positive-sequence comparison stayed above tolerance.

### Fix

`src/powermodelconverter/importers/opendss.py`

- parsed transformer winding connections
- derived `shift_degree` and `vector_group`
- applied `+30 deg` for `delta -> wye`, `-30 deg` for `wye -> delta`
- added `_reconcile_bus_nominal_voltages(net)` after transformer creation
- seeded voltage islands from transformer HV/LV nominal voltages and propagated those
  nominal bases across line- and bus-switch-connected components

### Result

The imported IEEE feeder now has:

- `sourcebus.vn_kv = 11.0`
- LV buses such as `1`, `2`, `34`: `vn_kv = 0.416`

That removed the per-unit-base distortion on the aggregated feeder.

## 5. Official benchmark validation oracle

One more issue remained: the reconstructed OpenDSS CSV wrapper compiled successfully in
OpenDSSDirect, but the runtime voltage accessors (`AllBusMagPu`, `Bus.puVmagAngle`,
`YNodeVArray`, `TotalPower`) returned zeros on this feeder even while `Solution.Converged()`
reported `True`.

This made the live OpenDSSDirect solve unreliable as a validation oracle for the official
CSV package, even though the imported network itself was structurally correct.

Further isolation showed that this is not a general OpenDSSDirect failure:

- `minimal_radial.dss` works correctly in the same compose image and through the same
  accessor calls
- a tiny probe circuit using `New Circuit ...` plus a transformer written in the
  compact vector form
  - `buses=(SourceBus,1) conns=(Delta,Wye) kvs=(11,0.416) ...`
  returned zeros
- the same probe circuit works immediately if either
  - the delta bus nodes are made explicit
    - `buses=(SourceBus.1.2.3,1.1.2.3) ...`
  - or the transformer is written in per-winding multiline form
    - `~ wdg=1 bus=SourceBus conn=delta ...`
    - `~ wdg=2 bus=1 conn=wye ...`
- compact vector-form transformers without delta windings, e.g. `wye-wye`, do not show
  this failure in the same runtime

### Root cause classification

- not feeder-specific: reproduced on a tiny synthetic probe
- not a general package-version failure: the same runtime solves other DSS files correctly
- not a pmc voltage-accessor bug: the zero state originates inside the OpenDSSDirect solve
  for that specific wrapper syntax

The issue is therefore a wrapper-construction pitfall in this OpenDSSDirect / DSS C-API
stack:

- runtime:
  - `OpenDSSDirect.py 0.9.4`
  - `DSS C-API 0.14.5`
- trigger:
  - compact vector-form transformer declaration with a delta-connected winding and
    implicit bus nodes
- safe forms:
  - explicit delta bus nodes, or
  - per-winding multiline transformer definitions

This matters beyond UC2 because any future generated OpenDSS wrappers, including UC3
support files, should emit the safe transformer form.

To avoid validating against this runtime/wrapper edge case, the route was checked against
the published OpenDSS snapshot shipped with the IEEE package:

- workbook: `Solutions/OpenDSS/Snapshots/Snapshot_1min_Initialization_Off Peak.xlsx`
- validation operating point: first multiplier from each published load profile
- comparison metric: positive-sequence bus voltage phasors vs. the published snapshot

### Result on the official IEEE European LV feeder

Balanced phase-sum aggregate vs. published OpenDSS snapshot:

- compared buses: `907`
- `max_voltage_delta_pu = 1.2557e-4`

Balanced aggregate `pandapower -> PyPSA`:

- `slack_delta_mva = 9.5207e-10`
- `max_voltage_delta_pu = 5.0309e-11`

Both legs are comfortably inside the `1e-3` balanced tolerance tier.

## Conclusion

UC2 required two real adapter fixes:

1. explicit transformer-preserving `pandapower -> PyPSA` export
2. transformer-aware nominal-voltage reconciliation in the OpenDSS importer

The remaining OpenDSSDirect issue is not an adapter mismatch in pmc itself; it is a
runtime/reference-extraction problem for the reconstructed IEEE CSV package. For the
research-grade UC2 benchmark, the published OpenDSS snapshot workbook is therefore the
correct reference artifact for source-side validation.
