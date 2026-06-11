# UC3 Diagnosis

## Scope

This note records the root-cause analysis for the failing CGMES roundtrip leg
(`CGMES -> pandapower -> CGMES -> pandapower`) that surfaced while promoting UC3 from the
old smoke-network sketch to the research-grade reproducer on the full ENTSO-E SmallGrid
BaseCase (118 buses, 176 lines, 10 transformers, 19 generators).

The previous roundtrip check (`docs/examples/uc3_cgmes_roundtrip.py`) exported a minimal
two-bus network with an external grid, a line, and a load. That network was green at
`~1e-13`, but it contained no `gen` and no `trafo` elements — which is exactly where the
two real defects below were hiding.

## 1. Reproduction

Re-importing the exported SmallGrid package failed outright:

- `pandapower.LoadflowNotConverged` on the re-imported network (Newton-Raphson and
  Gauss-Seidel, flat and auto init).

Element counts, topology, line impedance totals, load sums, generator voltage setpoints,
transformer ratings, and tap data all matched the exported source — so the defect had to
sit in a quantity that flips the operating point rather than the structure.

## 2. First bug: generator sign convention in the SSH profile

Summed generator active power:

- exported source: `+4374.4 MW`
- re-imported network: `-4376.3 MW`

Every synchronous machine came back consuming instead of generating. The exporter wrote
`RotatingMachine.p`/`RotatingMachine.q` with pandapower's generation-positive sign, but the
CIM SSH profile uses the load sign convention for `RotatingMachine` — injection is
negative. The official ENTSO-E SmallGrid SSH confirms this (all generating machines carry
`RotatingMachine.p` values such as `-450`, `-85`).

The defect was invisible to the old smoke roundtrip because external grids take a
different path: the exporter already negated `ExternalNetworkInjection.p/q`, and the smoke
network's only source was an ext-grid.

### Fix

`src/powermodelconverter/exporters/cgmes.py`: negate `RotatingMachine.p`/`RotatingMachine.q`
in the SSH source rendering.

### Result

The re-imported network converges, but the roundtrip residuals were still above the
balanced tier: `slack_delta_mva = 6.22e-2`, `max_voltage_delta_pu = 2.24e-3`.

## 3. Second bug: transformer magnetizing branch dropped in the EQ profile

A name-keyed Ybus comparison between source and re-import localised the remaining error to
a single branch (`relative Frobenius norm 2.1e-5`, all of it on one transformer). Column
diff on that transformer:

- `i0_percent`: source `8.9`, re-import `0.0`

The exporter wrote `PowerTransformerEnd.g = 0` and `PowerTransformerEnd.b = 0`
unconditionally, discarding the magnetizing branch (`i0_percent`, `pfe_kw`). On this
SmallGrid unit (50 MVA, 132/33 kV) that silently removed ~4.45 MVA of magnetizing demand.
Because pandapower's T-model is converted to an equivalent pi for the Ybus, the missing
shunt perturbs the off-diagonal terms too — which is why the residual showed up as a
voltage error two buses wide rather than a pure local reactive deficit.

### Fix

`src/powermodelconverter/exporters/cgmes.py`: emit the magnetizing admittance on the HV
transformer end, inverting the cim2pp reconstruction formulas:

- `g_hv = pfe_kw / 1000 / vn_hv_kv^2`
- `|y_hv| = i0_percent / 100 * sn_mva / vn_hv_kv^2`, `b_hv = -sqrt(y^2 - g^2)`
  (negative: inductive, matching the official ENTSO-E sign convention)

cim2pp recovers `pfe_kw = g * ratedU^2 * 1000` and
`i0_percent = |y| * ratedU^2 * 100 / ratedS` per end, so placing the full branch on the HV
end roundtrips exactly.

## 4. Result on the full SmallGrid BaseCase

Roundtrip with shunts and wards frozen at solved-state PQ (the CGMES exporter does not yet
carry those equipment classes; the freeze keeps the operating point an exact fixed point):

- `slack_delta_mva = 2.89e-11`
- `max_voltage_delta_pu = 4.76e-14`
- compared buses: `118`

## Conclusion

UC3 required two real exporter fixes, both invisible to the previous smoke-level check:

1. CIM load-sign convention for synchronous machine injections in SSH
2. preservation of the transformer magnetizing branch in EQ

Both defects were caught by the validation layer the moment the roundtrip was upgraded
from a supported-subset demo to the full conformity-package topology — which is the
paper's core claim about automated, validated conversion.
