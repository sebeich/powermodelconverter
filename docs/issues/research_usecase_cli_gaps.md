# Research Use-Case CLI Gaps

This repository currently lacks two first-class CLI surfaces that the IEEE Access use cases would benefit from:

1. `pmc merge`
   Needed by the TSO-DSO boundary coordination workflow (`UC4`) so the boundary object and linked canonical cases can be expressed as a supported end-user command rather than ad hoc Python.

2. `pmc validate --cross-tool`
   Needed by the PyPSA-Eur AC-feasibility cross-check workflow (`UC5`) so three-way artifact comparisons can be reproduced through the same CLI contract as the rest of the repo.

3. PowerModels export metadata leak (`user_defined_params` / `gen_and_controllable_sgen`)
   Observed during `UC1`: pandapower-internal metadata currently reaches the canonical-to-PowerModels export artifact and must be stripped before Julia-side parsing. This is not the UC1 root-cause bug, but it is an architectural follow-up because backend-specific bookkeeping should not leak into the reproducible exchange format.

4. OpenDSS wrapper generation should avoid compact delta-transformer declarations
   Observed during `UC2`: in the current compose runtime (`OpenDSSDirect.py 0.9.4` on
   `DSS C-API 0.14.5`), a generated DSS wrapper that uses vector-form transformer syntax
   with a delta-connected winding and implicit bus nodes can compile and report
   convergence while returning zero bus voltages through the standard runtime accessors.
   Safe forms are:
   - explicit delta bus nodes, e.g. `SourceBus.1.2.3`
   - per-winding multiline transformer declarations

   This is not a blocker for importing existing DSS cases, but it matters for any
   generated OpenDSS benchmark wrappers in `UC2`/`UC3` and should be encoded as a shared
   helper or exporter convention instead of rediscovered case by case.

5. pandapower `convert_pp_to_pm` assumes contiguous `ext_grid` indices
   Observed during `UC4`: the upstream converter indexes `net.ext_grid["vm_pu"].values`
   positionally with the pandas index (`build_gen.py:_build_pp_ext_grid`), so any network
   whose element tables keep non-contiguous indices (e.g. islands carved out of a larger
   net) crashes with an `IndexError`. Worked around in `docs/examples/uc4/run.py` via
   `pandapower.toolbox.create_continuous_elements_index`; worth reporting upstream.

6. cim2pp does not map CGMES `CurrentLimit` values into pandapower thermal ratings
   Observed during `UC3`: imported lines fall back to a `max_i_ka = 1.0` placeholder, so
   loading percentages on CGMES-sourced models are relative indicators only. Either map
   the EQ `CurrentLimit`/`OperationalLimitSet` chain into `max_i_ka` during import or
   surface the limitation in the validation report.

7. Pre-existing test failure on this branch:
   `tests/test_importers.py::test_opendss_regulator_bank_specs_are_aggregated_for_pandapower`
   passes a `pfe_kw` kwarg that `_TransformerSpec` does not accept — the test and the
   importer dataclass are out of sync (unrelated to the UC3 CGMES exporter fixes; it fails
   identically on the unmodified tree).

Context:

- branch: `refactor/hub-and-spoke`
- current limitation: no `gh` client is installed in this environment, so these are tracked here in-repo for now instead of being filed remotely
- target follow-up: convert these notes into GitHub issues when repository issue tooling is available
