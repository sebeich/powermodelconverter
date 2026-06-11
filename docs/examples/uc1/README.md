# UC1 — PGLib Convex OPF Relaxation Gaps

This reproducer uses three pinned PGLib-OPF MATPOWER cases: `pglib_opf_case5_pjm`, `pglib_opf_case14_ieee`, and `pglib_opf_case30_ieee__api`.
The workflow imports each case through pmc, validates MATPOWER → pandapower and pandapower → PowerModels legs, then solves AC, SOC, QC, and SDP OPF formulations in PowerModels.jl.
The scientific output is the relaxation gap `(AC_obj - relax_obj) / AC_obj`, compared against the upstream PGLib baseline table for QC and SOC.
Expected conversion residuals stay inside the balanced `10^-3` envelope; any larger discrepancy blocks the use case.
Citations: Babaeinejadsarookolaee et al. 2019 (PGLib-OPF), Coffrin-Hijazi-Van Hentenryck 2016 (QC relaxation), Low 2014 Parts I/II, Molzahn-Hiskens 2019.
