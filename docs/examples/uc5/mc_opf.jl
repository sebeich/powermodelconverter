using Pkg
Pkg.resolve()
Pkg.instantiate()

using JSON3
using Ipopt
using PowerModelsDistribution
const PMD = PowerModelsDistribution

const OK_STATUSES = ("LOCALLY_SOLVED", "ALMOST_LOCALLY_SOLVED", "SOLVED", "ALMOST_SOLVED")

function main()
    if length(ARGS) < 1
        error("Usage: mc_opf.jl <dss_path> [acp|ivr] [vm_ub_pu]")
    end
    dss_path = ARGS[1]
    formulation = length(ARGS) >= 2 ? lowercase(ARGS[2]) : "acp"
    vm_ub = length(ARGS) >= 3 ? parse(Float64, ARGS[3]) : 1.10

    data = PMD.parse_file(dss_path)

    # The DSS default base is 100 MVA; this feeder runs at ~100 kW. Rescaling the power
    # base (kVA units) is essential for Ipopt convergence.
    data["settings"]["sbase_default"] = 100.0

    # PV generators (from the DSS scenario file) dispatch for free; the substation source
    # carries a positive marginal cost, so the optimum absorbs all PV power that the
    # per-phase voltage cap admits — curtailment emerges per phase where the cap binds.
    for (_, gen) in get(data, "generator", Dict())
        gen["qg_lb"] = zeros(length(gen["qg"]))
        gen["qg_ub"] = zeros(length(gen["qg"]))
        gen["cost_pg_parameters"] = [0.0, 0.0, 0.0]
    end
    for (_, source) in get(data, "voltage_source", Dict())
        source["cost_pg_parameters"] = [0.0, 1.0, 0.0]
    end

    # Voltage bounds are applied on the mathematical model, where they are expressed in
    # per unit of each bus base (the engineering model expects SI volts there).
    math = PMD.transform_data_model(data)
    for (_, bus) in math["bus"]
        get(bus, "bus_type", 1) == 3 && continue
        if haskey(bus, "vmax")
            bus["vmax"] = min.(bus["vmax"], vm_ub)
        end
    end

    model = formulation == "ivr" ? PMD.IVRUPowerModel : PMD.ACPUPowerModel
    started = time()
    result = PMD.solve_mc_opf(math, model, Ipopt.Optimizer)
    elapsed = time() - started
    status = string(get(result, "termination_status", ""))

    solution = Dict{String,Any}()
    if haskey(result, "solution") && !isempty(result["solution"])
        solution = PMD.transform_solution(result["solution"], math)
    end

    pv_dispatch = Any[]
    total_capacity_kw = 0.0
    total_dispatch_kw = 0.0
    for (name, gen) in get(data, "generator", Dict())
        sol_gen = get(get(solution, "generator", Dict()), name, nothing)
        # connections may carry the neutral terminal; pg covers active phases only
        for idx in eachindex(gen["pg_ub"])
            capacity_kw = Float64(gen["pg_ub"][idx])
            capacity_kw <= 0.0 && continue
            pg_kw = sol_gen === nothing ? 0.0 : Float64(sol_gen["pg"][idx])
            total_capacity_kw += capacity_kw
            total_dispatch_kw += pg_kw
            push!(pv_dispatch, Dict(
                "generator" => name,
                "bus" => gen["bus"],
                "phase" => gen["connections"][idx],
                "pg_kw" => pg_kw,
                "capacity_kw" => capacity_kw,
                "curtailment_kw" => capacity_kw - pg_kw,
            ))
        end
    end

    # Per-unit voltage extremes over non-reference math buses (vm for ACP, vr/vi for IVR).
    vm_min = nothing
    vm_max = nothing
    if status in OK_STATUSES && haskey(result["solution"], "bus")
        magnitudes = Float64[]
        for (bus_id, bus_sol) in result["solution"]["bus"]
            vm = if haskey(bus_sol, "vm")
                Float64.(bus_sol["vm"])
            elseif haskey(bus_sol, "vr") && haskey(bus_sol, "vi")
                sqrt.(Float64.(bus_sol["vr"]) .^ 2 .+ Float64.(bus_sol["vi"]) .^ 2)
            else
                Float64[]
            end
            append!(magnitudes, vm)
        end
        nonzero = filter(v -> v > 1e-3, magnitudes)
        if !isempty(nonzero)
            vm_min = minimum(nonzero)
            vm_max = maximum(nonzero)
        end
    end

    source_pg_kw = nothing
    if haskey(solution, "voltage_source") && haskey(solution["voltage_source"], "source")
        source_pg_kw = sum(Float64.(solution["voltage_source"]["source"]["pg"]))
    end

    print(JSON3.write(Dict(
        "termination_status" => status,
        "objective" => get(result, "objective", nothing),
        "solve_time_seconds" => elapsed,
        "pv_capacity_kw" => total_capacity_kw,
        "pv_dispatch_kw" => total_dispatch_kw,
        "pv_curtailment_kw" => total_capacity_kw - total_dispatch_kw,
        "source_pg_kw" => source_pg_kw,
        "vm_extremes_pu" => Dict("min_nonzero" => vm_min, "max" => vm_max),
        "pv_dispatch" => pv_dispatch,
    )))
end

main()
