using JSON3
using Ipopt
using PowerModelsDistribution

function build_node_results(solution)
    node_results = Dict{String, Any}()
    for (bus_id, bus_data) in solution["bus"]
        vm = bus_data["vm"]
        va = bus_data["va"]
        for (idx, (vm_phase, va_phase)) in enumerate(zip(vm, va))
            angle = deg2rad(Float64(va_phase))
            node_results["$(bus_id).$(idx)"] = Dict(
                "vm_pu_real" => Float64(vm_phase) * cos(angle),
                "vm_pu_imag" => Float64(vm_phase) * sin(angle),
            )
        end
    end
    return node_results
end

function main()
    if length(ARGS) != 1
        error("Usage: run_powermodels_distribution_pf.jl <distribution_input_path>")
    end

    case_path = ARGS[1]
    result_si = PowerModelsDistribution.solve_mc_pf(
        case_path,
        PowerModelsDistribution.ACPUPowerModel,
        Ipopt.Optimizer,
    )
    status = string(get(result_si, "termination_status", ""))
    if status ∉ ("LOCALLY_SOLVED", "SOLVED", "ALMOST_LOCALLY_SOLVED", "ALMOST_SOLVED")
        error("PowerModelsDistribution AC power flow did not converge: $(status)")
    end

    result_pu = PowerModelsDistribution.solve_mc_pf(
        case_path,
        PowerModelsDistribution.ACPUPowerModel,
        Ipopt.Optimizer;
        make_si=false,
    )

    source = result_si["solution"]["voltage_source"]["source"]
    slack_p_kw = sum(Float64.(source["pg"]))
    slack_q_kvar = sum(Float64.(source["qg"]))

    print(JSON3.write(Dict(
        "slack_p_mw" => slack_p_kw / 1000.0,
        "slack_q_mvar" => slack_q_kvar / 1000.0,
        "node_results" => build_node_results(result_pu["solution"]),
    )))
end

main()
