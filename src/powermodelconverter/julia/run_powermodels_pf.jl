using JSON3
using Ipopt
using PowerModels

function main()
    if length(ARGS) != 1
        error("Usage: run_powermodels_pf.jl <powermodels_json_path>")
    end

    case_path = ARGS[1]
    data = PowerModels.parse_file(case_path)
    result = PowerModels.solve_ac_pf(data, Ipopt.Optimizer)
    status = string(get(result, "termination_status", ""))

    if status ∉ ("LOCALLY_SOLVED", "SOLVED", "ALMOST_LOCALLY_SOLVED", "ALMOST_SOLVED")
        error("PowerModels AC power flow did not converge: $(status)")
    end

    solution = result["solution"]
    bus_results = Dict{String, Any}()
    for (bus_id, bus_data) in solution["bus"]
        vm = Float64(bus_data["vm"])
        va = Float64(bus_data["va"])
        bus_results[string(bus_id)] = Dict(
            "vm_pu_real" => vm * cos(va),
            "vm_pu_imag" => vm * sin(va),
        )
    end

    ref_buses = Set(
        bus_id for (bus_id, bus_data) in data["bus"] if Int(bus_data["bus_type"]) == 3
    )
    slack_p = 0.0
    slack_q = 0.0
    for (gen_id, gen_solution) in solution["gen"]
        gen_bus = string(data["gen"][gen_id]["gen_bus"])
        if gen_bus in ref_buses
            slack_p += Float64(get(gen_solution, "pg", 0.0))
            slack_q += Float64(get(gen_solution, "qg", 0.0))
        end
    end

    print(JSON3.write(Dict(
        "slack_p_mw" => slack_p,
        "slack_q_mvar" => slack_q,
        "bus_results" => bus_results,
    )))
end

main()
