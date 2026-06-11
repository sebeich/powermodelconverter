using Pkg
Pkg.resolve()
Pkg.instantiate()

using JSON3
using Ipopt
using PowerModels
using SCS

const FORMULATIONS = [
    ("ac", ACPPowerModel, Ipopt.Optimizer),
    ("soc", SOCWRPowerModel, Ipopt.Optimizer),
    ("qc", QCRMPowerModel, Ipopt.Optimizer),
    ("sdp", SDPWRMPowerModel, SCS.Optimizer),
]

function solve_formulation(data, formulation_name, model_type, optimizer)
    started = time()
    result = PowerModels.solve_opf(data, model_type, optimizer)
    elapsed = time() - started
    status = string(get(result, "termination_status", ""))
    objective = get(result, "objective", nothing)
    return Dict(
        "formulation" => formulation_name,
        "termination_status" => status,
        "objective" => objective,
        "solve_time_seconds" => elapsed,
    )
end

function main()
    if length(ARGS) != 1
        error("Usage: opf_relaxations.jl <powermodels_json_path>")
    end

    case_path = ARGS[1]
    data = PowerModels.parse_file(case_path)
    results = Dict{String, Any}()
    for (name, model_type, optimizer) in FORMULATIONS
        results[name] = solve_formulation(data, name, model_type, optimizer)
    end
    print(JSON3.write(results))
end

main()
