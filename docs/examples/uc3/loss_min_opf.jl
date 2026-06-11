using Pkg
Pkg.resolve()
Pkg.instantiate()

using JSON3
using Ipopt
using PowerModels

function main()
    if length(ARGS) != 1
        error("Usage: loss_min_opf.jl <powermodels_json_path>")
    end

    data = PowerModels.parse_file(ARGS[1])
    total_load = sum(load["pd"] for (_, load) in get(data, "load", Dict()); init = 0.0)

    started = time()
    result = PowerModels.solve_opf(data, ACPPowerModel, Ipopt.Optimizer)
    elapsed = time() - started

    solution_gens = get(get(result, "solution", Dict()), "gen", Dict())
    total_pg = isempty(solution_gens) ? nothing : sum(gen["pg"] for (_, gen) in solution_gens)

    print(JSON3.write(Dict(
        "termination_status" => string(get(result, "termination_status", "")),
        "objective" => get(result, "objective", nothing),
        "total_pg_pu" => total_pg,
        "total_load_pu" => total_load,
        "base_mva" => get(data, "baseMVA", 1.0),
        "solve_time_seconds" => elapsed,
    )))
end

main()
