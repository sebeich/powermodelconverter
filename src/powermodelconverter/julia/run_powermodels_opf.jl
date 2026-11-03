# Solve an optimisation problem on a converted model in PowerModels.jl.
#
#   run_powermodels_opf.jl <powermodels_json> <dc_opf|ac_opf> \
#       [--vbounds <vmin> <vmax>] [--loss-slack-capacity <mw>]
#
# The PyPSA-Eur workflow uses this twice on the same island, for two different questions:
#
#   dc_opf  reconstructs the linear problem PyPSA-Eur itself solved, so the
#           objective can be compared against PyPSA's optimum directly. The
#           injected power-flow slacks must already be stripped from the model,
#           or the LP dispatches an unbounded zero-cost unit and answers a
#           cheaper question than the one that was asked.
#
#   ac_opf  is a genuinely new result rather than a check: the source tool never
#           posed it. Bus voltage bounds are optional arguments because a PyPSA
#           network carries none, so any limit here is a choice and belongs with
#           the caller, not buried in this script.
#
# Emits one JSON object on stdout.

using JSON3
using Ipopt
using PowerModels

const SOLVED = ("LOCALLY_SOLVED", "SOLVED", "ALMOST_LOCALLY_SOLVED", "ALMOST_SOLVED")

function main()
    if length(ARGS) < 2
        error("Usage: run_powermodels_opf.jl <powermodels_json> <dc_opf|ac_opf> " *
              "[--vbounds <vmin> <vmax>] [--loss-slack-capacity <mw>]")
    end
    case_path, problem = ARGS[1], ARGS[2]

    # The loss slack is named by the caller rather than guessed at from the data:
    # a capacity threshold picked here would misfire on any model that legitimately
    # contains a very large machine.
    vbounds = nothing
    loss_slack_capacity = nothing
    argi = 3
    while argi <= length(ARGS)
        if ARGS[argi] == "--vbounds"
            vbounds = [parse(Float64, ARGS[argi + 1]), parse(Float64, ARGS[argi + 2])]
            argi += 3
        elseif ARGS[argi] == "--loss-slack-capacity"
            loss_slack_capacity = parse(Float64, ARGS[argi + 1])
            argi += 2
        else
            error("Unknown argument '$(ARGS[argi])'")
        end
    end

    data = PowerModels.parse_file(case_path)
    base_mva = Float64(get(data, "baseMVA", 1.0))
    if vbounds !== nothing
        for (_, bus) in data["bus"]
            bus["vmin"] = vbounds[1]
            bus["vmax"] = vbounds[2]
        end
    end

    optimizer = optimizer_with_attributes(
        # 3000 is not enough for a national-scale AC OPF: the 7,824-bus island
        # needs ~3900 s of interior-point work and stops at Ipopt's acceptable
        # tolerance, so a lower cap reports ITERATION_LIMIT on a problem that is
        # merely large rather than infeasible.
        Ipopt.Optimizer, "print_level" => 0, "sb" => "yes", "max_iter" => 10000)
    result = if problem == "dc_opf"
        PowerModels.solve_dc_opf(data, optimizer)
    elseif problem == "ac_opf"
        # Rectangular, not polar. On the 3,281-bus transmission island the polar
        # formulation reaches the iteration limit at every voltage band worth
        # asking for, and no barrier strategy, warm start, bound handling or
        # tolerance rescues it; the rectangular one solves the same model at
        # +/-10 % in 2,575 s. Polar puts a trigonometric term in every power
        # balance constraint, which conditions badly across a wide voltage
        # spread, while rectangular keeps them quadratic.
        PowerModels.solve_opf(data, PowerModels.ACRPowerModel, optimizer)
    else
        error("Unknown problem '$(problem)': expected dc_opf or ac_opf")
    end

    status = string(get(result, "termination_status", ""))
    payload = Dict{String, Any}(
        "problem" => problem,
        "termination_status" => status,
        "solved" => status in SOLVED,
        "solve_time_s" => Float64(get(result, "solve_time", NaN)),
        "objective" => Float64(get(result, "objective", NaN)),
        "load_mw" => sum(load["pd"] for (_, load) in data["load"]; init = 0.0) * base_mva,
    )
    if vbounds !== nothing
        payload["voltage_bounds_pu"] = vbounds
    end

    solution = get(result, "solution", Dict())
    if haskey(solution, "gen") && !isempty(solution["gen"])
        total_pg = sum(gen["pg"] for (_, gen) in solution["gen"]; init = 0.0) * base_mva
        payload["total_pg_mw"] = total_pg
        payload["losses_mw"] = total_pg - payload["load_mw"]
        # Report the loss slack separately: an AC OPF that leans on it is not
        # reproducing the source dispatch, and that has to be visible.
        if loss_slack_capacity !== nothing
            for (gen_id, gen) in data["gen"]
                if isapprox(get(gen, "pmax", 0.0) * base_mva, loss_slack_capacity; rtol = 1e-6)
                    payload["loss_slack_mw"] = Float64(solution["gen"][gen_id]["pg"]) * base_mva
                    break
                end
            end
        end
    end
    if haskey(solution, "bus") && !isempty(solution["bus"])
        # The rectangular formulation reports vr/vi rather than a magnitude.
        vms = [haskey(bus, "vm") ? Float64(bus["vm"]) : hypot(Float64(bus["vr"]), Float64(bus["vi"]))
               for (_, bus) in solution["bus"] if haskey(bus, "vm") || haskey(bus, "vr")]
        if !isempty(vms)
            payload["vm_min_pu"] = minimum(vms)
            payload["vm_max_pu"] = maximum(vms)
            if vbounds !== nothing
                payload["buses_at_vmin"] = count(v -> v <= vbounds[1] + 1e-6, vms)
                payload["buses_at_vmax"] = count(v -> v >= vbounds[2] - 1e-6, vms)
            end
        end
    end
    print(JSON3.write(payload))
end

main()
