% Solve a MATPOWER case natively and print its operating point as JSON.
%
% Usage:  octave-cli scripts/matpower_reference.m <case.m>
%
% This is the source-side oracle for MATPOWER routes. pmc reads and writes the
% .m format through `matpowercaseframes`, which parses but does not solve; the
% reference operating point therefore has to come from MATPOWER itself, run here
% on Octave, exactly as PowerModels.jl and PMD.jl are run for their formats.
%
% Emits one JSON object on the last stdout line:
%   {"case": ..., "success": 0|1, "baseMVA": ..., "slack_p_mw": ..., "slack_q_mvar": ...,
%    "bus": [{"bus_i":..,"vm_pu":..,"va_deg":..}, ...]}

args = argv();
if numel(args) < 1
  error('usage: matpower_reference.m <case.m>');
end
casefile = args{1};

addpath(genpath(getenv('MATPOWER_HOME')));

[pathstr, name, ~] = fileparts(casefile);
addpath(pathstr);

mpopt = mpoption('verbose', 0, 'out.all', 0);
mpc = loadcase(name);
r = runpf(mpc, mpopt);

% Generation at the reference buses. Deliberately NOT net of co-located load:
% pandapower reports the external grid's supply, and a load at the same bus is a
% separate element, so netting here would compare two different quantities. In
% pglib_opf_case5_pjm the reference bus also carries a 400 MW load, which is
% exactly the discrepancy this avoids.
ref = find(r.bus(:, 2) == 3);
slack_p = 0; slack_q = 0;
for k = 1:numel(ref)
  b = r.bus(ref(k), 1);
  gi = find(r.gen(:, 1) == b & r.gen(:, 8) > 0);
  slack_p = slack_p + sum(r.gen(gi, 2));
  slack_q = slack_q + sum(r.gen(gi, 3));
end

parts = {};
for i = 1:rows(r.bus)
  parts{end+1} = sprintf('{"bus_i":%d,"vm_pu":%.12g,"va_deg":%.12g}', ...
                         r.bus(i, 1), r.bus(i, 8), r.bus(i, 9));
end

printf('{"case":"%s","success":%d,"baseMVA":%.12g,"slack_p_mw":%.12g,"slack_q_mvar":%.12g,"bus":[%s]}\n', ...
       name, r.success, r.baseMVA, slack_p, slack_q, strjoin(parts, ','));
