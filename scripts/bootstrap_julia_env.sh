#!/usr/bin/env bash
set -euo pipefail

JULIA_BIN="${JULIA_BIN:-$HOME/.julia/juliaup/julia-1.12.3+0.x64.linux.gnu/bin/julia}"
JULIA_DEPOT_PATH="${JULIA_DEPOT_PATH:-$(pwd)/.julia_depot}"
BALANCED_PROJECT="$(pwd)/src/powermodelconverter/julia"
UNBALANCED_PROJECT="$(pwd)/src/powermodelconverter/julia_pmd"

export JULIA_DEPOT_PATH

JULIA_PROJECT="$BALANCED_PROJECT" "$JULIA_BIN" -e 'using Pkg; Pkg.resolve(); Pkg.instantiate(); Pkg.precompile()'
JULIA_PROJECT="$UNBALANCED_PROJECT" "$JULIA_BIN" -e 'using Pkg; Pkg.resolve(); Pkg.instantiate(); Pkg.precompile()'
