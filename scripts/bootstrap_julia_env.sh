#!/usr/bin/env bash
set -euo pipefail

JULIA_BIN="${JULIA_BIN:-$(command -v julia || true)}"
JULIA_DEPOT_PATH="${JULIA_DEPOT_PATH:-$(pwd)/.julia_depot}"
BALANCED_PROJECT="$(pwd)/src/powermodelconverter/julia"
UNBALANCED_PROJECT="$(pwd)/src/powermodelconverter/julia_pmd"
POWERSYSTEMS_PROJECT="$(pwd)/src/powermodelconverter/julia_psi"

if [[ -z "$JULIA_BIN" ]]; then
  JULIA_BIN="/opt/julia/bin/julia"
fi

export JULIA_DEPOT_PATH

bootstrap_project() {
  local project_path="$1"
  JULIA_PROJECT="$project_path" "$JULIA_BIN" -e '
    using Pkg
    if isempty(Pkg.Registry.reachable_registries())
      Pkg.Registry.add(Pkg.RegistrySpec(name="General"))
    end
    Pkg.resolve()
    Pkg.instantiate()
    Pkg.precompile()
  '
}

bootstrap_project "$BALANCED_PROJECT"
bootstrap_project "$POWERSYSTEMS_PROJECT"
bootstrap_project "$UNBALANCED_PROJECT"
