#!/usr/bin/env bash
set -euo pipefail

export HOME="${HOME:-/tmp/powermodelconverter-home}"
export XDG_CACHE_HOME="${XDG_CACHE_HOME:-$HOME/.cache}"
export MPLCONFIGDIR="${MPLCONFIGDIR:-$HOME/.config/matplotlib}"
mkdir -p "$MPLCONFIGDIR" "$XDG_CACHE_HOME/fontconfig"

if [[ -d "/workspace/src" ]]; then
  export PYTHONPATH="/workspace/src:${PYTHONPATH:-}"
else
  export PYTHONPATH="/opt/powermodelconverter/src:${PYTHONPATH:-}"
fi

exec "$@"
