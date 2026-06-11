#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

COMPOSE=(docker compose -f "$ROOT_DIR/docker-compose.yml")
PYTHON_SERVICE="worker-python"
CONTAINER_HOME="/tmp/powermodelconverter-home"
RUN_ARGS=(
  run
  --rm
  --user "$(id -u):$(id -g)"
  -e "HOME=$CONTAINER_HOME"
  -e "XDG_CACHE_HOME=$CONTAINER_HOME/.cache"
  -e "MPLCONFIGDIR=$CONTAINER_HOME/.config/matplotlib"
)

ensure_mount_for_path() {
  local raw_path="$1"
  [[ -z "$raw_path" ]] && return 0

  local host_path
  if [[ "$raw_path" = /* ]]; then
    host_path="$raw_path"
  else
    host_path="$(realpath -m "$PWD/$raw_path")"
  fi

  local mount_path="$host_path"
  if [[ ! -d "$host_path" ]]; then
    mount_path="$(dirname "$host_path")"
  fi

  if [[ -d "$mount_path" && "$mount_path" != "$ROOT_DIR" ]]; then
    RUN_ARGS+=(-v "$mount_path:$mount_path")
  fi
}

load_env_file() {
  local env_file="$1"
  [[ -f "$env_file" ]] || return 0

  local line key value
  while IFS= read -r line || [[ -n "$line" ]]; do
    line="${line#"${line%%[![:space:]]*}"}"
    line="${line%$'\r'}"
    line="${line#export }"
    [[ -z "$line" || "$line" == \#* || "$line" != *=* ]] && continue
    key="${line%%=*}"
    key="${key%"${key##*[![:space:]]}"}"
    [[ "$key" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]] || continue
    value="${line#*=}"
    value="${value#"${value%%[![:space:]]*}"}"
    value="${value%"${value##*[![:space:]]}"}"
    if [[ "$value" == \"*\" && "$value" == *\" ]]; then
      value="${value:1:${#value}-2}"
    elif [[ "$value" == \'*\' && "$value" == *\' ]]; then
      value="${value:1:${#value}-2}"
    fi
    RUN_ARGS+=(-e "$key=$value")
  done < "$env_file"
}

load_env_file "$ROOT_DIR/.env.ding0"

prepare_run_args() {
  local args=("$@")
  local i=0
  while [[ $i -lt ${#args[@]} ]]; do
    case "${args[$i]}" in
      --source|--output)
        if [[ $((i + 1)) -lt ${#args[@]} ]]; then
          ensure_mount_for_path "${args[$((i + 1))]}"
          i=$((i + 2))
          continue
        fi
        ;;
    esac
    i=$((i + 1))
  done
}

if [[ $# -eq 0 ]]; then
  "${COMPOSE[@]}" "${RUN_ARGS[@]}" "$PYTHON_SERVICE" --help
  exit 0
fi

case "$1" in
  build)
    shift
    "${COMPOSE[@]}" build "$PYTHON_SERVICE" "$@"
    ;;
  shell)
    shift
    "${COMPOSE[@]}" "${RUN_ARGS[@]}" --entrypoint /usr/local/bin/powermodelconverter-entrypoint "$PYTHON_SERVICE" bash "$@"
    ;;
  test)
    shift
    "${COMPOSE[@]}" "${RUN_ARGS[@]}" --entrypoint /usr/local/bin/powermodelconverter-entrypoint "$PYTHON_SERVICE" pytest -q "$@"
    ;;
  report)
    shift
    "${COMPOSE[@]}" "${RUN_ARGS[@]}" --entrypoint /usr/local/bin/powermodelconverter-entrypoint "$PYTHON_SERVICE" python scripts/generate_validation_report.py "$@"
    ;;
  capabilities|precheck|translate|validate)
    subcommand="$1"
    shift
    prepare_run_args "$@"
    "${COMPOSE[@]}" "${RUN_ARGS[@]}" "$PYTHON_SERVICE" "$subcommand" "$@"
    ;;
  *)
    "${COMPOSE[@]}" "${RUN_ARGS[@]}" --entrypoint /usr/local/bin/powermodelconverter-entrypoint "$PYTHON_SERVICE" "$@"
    ;;
esac
