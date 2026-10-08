#!/usr/bin/env bash
set -e

# Resolve directory of this script safely
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
cd "$SCRIPT_DIR"

export JAVA_HOME="$SCRIPT_DIR/.java_local/current"
export PATH="$JAVA_HOME/bin:$PATH"
export KAGGLEHUB_CACHE="$SCRIPT_DIR/.cache/kagglehub"

source "$SCRIPT_DIR/.venv/bin/activate"

exec python3 "$@"
