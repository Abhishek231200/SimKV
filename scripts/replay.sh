#!/usr/bin/env bash
# Replay a simulation run from a seed and dump the event trace.
# Usage: ./scripts/replay.sh <seed> [--nodes N] [--ops N] [--inject-commit-bug]
#
# Example: ./scripts/replay.sh 12345 --nodes 3 --ops 200

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD_DIR="$(dirname "$SCRIPT_DIR")/build"

SIMKV="$BUILD_DIR/src/cli/simkv"
if [[ ! -f "$SIMKV" ]]; then
    echo "Error: simkv binary not found at $SIMKV" >&2
    echo "       Run: cmake -B build && cmake --build build" >&2
    exit 1
fi

if [[ $# -eq 0 ]]; then
    echo "Usage: $0 <seed> [options]" >&2
    exit 1
fi

SEED="$1"
shift

"$SIMKV" run --seed "$SEED" --dump-trace "$@"
