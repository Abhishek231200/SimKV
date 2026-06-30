#!/usr/bin/env bash
# Run the Porcupine linearizability checker against a history JSON file emitted
# by simkv. Requires Go 1.18+ and the porcupine_glue binary to be built.
#
# Usage: ./scripts/run_porcupine.sh <history.json>

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
GLUE_DIR="$PROJECT_ROOT/src/checker/porcupine_glue"

if [[ $# -ne 1 ]]; then
    echo "Usage: $0 <history.json>" >&2
    exit 1
fi

HISTORY_FILE="$1"
if [[ ! -f "$HISTORY_FILE" ]]; then
    echo "Error: history file not found: $HISTORY_FILE" >&2
    exit 1
fi

# Build the Go checker if not already built.
CHECKER_BIN="$GLUE_DIR/simkv_porcupine"
if [[ ! -f "$CHECKER_BIN" ]]; then
    echo "Building Porcupine glue..."
    (cd "$GLUE_DIR" && go build -o simkv_porcupine .)
fi

"$CHECKER_BIN" "$HISTORY_FILE"
