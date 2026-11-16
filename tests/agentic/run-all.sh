#!/usr/bin/env bash
set -uo pipefail

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Read model arguments and gate flag.
GATE=false
GATE_ARGS=()
MODELS=()
for arg in "$@"; do
    case "$arg" in
         --gate)
            GATE=true
            GATE_ARGS+=(--gate)
             ;;
         --max-mcp-errors=*)
            GATE_ARGS+=(--max-mcp-errors="${arg#--max-mcp-errors=}")
             ;;
         *)
            MODELS+=("$arg")
             ;;
    esac
done
if [ ${#MODELS[@]} -eq 0 ]; then
    MODELS=("${OLLAMA_MODEL:-gemma4:12b-mlx}")
fi

FAILED=false

for MODEL in "${MODELS[@]}"; do
    echo "=================================================="
    echo "Starting Agentic E2E Test Suite"
    echo "Model: $MODEL"
    echo "=================================================="
    echo

     "$SCRIPT_DIR/test-mission-aggregation.sh" "$MODEL" || FAILED=true
     "$SCRIPT_DIR/test-mission-anonymisation.sh" "$MODEL" || FAILED=true
     "$SCRIPT_DIR/test-mission-join.sh" "$MODEL" || FAILED=true
     "$SCRIPT_DIR/test-mission-yaml.sh" "$MODEL" || FAILED=true
     "$SCRIPT_DIR/test-mission-mcp-enrichment.sh" "$MODEL" || FAILED=true

    echo "🔌 Unloading model: $MODEL from Ollama memory..."
    curl -s -X POST http://localhost:11434/api/generate -d "{\"model\": \"$MODEL\", \"keep_alive\": 0}" > /dev/null

    echo "💤 Sleeping 5 seconds..."
    sleep 5
    echo
done

# Print final comparative report
"$SCRIPT_DIR/print-benchmark.sh"

# (F7) When gated, run the fail-closed analyzer over the produced traces.
if [ "$GATE" = "true" ]; then
    echo
    "$SCRIPT_DIR/analyze-traces.sh" "${GATE_ARGS[@]}" || FAILED=true
fi

if [ "$FAILED" = "true" ]; then
    echo "❌ SOME AGENTIC E2E MISSIONS FAILED!"
    exit 1
else
    echo "🎉 ALL AGENTIC E2E MISSIONS SUCCEEDED!"
fi
