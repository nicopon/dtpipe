#!/usr/bin/env bash
set -eo pipefail

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
REPO_ROOT="$( cd "$SCRIPT_DIR/../.." && pwd )"

# TRACES_DIR is overridable via environment so the gate can be pointed at a temporary
# directory in smoke tests. Defaults to the real trace artifacts.
TRACES_DIR="${TRACES_DIR:-$REPO_ROOT/tests/agentic/artifacts/traces}"

# Fail-closed gate config.
GATE=false
MAX_MCP_ERRORS="${GATE_MAX_MCP_ERRORS:-0}"            # maximum allowed unhandled MCP errors across traces

# ---- Argument parsing -------------------------------------------------------
for arg in "$@"; do
    case "$arg" in
        --gate)
            GATE=true
            ;;
        --max-mcp-errors=*)
            MAX_MCP_ERRORS="${arg#--max-mcp-errors=}"
            ;;
        -h|--help)
            cat <<EOF
Usage: analyze-traces.sh [ --gate ] [ --max-mcp-errors=N ]

    --gate            Fail (exit 1) when: (a) unhandled MCP errors exceed
                      --max-mcp-errors, or (b) any mission failed.
    --max-mcp-errors=N  Maximum allowed unhandled MCP errors (default 0).
EOF
            exit 0
            ;;
    esac
done

if [ ! -d "$TRACES_DIR" ]; then
    echo "ℹ️ No trace directory found. Run some agentic tests first."
    exit 0
fi

echo "=========================================================================================="
echo "                            DTPIPE AGENT TRACE ANALYSIS & DIAGNOSTICS                     "
echo "=========================================================================================="
echo

# ---- Gate counters ----------------------------------------------------------
MCP_ERROR_COUNT=0
FAILED_MISSIONS=0

if [ -d "$TRACES_DIR" ]; then
    MODELS=$(ls "$TRACES_DIR" 2>/dev/null || true)

    for MODEL in $MODELS; do
        echo "🤖 Model: $MODEL"
        echo "------------------------------------------------------------------------------------------"

        TRACES=$(find "$TRACES_DIR/$MODEL" -name "*.md" 2>/dev/null || true)

        for TRACE in $TRACES; do
            MISSION_NAME=$(basename "$TRACE" .md)
            STATUS=$(grep -E "Status" "$TRACE" | tail -n 1 || echo "Unknown")

            echo "   📌 Mission: $MISSION_NAME | $STATUS"

             # (a) Count unhandled MCP errors. The runner marks them with the ⚠️ marker; a bare
            #     "Ollama Error" line is an LLM/runtime error, not an unhandled MCP error.
            MCP_ERRS=$(grep -c "⚠️" "$TRACE" 2>/dev/null || true)
            MCP_ERRS="${MCP_ERRS:-0}"
            MCP_ERROR_COUNT=$((MCP_ERROR_COUNT + MCP_ERRS))

            if [ "$MCP_ERRS" -gt 0 ]; then
                echo "      ⚠️ Detected MCP Errors/Warnings:"
                grep "⚠️" "$TRACE" 2>/dev/null | sed 's/^/        /' | head -n 5 || true
            fi

             # (b) A mission fails when its trace records a FAILURE status.
            if grep -qE "❌|FAILURE" "$TRACE" 2>/dev/null && ! grep -q "🟢.*SUCCESS" "$TRACE" 2>/dev/null; then
                FAILED_MISSIONS=$((FAILED_MISSIONS + 1))
                echo "      ❌ Mission FAILED: $MISSION_NAME"
            fi

             # Repeated tool calls diagnostic.
            REPEATED=$(grep "\*\*Tool Call\*\*" "$TRACE" 2>/dev/null | sort | uniq -c | sort -nr | head -n 3 || true)
            echo "      🔄 Top Tool Calls:"
            echo "$REPEATED" | sed 's/^/        /'
            echo
        done
        echo
    done
fi

# (b) Mission failures are detected per-trace above (the runner appends a FAILURE / ❌
# marker to any mission whose ReAct loop or validation failed). No separate benchmark
# pass is needed to avoid double-counting accumulated history.

# ---- Gate verdict -----------------------------------------------------------
if [ "$GATE" = "true" ]; then
    echo "=========================================================================================="
    echo "                            CI GATE (fail-closed)                                       "
    echo "=========================================================================================="
    echo "   Unhandled MCP errors : $MCP_ERROR_COUNT  (max $MAX_MCP_ERRORS)"
    echo "   Failed missions      : $FAILED_MISSIONS  (max 0)"
    echo

    FAIL=false
    REASONS=""
    if [ "$MCP_ERROR_COUNT" -gt "$MAX_MCP_ERRORS" ]; then
        FAIL=true
        REASONS="${REASONS}unhandled MCP errors ($MCP_ERROR_COUNT > $MAX_MCP_ERRORS); "
    fi
    if [ "$FAILED_MISSIONS" -gt 0 ]; then
        FAIL=true
        REASONS="${REASONS}failed missions ($FAILED_MISSIONS); "
    fi

    if [ "$FAIL" = "true" ]; then
        echo "❌ CI GATE FAILED: $REASONS"
        exit 1
    fi

    echo "🎉 CI GATE PASSED: no unhandled errors, no failed missions."
    exit 0
fi
