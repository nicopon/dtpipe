#!/bin/bash
set -e

# validate_fault_rendering.sh
# One fault is reported once, and a stack trace is a DEBUG=1 concern.
#
# The top-level handler in LinearPipelineService decides how a fault is presented:
# the full causal chain for everyone, the stack behind DEBUG=1. Two upstream sites
# used to defeat that decision -- Serilog's unconditional {Exception} and a second
# Observer.LogError of an exception that is rethrown anyway. A broken pipe therefore
# printed the same fact three times, thirteen stack frames included.
#
# A broken pipe is not an exotic case: it is the NORMAL failure shape of a split
# pipeline, read by the team that owns the fragment rather than by whoever launched
# the run. That is why it is the case this script drives.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
ARTIFACTS_DIR="$SCRIPT_DIR/artifacts/fault_rendering"
mkdir -p "$ARTIFACTS_DIR"

DTPIPE="$PROJECT_ROOT/dist/release/dtpipe"
export DTPIPE_NO_TUI=1

GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

pass() { echo -e "  ${GREEN}OK: $1${NC}"; }
fail() { echo -e "  ${RED}FAIL: $1${NC}"; exit 1; }

echo "========================================"
echo "    DtPipe Fault Rendering Validation"
echo "========================================"

if [ ! -f "$DTPIPE" ]; then
    echo "Building release..."
    "$PROJECT_ROOT/build.sh" > /dev/null
fi

A="$ARTIFACTS_DIR"
cleanup() { rm -f "$A"/fifo "$A"/*.log; }
trap cleanup EXIT

# Break the pipe for real: a reader that takes a couple of KB and leaves, so the
# writer hits EPIPE partway through a throttled run rather than at its last flush.
run_broken_pipe() {  # run_broken_pipe <logfile>
    rm -f "$A/fifo"; mkfifo "$A/fifo"
    { dd of=/dev/null bs=1 count=2000 2>/dev/null; } < "$A/fifo" &
    sleep 0.3
    set +e
    "$DTPIPE" -i generate: -r 400000 --throttle 40000 -o arrow:"$A/fifo" >"$1" 2>&1
    local rc=$?
    set -e
    wait 2>/dev/null || true
    return $rc
}

echo ""
echo "--- Case 1: ordinary verbosity ---"
run_broken_pipe "$A/plain.log" && fail "broken pipe exited 0"

frames=$(grep -c '^ *at ' "$A/plain.log" || true)
chain=$(grep -c 'Broken pipe' "$A/plain.log" || true)

[ "$frames" -eq 0 ] || fail "stack trace at ordinary verbosity ($frames frames) — {Exception} is attached again"
pass "no stack frames without DEBUG=1"

[ "$chain" -eq 1 ] || fail "the fault is reported $chain times, expected once"
pass "the fault is reported exactly once"

grep -q -- '-> IOException: Broken pipe' "$A/plain.log" \
    || fail "the causal chain is gone — the flattener's rendering must survive"
pass "the causal chain names the cause and the source"

echo ""
echo "--- Case 2: DEBUG=1 ---"
DEBUG=1 run_broken_pipe "$A/debug.log" && fail "broken pipe exited 0 under DEBUG=1"

dbg_frames=$(grep -c '^ *at ' "$A/debug.log" || true)
[ "$dbg_frames" -gt 0 ] || fail "DEBUG=1 produced no stack trace — the diagnostic is lost, not moved"
pass "DEBUG=1 still yields a stack trace ($dbg_frames frames)"

echo ""
echo -e "${GREEN}Fault rendering validation passed.${NC}"
