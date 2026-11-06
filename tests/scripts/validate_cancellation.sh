#!/bin/bash
set -e

# validate_cancellation.sh
# F16 — cancellation must not mask as success.
# Case 1: Ctrl-C (SIGINT) on a linear pipeline exits non-zero (POSIX 130 convention).
# Case 2: Ctrl-C on a 3-branch DAG exits non-zero.
# Case 3: Ctrl-C under a --sql branch ends the run.
# Case 4: a source failing mid-read under a --sql branch ends the run with code 1.
# Case 5: a query failing mid-stream exits 1, never 0 with part of its rows.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
ARTIFACTS_DIR="$SCRIPT_DIR/artifacts/cancellation"
mkdir -p "$ARTIFACTS_DIR"

DTPIPE="$PROJECT_ROOT/dist/release/dtpipe"
export DTPIPE_NO_TUI=1

GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

pass() { echo -e "  ${GREEN}OK: $1${NC}"; }
fail() { echo -e "  ${RED}FAIL: $1${NC}"; exit 1; }

echo "========================================"
echo "    DtPipe Cancellation Validation"
echo "========================================"

if [ ! -f "$DTPIPE" ]; then
    echo "Building release..."
    "$PROJECT_ROOT/build.sh" > /dev/null
fi

A="$ARTIFACTS_DIR"

cleanup() {
    rm -f "$A"/*.csv "$A"/*.log
    if [ -n "${PROC:-}" ]; then
        kill "$PROC" 2>/dev/null || true
    fi
}
trap cleanup EXIT

# Launch a command with its SIGINT disposition restored to SIG_DFL.
# Background jobs of a non-interactive shell inherit SIGINT=SIG_IGN, which .NET
# respects (CancelKeyPress never fires); the perl launcher resets it before exec.
# `exec` replaces the background subshell so $! is the perl→dtpipe PID itself.
launch_interruptible() {
    exec perl -e 'setpgrp(0,0); $SIG{INT}="DEFAULT"; exec(@ARGV) or die "exec failed"' "$@"
}

# The exit code of a run, 124 for one still running after 60 seconds. A run that never returns is what the
# cases below guard against, and a bare `wait` would hang this script with it.
wait_bounded() {
    local waited=0
    while kill -0 "$1" 2>/dev/null && [ "$waited" -lt 60 ]; do
        sleep 1
        waited=$((waited + 1))
    done
    if kill -0 "$1" 2>/dev/null; then
        kill -9 "$1" 2>/dev/null || true
        wait "$1" 2>/dev/null || true
        return 124
    fi
    wait "$1"
}

# Runs dtpipe with the given arguments and fails unless it exits with the code named first.
expect_exit() {
    local expected=$1 label=$2
    shift 2
    "$DTPIPE" "$@" --no-stats > "$A/run.log" 2>&1 &
    PROC=$!
    set +e
    wait_bounded "$PROC"
    local actual=$?
    set -e
    PROC=""
    if [ "$actual" -ne "$expected" ]; then
        tail -5 "$A/run.log"
        fail "$label: exit code $actual, expected $expected"
    fi
    pass "$label: exit code $actual"
}

# ----------------------------------------
# Case 1: linear pipeline interrupted by SIGINT
# ----------------------------------------
echo "--- [1] Linear pipeline Ctrl-C ---"
rm -f "$A/c.csv"
launch_interruptible "$DTPIPE" -i generate:100000000 -o "$A/c.csv" --no-stats &
PROC=$!
sleep 2
if ! ps -p $PROC > /dev/null; then
    fail "linear pipeline finished before SIGINT could be delivered"
fi
kill -INT $PROC 2>/dev/null || true
set +e
wait $PROC
EXIT=$?
set -e
PROC=""
if [ "$EXIT" -eq 0 ]; then
    fail "linear Ctrl-C returned 0 (exit code: $EXIT)"
fi
pass "linear Ctrl-C non-zero ($EXIT)"

# No partial output reported as a clean run — a partial file may exist on disk, but the
# process must not have exited successfully while leaving it behind.
if [ -s "$A/c.csv" ]; then
    echo "  WARN: partial file exists (expected for an interrupted write)"
fi

# ----------------------------------------
# Case 2: 3-branch DAG interrupted by SIGINT
# ----------------------------------------
echo "--- [2] DAG (fan-out, 3 branches) Ctrl-C ---"
rm -f "$A/d1.csv" "$A/d2.csv" "$A/d3.csv"
# --no-stats is global (GlobalOptions.NoStats): once for the whole run, not per branch.
launch_interruptible "$DTPIPE" -i generate:100000000 --alias s \
  --from s -o "$A/d1.csv" \
  --from s -o "$A/d2.csv" \
  --from s -o "$A/d3.csv" --no-stats &
PROC=$!
sleep 2
if ! ps -p $PROC > /dev/null; then
    fail "DAG finished before SIGINT could be delivered"
fi
kill -INT $PROC 2>/dev/null || true
set +e
wait $PROC
EXIT=$?
set -e
PROC=""
if [ "$EXIT" -eq 0 ]; then
    fail "DAG Ctrl-C returned 0 (exit code: $EXIT)"
fi
pass "DAG Ctrl-C non-zero ($EXIT)"

# ----------------------------------------
# Case 3: SIGINT under a --sql branch
# ----------------------------------------
echo "--- [3] --sql branch Ctrl-C ---"
launch_interruptible "$DTPIPE" -i generate:100000000000 --alias s \
  --from s --sql "SELECT * FROM s" -o null: --no-stats &
PROC=$!
sleep 2
if ! ps -p $PROC > /dev/null; then
    fail "--sql run finished before SIGINT could be delivered"
fi
kill -INT $PROC 2>/dev/null || true
set +e
wait_bounded $PROC
EXIT=$?
set -e
PROC=""
if [ "$EXIT" -ne 130 ]; then
    fail "--sql Ctrl-C: exit code $EXIT, expected 130"
fi
pass "--sql Ctrl-C: exit code 130"

# ----------------------------------------
# Case 4: a source failing mid-read under a --sql branch
# ----------------------------------------
echo "--- [4] Source failing under a --sql branch ---"
# Past the batch size, so batches flow before the fault and DuckDB is reading when it comes.
{ echo v; seq 1 300000; echo abc; } > "$A/late.csv"
expect_exit 1 "source refusing a value" -i "csv:$A/late.csv" --column-types v:decimal --alias s \
  --from s --sql "SELECT * FROM s" -o "csv:$A/sql.csv"
expect_exit 1 "the same source in a fan-out" -i "csv:$A/late.csv" --column-types v:decimal --alias s \
  --from s --sql "SELECT * FROM s" -o "csv:$A/fan1.csv" \
  --from s -o "csv:$A/fan2.csv"

# ----------------------------------------
# Case 5: a query failing mid-stream
# ----------------------------------------
echo "--- [5] Query failing mid-stream ---"
FAILING="SELECT CASE WHEN i < 200000 THEN i ELSE error('boom') END AS v FROM range(400000) t(i)"
expect_exit 1 "duck: reader" -i "duck::memory:" --query "$FAILING" -o "csv:$A/duck.csv"
expect_exit 1 "--sql branch" -i generate:10 --alias s --from s --sql "$FAILING" -o "csv:$A/mid.csv"

echo ""
echo "All cancellation checks passed."
