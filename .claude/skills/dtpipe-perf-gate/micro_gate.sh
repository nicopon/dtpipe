#!/usr/bin/env bash
# Runs tests/scripts/micro_perf_gate.sh with BenchmarkDotNet's output sent to a log; stdout carries
# only the gate's own verdict (fingerprint check, comparison table, PASS/FAIL/REFUSED).
# All arguments pass through unchanged.
set -uo pipefail

ROOT="$(git -C "$(dirname "$0")" rev-parse --show-toplevel)"
cd "$ROOT" || exit 1
LOG_DIR="tests/scripts/artifacts"
mkdir -p "$LOG_DIR"
LOG="$LOG_DIR/micro_perf_gate.log"

tests/scripts/micro_perf_gate.sh "$@" > "$LOG" 2>&1
status=$?

strip() { perl -pe 's/\e\[[0-9;]*[A-Za-z]//g'; }
sed -n '/Host:/p;/Filter:/p' "$LOG" | strip
# The gate's own output begins at the first of these markers, after BenchmarkDotNet has finished.
start="$(strip < "$LOG" | grep -nE '^(Machine fingerprint differs|Benchmark +(baseline|Mean)|Baseline written|No baseline at|Benchmark run failed|No BenchmarkDotNet JSON)' | head -1 | cut -d: -f1)"
if [ -n "$start" ]; then
    strip < "$LOG" | tail -n +"$start"
else
    strip < "$LOG" | tail -n 40
fi
echo "Exit: $status (0 pass, 1 regression, 2 refused foreign host, 3 error — full log: $LOG)"
exit $status
