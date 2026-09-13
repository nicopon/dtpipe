#!/bin/bash
set -e

# validate_duck_streaming.sh
# Does a duck: read stay flat as the result grows? On the real binary.
#
# duckdb_execute_prepared_streaming + duckdb_fetch_chunk hand out one chunk at a time, and every
# stage downstream is supposed to pass it on rather than pile it up. If any of them stopped —
# the optimizer picking materialized execution, a broadcaster buffering, a writer collecting —
# the result would be held whole, and only the process's memory says so.
#
# Three claims, the same query at two volumes:
#   0. the measure can see a result held whole — established first, or nothing else counts
#   1. a duck: read of 20x the rows costs no more memory
#   2. so does a fan-out — two --sql consumers of one reader
#
# THIS IS NOT THE OWNERSHIP NET. A batch a consumer forgets to dispose is released by its
# finalizer, so a broken dispose costs a backlog, not the payload: a binary whose null writer
# drops every batch measures 108 MB here against 108 MB for the correct one, on 4 000 000 rows.
# What guards the ownership contract is CDataOwnershipTests, which counts the C Data release
# callback itself.
#
# The tolerance is derived from claim 0 on the machine that runs it, not copied from the
# reference machine: a peak that cannot move cannot judge, and a run that cannot judge says so
# and stops rather than turning an unrelated machine red.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
DTPIPE="$PROJECT_ROOT/dist/release/dtpipe"

GREEN='\033[0;32m'
YELLOW='\033[0;33m'
RED='\033[0;31m'
NC='\033[0m'

pass() { echo -e "  ${GREEN}OK: $1${NC}"; }
fail() { echo -e "  ${RED}FAIL: $1${NC}"; exit 1; }
skip() { echo -e "  ${YELLOW}SKIP: $1${NC}"; exit 0; }

SMALL=200000
BIG=4000000
# id + val + a 64-char pad: ~80 bytes a row once in Arrow, so holding every row of the big read
# costs ~300 MB. CALIBRATION_FLOOR_MB is what the measure must be able to see for that to count.
CALIBRATION_FLOOR_MB=128

echo "========================================"
echo "    DtPipe duck: streaming validation"
echo "========================================"

[ -x "$DTPIPE" ] || fail "dist/release/dtpipe not built — run ./build.sh"

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

query() { echo "SELECT i AS id, i*1.5 AS val, repeat('x', 64) AS pad FROM range(0, $1) t(i)"; }

# Peak working set of a single-branch run, as the run itself reports it.
read_peak() {
    local rows="$1" metrics="$WORK/m.json"
    rm -f "$metrics"
    "$DTPIPE" -i duck::memory: -q "$(query "$rows") $2" -o null: \
        --metrics-path "$metrics" --no-stats >/dev/null 2>&1 || return 1
    awk -F'[:,]' '/PeakMemoryWorkingSetMb/ {printf "%.0f", $2}' "$metrics"
}

# A DAG run writes one metrics file per branch over the same path, so its peak is read off the
# run summary instead — the maximum over the branches, on stderr.
fanout_peak() {
    "$DTPIPE" -i duck::memory: -q "$(query "$1")" --alias src \
        --from src --sql "SELECT id FROM src" -o null: \
        --from src --sql "SELECT val, pad FROM src" -o null: 2>&1 >/dev/null \
        | sed -n 's/.*peak \([0-9][0-9]*\) MB.*/\1/p' | tail -1
}

# ── 0. Calibration: the measure must move when the rows are held ──────────────
SMALL_MB=$(read_peak "$SMALL") || fail "the $SMALL-row read failed"
HELD_MB=$(read_peak "$BIG" "ORDER BY i DESC") || fail "the buffering control run failed"
CALIBRATION=$(( HELD_MB - SMALL_MB ))
echo "  ${SMALL} rows: ${SMALL_MB} MB · ${BIG} rows held in memory: ${HELD_MB} MB"

[ "$CALIBRATION" -ge "$CALIBRATION_FLOOR_MB" ] \
    || skip "holding ${BIG} rows moved the peak by only ${CALIBRATION} MB on this machine — the measure cannot see a result held whole, so nothing below would prove anything"
pass "the peak moves by ${CALIBRATION} MB when the rows are held — the measure can see it"

# A quarter of what claim 0 just demonstrated: far below the cost of holding the result, far
# above the few MB that separate two runs of the same pipeline.
BUDGET_MB=$(( CALIBRATION / 4 ))

# ── 1. A duck: read releases what it imports ──────────────────────────────────
BIG_MB=$(read_peak "$BIG") || fail "the $BIG-row read failed"
GROWTH=$(( BIG_MB - SMALL_MB ))
echo "  streaming ${BIG} rows: ${BIG_MB} MB · growth ${GROWTH} MB · budget ${BUDGET_MB} MB"

[ "$GROWTH" -le "$BUDGET_MB" ] \
    || fail "reading 20x the rows cost ${GROWTH} MB more — the result is no longer streamed chunk by chunk"
pass "20x the rows costs no more memory — the read streams"

# ── 2. Fan-out releases too ───────────────────────────────────────────────────
FAN_SMALL_MB=$(fanout_peak "$SMALL"); FAN_BIG_MB=$(fanout_peak "$BIG")
[ -n "$FAN_SMALL_MB" ] && [ -n "$FAN_BIG_MB" ] || fail "a fan-out run printed no peak — did the run fail?"
FAN_GROWTH=$(( FAN_BIG_MB - FAN_SMALL_MB ))
echo "  fan-out ${SMALL} rows: ${FAN_SMALL_MB} MB · ${BIG} rows: ${FAN_BIG_MB} MB · growth ${FAN_GROWTH} MB"

[ "$FAN_GROWTH" -le "$BUDGET_MB" ] \
    || fail "fan-out cost ${FAN_GROWTH} MB more on 20x the rows — the broadcast is buffering instead of passing batches through"
pass "two consumers of one reader stream too"

echo ""
echo -e "${GREEN}duck: streaming validated${NC}"
