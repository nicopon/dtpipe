#!/bin/bash
# validate_sql.sh
# Validates advanced SQL capabilities on the DuckDB engine.
# Focuses on window functions, rolling aggregates, and time-based bucketing.
#
# NOTE on "streamed aggregates":
# DuckDB is a batch SQL engine. It operates on a fixed snapshot —
# it does NOT support continuous/incremental processing over a live stream
# (that would require Flink, Kafka Streams, etc.).
#
# Window functions CAN emit results row-by-row (streaming output) because they
# do not collapse rows. This differs from GROUP BY which must see all rows first.
# DuckDB can therefore produce window function results lazily, chunk by chunk.
#
# For time-based windowing, the timestamp must come FROM THE DATA (event time).
# "Every X seconds based on sysdate" must be handled by DtPipe splitting the
# pipeline into time slices — not by the SQL engine itself.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
ARTIFACTS_DIR="$SCRIPT_DIR/artifacts/sql"
mkdir -p "$ARTIFACTS_DIR"

DTPIPE="$PROJECT_ROOT/dist/release/dtpipe"
export DTPIPE_NO_TUI=1

GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

FAILURES=0

pass() { echo -e "  ${GREEN}OK: $1${NC}"; }
fail() { echo -e "  ${RED}FAIL: $1${NC}"; FAILURES=$((FAILURES + 1)); }

verify_baseline() {
    local actual_file=$1 name=$2
    local baseline_file="$SCRIPT_DIR/baselines/${name}.csv"
    
    mkdir -p "$SCRIPT_DIR/baselines"

    if [ "$UPDATE_BASELINES" = "1" ]; then
        cp "$actual_file" "$baseline_file"
        pass "$name: baseline updated"
    else
        if [ ! -f "$baseline_file" ]; then
            fail "$name: baseline not found at $baseline_file. Run with UPDATE_BASELINES=1 to generate."
            return
        fi
        
        if diff -u "$baseline_file" "$actual_file" > "$A/temp.diff"; then
            pass "$name: matches baseline"
            rm -f "$A/temp.diff"
        else
            echo -e "${RED}  FAIL: $name: mismatch with baseline! Diff:${NC}"
            cat "$A/temp.diff"
            rm -f "$A/temp.diff"
            fail "$name: mismatch with baseline"
        fi
    fi
}

echo "========================================"
echo "    DtPipe SQL Feature Validation"
echo "========================================"

if [ ! -f "$DTPIPE" ]; then
    echo "Building release..."
    "$PROJECT_ROOT/build.sh" > /dev/null
fi

A="$ARTIFACTS_DIR"

# Helper: run a query on DuckDB, call check_func with (output_csv, engine).
# Does NOT exit on failure — all tests run regardless of individual failures.
validate_engine() {
    local name="$1"
    local source_args="$2"   # -i ... --alias src
    local sql="$3"
    local check_func="$4"

    echo "  [SQL Engine: DuckDB] $name..."
    rm -f "$A/out.csv"
    if eval "\"$DTPIPE\" $source_args --from src --sql \"$sql\" -o \"$A/out.csv\" --no-stats" 2>/dev/null; then
        $check_func "$A/out.csv" "duckdb"
    else
        fail "$name (duckdb): pipeline failed"
    fi
}

# ─────────────────────────────────────────────────────────────────────────────
# T1: ROW_NUMBER — basic row numbering
# ─────────────────────────────────────────────────────────────────────────────
echo "--- [T1] ROW_NUMBER: row numbering (streaming-friendly) ---"
check_t1() {
    verify_baseline "$1" "T1_ROW_NUMBER"
}
validate_engine "T1: ROW_NUMBER" \
    "-i \"generate:50\" --fake \"Val:random.number\" --fake-seed-row --drop \"GenerateIndex\" --alias src" \
    "SELECT Val, ROW_NUMBER() OVER (ORDER BY Val) AS rn FROM src" \
    check_t1

# ─────────────────────────────────────────────────────────────────────────────
# T2: N-record batching via ROW_NUMBER
# ─────────────────────────────────────────────────────────────────────────────
echo "--- [T2] Batching by N records (CTE + FLOOR + ROW_NUMBER) ---"
BATCH_SIZE=10
TOTAL_ROWS=50
EXPECTED_BATCHES=$(( TOTAL_ROWS / BATCH_SIZE ))
check_t2() {
    verify_baseline "$1" "T2_Batch_N"
}
validate_engine "T2: Batching by N records" \
    "-i \"generate:$TOTAL_ROWS\" --fake \"Val:random.number\" --fake-seed-row --drop \"GenerateIndex\" --alias src" \
    "WITH numbered AS (SELECT Val, ROW_NUMBER() OVER (ORDER BY Val) AS rn FROM src) SELECT FLOOR(CAST(rn - 1 AS DOUBLE) / $BATCH_SIZE) AS batch_id, COUNT(*) AS cnt, SUM(Val) AS total FROM numbered GROUP BY batch_id ORDER BY batch_id" \
    check_t2

# ─────────────────────────────────────────────────────────────────────────────
# T3: Running (cumulative) SUM
# ─────────────────────────────────────────────────────────────────────────────
echo "--- [T3] Running (cumulative) SUM ---"
check_t3() {
    verify_baseline "$1" "T3_Running_SUM"
}
validate_engine "T3: Running SUM" \
    "-i \"generate:20\" --fake \"Val:random.number\" --fake-seed-row --drop \"GenerateIndex\" --alias src" \
    "SELECT Val, SUM(Val) OVER (ORDER BY Val ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total FROM src" \
    check_t3

# ─────────────────────────────────────────────────────────────────────────────
# T4: Moving average (sliding window of last N rows)
# ─────────────────────────────────────────────────────────────────────────────
echo "--- [T4] Moving average (5-row sliding window) ---"
check_t4() {
    verify_baseline "$1" "T4_Moving_average"
}
validate_engine "T4: Moving average" \
    "-i \"generate:30\" --fake \"Val:random.number\" --fake-seed-row --drop \"GenerateIndex\" --alias src" \
    "SELECT Val, AVG(Val) OVER (ORDER BY Val ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS moving_avg FROM src" \
    check_t4

# ─────────────────────────────────────────────────────────────────────────────
# T5: NTILE — divide rows into N equal buckets
# ─────────────────────────────────────────────────────────────────────────────
echo "--- [T5] NTILE buckets ---"
check_t5() {
    verify_baseline "$1" "T5_NTILE"
}
validate_engine "T5: NTILE(4)" \
    "-i \"generate:100\" --fake \"Val:random.number\" --fake-seed-row --drop \"GenerateIndex\" --alias src" \
    "SELECT Val, NTILE(4) OVER (ORDER BY Val) AS quartile FROM src" \
    check_t5

# ─────────────────────────────────────────────────────────────────────────────
# T6: RANK / DENSE_RANK
# ─────────────────────────────────────────────────────────────────────────────
echo "--- [T6] RANK and DENSE_RANK ---"
check_t6() {
    verify_baseline "$1" "T6_RANK"
}
validate_engine "T6: RANK" \
    "-i \"generate:20\" --fake \"Val:random.number\" --fake-seed-row --drop \"GenerateIndex\" --alias src" \
    "SELECT Val, RANK() OVER (ORDER BY Val) AS rnk, DENSE_RANK() OVER (ORDER BY Val) AS dense_rnk FROM src" \
    check_t6

# ─────────────────────────────────────────────────────────────────────────────
# T7: Time-bucket aggregation on event timestamps (NOT sysdate)
# ─────────────────────────────────────────────────────────────────────────────
echo "--- [T7] Time-bucket aggregation (event-time, 10-second windows) ---"
cat > "$A/t7_events.csv" << 'EOF'
ts_epoch,val
0,10
3,20
7,30
12,40
15,50
18,60
25,70
28,80
35,90
38,100
42,110
55,120
58,130
EOF
check_t7() {
    verify_baseline "$1" "T7_Time_bucket"
}
validate_engine "T7: Time-bucket (10s windows)" \
    "-i \"$A/t7_events.csv\" --column-types \"ts_epoch:double,val:double\" --alias src" \
    "SELECT FLOOR(ts_epoch / 10) * 10 AS bucket, COUNT(*) AS cnt, SUM(val) AS total FROM src GROUP BY bucket ORDER BY bucket" \
    check_t7

# ─────────────────────────────────────────────────────────────────────────────
# T8: LAG / LEAD — access previous/next row value
# ─────────────────────────────────────────────────────────────────────────────
# T8: LAG / LEAD — access previous/next row value
# ─────────────────────────────────────────────────────────────────────────────
echo "--- [T8] LAG / LEAD (delta between consecutive rows) ---"
check_t8() {
    verify_baseline "$1" "T8_LAG_LEAD"
}
validate_engine "T8: LAG/LEAD delta" \
    "-i \"generate:10\" --fake \"Val:random.number\" --fake-seed-row --drop \"GenerateIndex\" --alias src" \
    "SELECT Val, Val - COALESCE(LAG(Val, 1) OVER (ORDER BY Val), Val) AS delta FROM src ORDER BY Val" \
    check_t8

# ─────────────────────────────────────────────────────────────────────────────
# T9: PARTITION BY — window per group
# ─────────────────────────────────────────────────────────────────────────────
echo "--- [T9] PARTITION BY (rank within groups) ---"
cat > "$A/t9_groups.csv" << 'EOF'
grp,val
A,10
A,30
A,20
B,5
B,15
B,25
C,100
C,50
EOF
check_t9() {
    verify_baseline "$1" "T9_PARTITION_BY"
}
validate_engine "T9: PARTITION BY" \
    "-i \"$A/t9_groups.csv\" --column-types \"grp:string,val:double\" --alias src" \
    "SELECT grp, val, RANK() OVER (PARTITION BY grp ORDER BY val DESC) AS grp_rank FROM src ORDER BY grp, val DESC" \
    check_t9

# ─────────────────────────────────────────────────────────────────────────────
# Summary
# ─────────────────────────────────────────────────────────────────────────────
echo ""
if [ "$FAILURES" -eq 0 ]; then
    echo -e "${GREEN}All SQL feature tests passed!${NC}"
else
    echo -e "${RED}${FAILURES} check(s) failed.${NC}"
    exit 1
fi
echo ""
echo "Notes:"
echo "  - Window functions (T1,T3,T4,T5,T6,T8,T9) do NOT collapse rows → streaming-compatible output"
echo "  - Aggregations with GROUP BY (T2,T7) require full scan → materialized in both engines"
echo "  - Time-windowing (T7) uses event timestamps from the data, not wall-clock time"
echo "  - True wall-clock streaming ('every 5s based on sysdate') requires DtPipe pipeline"
echo "    orchestration, not SQL-level handling"
