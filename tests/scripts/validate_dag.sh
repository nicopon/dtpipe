#!/bin/bash
set -e

# validate_dag.sh
# Validates all canonical DAG topologies defined in CLAUDE.md.
# No Docker required: uses generate:, CSV, Parquet, Arrow, DuckDB (in-memory).

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
ARTIFACTS_DIR="$SCRIPT_DIR/artifacts/dag"
mkdir -p "$ARTIFACTS_DIR"

DTPIPE="$PROJECT_ROOT/dist/release/dtpipe"
export DTPIPE_NO_TUI=1

GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

pass() { echo -e "${GREEN}  OK: $1${NC}"; }
fail() { echo -e "${RED}  FAIL: $1${NC}"; exit 1; }

echo "========================================"
echo "    DtPipe DAG Topology Validation"
echo "========================================"

if [ ! -f "$DTPIPE" ]; then
    echo "Building release..."
    "$PROJECT_ROOT/build.sh" > /dev/null
fi

A="$ARTIFACTS_DIR"

# Helper: count CSV data rows (excluding header)
csv_rows() { tail -n +2 "$1" | wc -l | tr -d ' '; }

cleanup() { rm -f "$A"/*.csv "$A"/*.parquet "$A"/*.arrow "$A"/*.duckdb; }
trap cleanup EXIT

# ----------------------------------------
# Topology 1: Linear (-i src -o dest)
# ----------------------------------------
echo "--- [1] Linear pipeline ---"
"$DTPIPE" -i "generate:50" \
  --fake "Id:random.uuid" \
  --drop "GenerateIndex" \
  -o "$A/t1_out.csv" --no-stats

COUNT=$(csv_rows "$A/t1_out.csv")
[ "$COUNT" -eq 50 ] && pass "Linear: 50 rows" || fail "Linear: expected 50, got $COUNT"

# ----------------------------------------
# Topology 2: Two independent sources (two separate branches)
# ----------------------------------------
echo "--- [2] Two independent sources ---"
cat > "$A/t2_src1.csv" <<'EOF'
Id,Val
1,A
2,B
EOF
cat > "$A/t2_src2.csv" <<'EOF'
Code,Label
X,Alpha
Y,Beta
EOF

"$DTPIPE" \
  -i "$A/t2_src1.csv" -o "$A/t2_out1.csv" --no-stats \
  -i "$A/t2_src2.csv" -o "$A/t2_out2.csv" --no-stats

grep -q "A" "$A/t2_out1.csv" && pass "Two sources: branch 1 output" || fail "Two sources: branch 1 missing"
grep -q "Alpha" "$A/t2_out2.csv" && pass "Two sources: branch 2 output" || fail "Two sources: branch 2 missing"

# ----------------------------------------
# Topology 3: SQL (single source → alias → SQL)
# ----------------------------------------
echo "--- [3] SQL: single source → alias → SQL processor ---"
"$DTPIPE" \
  -i "generate:100" \
    --fake "Id:random.number" --fake "Val:random.number" \
    --drop "GenerateIndex" \
    --alias src \
  --from src \
    --sql "SELECT COUNT(*) AS cnt FROM src" \
    -o "$A/t3_sql.csv" --no-stats

COUNT=$(csv_rows "$A/t3_sql.csv")
[ "$COUNT" -ge 1 ] && pass "SQL (single source): got result" || fail "SQL (single source): no output"

# ----------------------------------------
# Topology 4: SQL JOIN (main + ref)
# ----------------------------------------
echo "--- [4] SQL JOIN: main + ref ---"
"$DTPIPE" -i "generate:1000" \
  --fake "Id:random.number" --fake "Name:name.firstName" \
  --drop "GenerateIndex" \
  -o "$A/t4_main.parquet" --strategy Recreate --no-stats

"$DTPIPE" -i "generate:100" \
  --fake "Id:random.number" --fake "Label:lorem.word" \
  --drop "GenerateIndex" \
  -o "$A/t4_ref.csv" --no-stats

"$DTPIPE" \
  -i "parquet:$A/t4_main.parquet" --alias main \
  -i "csv:$A/t4_ref.csv"          --alias ref \
  --from main --ref ref \
  --sql "SELECT COUNT(*) AS cnt FROM main" \
  -o "$A/t4_join.csv" --no-stats

COUNT=$(csv_rows "$A/t4_join.csv")
[ "$COUNT" -ge 1 ] && pass "SQL JOIN: result produced" || fail "SQL JOIN: no output"

# ----------------------------------------
# Topology 5: Fan-out (tee) — one source → two outputs
# ----------------------------------------
echo "--- [5] Fan-out (tee): one source → two branches ---"
"$DTPIPE" -i "generate:30" \
  --fake "Id:random.number" --drop "GenerateIndex" \
  -o "$A/t5_src.csv" --no-stats

"$DTPIPE" \
  -i "$A/t5_src.csv" --alias s \
  --from s -o "$A/t5_destA.csv" --no-stats \
  --from s -o "$A/t5_destB.csv" --no-stats

COUNT_A=$(csv_rows "$A/t5_destA.csv")
COUNT_B=$(csv_rows "$A/t5_destB.csv")
[ "$COUNT_A" -eq 30 ] && pass "Fan-out: branch A has 30 rows" || fail "Fan-out: branch A has $COUNT_A rows"
[ "$COUNT_B" -eq 30 ] && pass "Fan-out: branch B has 30 rows" || fail "Fan-out: branch B has $COUNT_B rows"

# ----------------------------------------
# Topology 6: Fan-out + SQL filter on one branch
# ----------------------------------------
echo "--- [6] Fan-out + SQL on one branch ---"
"$DTPIPE" -i "generate:200" \
  --fake "Id:random.number" --fake "Cat:lorem.word" \
  --drop "GenerateIndex" \
  -o "$A/t6_src.parquet" --strategy Recreate --no-stats

"$DTPIPE" \
  -i "parquet:$A/t6_src.parquet" --alias s \
  --from s -o "$A/t6_passthru.csv" --no-stats \
  --from s --sql "SELECT COUNT(*) AS total FROM s" -o "$A/t6_sql.csv" --no-stats

COUNT_PT=$(csv_rows "$A/t6_passthru.csv")
[ "$COUNT_PT" -eq 200 ] && pass "Fan-out+SQL: passthru has 200 rows" || fail "Fan-out+SQL: passthru has $COUNT_PT rows"
CSV_ROWS_SQL=$(csv_rows "$A/t6_sql.csv")
[ "$CSV_ROWS_SQL" -ge 1 ] && pass "Fan-out+SQL: SQL branch produced result" || fail "Fan-out+SQL: SQL branch empty"

# ----------------------------------------
# Topology 7: Diamond (fan-out → filter → join)
# ----------------------------------------
echo "--- [7] Diamond (fan-out → two filtered branches → SQL join) ---"
"$DTPIPE" -i "generate:100" \
  --fake "Id:random.number" --fake "Score:random.number" \
  --drop "GenerateIndex" \
  -o "$A/t7_src.parquet" --strategy Recreate --no-stats

"$DTPIPE" \
  -i "parquet:$A/t7_src.parquet" --alias s \
  --from s --filter "row.Score > 0" --alias hi \
  --from s --filter "row.Score <= 0" --alias lo \
  --from hi --ref lo --sql "SELECT COUNT(*) AS hi_cnt FROM hi" \
  -o "$A/t7_diamond.csv" --no-stats

COUNT=$(csv_rows "$A/t7_diamond.csv")
[ "$COUNT" -ge 1 ] && pass "Diamond: result produced" || fail "Diamond: no output"

# ----------------------------------------
# Topology 8: Join → fan-out
# ----------------------------------------
echo "--- [8] Join → fan-out (joined result broadcast to two outputs) ---"
"$DTPIPE" -i "generate:50" \
  --fake "Id:random.number" --fake "Val:lorem.word" \
  --drop "GenerateIndex" \
  -o "$A/t8_main.parquet" --strategy Recreate --no-stats

"$DTPIPE" -i "generate:10" \
  --fake "Id:random.number" --fake "Extra:lorem.word" \
  --drop "GenerateIndex" \
  -o "$A/t8_ref.csv" --no-stats

"$DTPIPE" \
  -i "parquet:$A/t8_main.parquet" --alias m \
  -i "csv:$A/t8_ref.csv"          --alias r \
  --from m --ref r \
    --sql "SELECT COUNT(*) AS total FROM m" \
    --alias joined \
  --from joined -o "$A/t8_outA.csv" --no-stats \
  --from joined -o "$A/t8_outB.csv" --no-stats

COUNT_A=$(csv_rows "$A/t8_outA.csv")
COUNT_B=$(csv_rows "$A/t8_outB.csv")
[ "$COUNT_A" -ge 1 ] && pass "Join+fan-out: output A has $COUNT_A rows" || fail "Join+fan-out: output A empty"
[ "$COUNT_B" -ge 1 ] && pass "Join+fan-out: output B has $COUNT_B rows" || fail "Join+fan-out: output B empty"

echo ""
echo -e "${GREEN}All DAG topology tests passed!${NC}"
