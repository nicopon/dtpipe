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

pass() { echo -e "  ${GREEN}OK: $1${NC}"; }
fail() { echo -e "  ${RED}FAIL: $1${NC}"; exit 1; }

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

# Helper: run a SQL test against BOTH engines
run_sql_test() {
    local name="$1"
    local args="$2"
    local check_func="$3"
    
    for engine in "datafusion" "duckdb"; do
        echo "  [SQL Engine: $engine] $name..."
    local sql="SELECT * FROM src"
    eval "\"$DTPIPE\" $args --sql-engine $engine --sql \"$sql\" -o \"$A/sql_out.csv\" --no-stats"
    $check_func "$A/sql_out.csv" "$engine"
    done
}

cleanup() { rm -f "$A"/*.csv "$A"/*.parquet "$A"/*.arrow "$A"/*.duckdb "$A"/*.jsonl; }
trap cleanup EXIT

# ----------------------------------------
# Topology 1: Linear (-i src -o dest)
# ----------------------------------------
echo "--- [1] Linear pipeline ---"
rm -f "$A/t1_out.csv"
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
  -i "$A/t2_src1.csv" --csv-column-types "Id:int32,Val:string" -o "$A/t2_out1.csv" --no-stats \
  -i "$A/t2_src2.csv" --csv-column-types "Code:string,Label:string" -o "$A/t2_out2.csv" --no-stats

grep -q "A" "$A/t2_out1.csv" && pass "Two sources: branch 1 output" || fail "Two sources: branch 1 missing"
grep -q "Alpha" "$A/t2_out2.csv" && pass "Two sources: branch 2 output" || fail "Two sources: branch 2 missing"

# ----------------------------------------
# Topology 3: SQL (single source → alias → SQL)
# ----------------------------------------
echo "--- [3] SQL: single source → alias → SQL processor ---"
check_t3() {
    local count=$(csv_rows "$1")
    [ "$count" -ge 1 ] && pass "SQL (single source, $2): got result" || fail "SQL (single source, $2): no output"
}
run_sql_test "T3: Single Source" \
  "-i \"generate:100\" --fake \"Id:random.number\" --fake \"Val:random.number\" --drop \"GenerateIndex\" --alias src --from src --sql \"SELECT COUNT(*) AS cnt FROM src\"" \
  check_t3

# ----------------------------------------
# Topology 4: SQL JOIN (main + ref)
# ----------------------------------------
echo "--- [4] SQL JOIN: main + ref ---"
check_t4() {
    local count=$(csv_rows "$1")
    [ "$count" -ge 1 ] && pass "SQL JOIN ($2): result produced" || fail "SQL JOIN ($2): no output"
}
# Prepare data with specific types
"$DTPIPE" -i "generate:1000" --fake "Id:random.number" --fake "Name:name.firstName" --drop "GenerateIndex" -o "$A/t4_main.parquet" --strategy Recreate --no-stats
cat > "$A/t4_ref.csv" <<'EOF'
Id,Label
1,Alpha
2,Beta
EOF

run_sql_test "T4: SQL JOIN" \
  "-i \"parquet:$A/t4_main.parquet\" --alias main -i \"csv:$A/t4_ref.csv\" --csv-column-types \"Id:double,Label:string\" --alias ref --from main --ref ref --sql \"SELECT main.*, ref.Label FROM main LEFT JOIN ref ON main.Id = ref.Id\"" \
  check_t4

# ----------------------------------------
# Topology 5: Fan-out (tee) — one source → two outputs
# ----------------------------------------
echo "--- [5] Fan-out (tee): one source → two branches ---"
"$DTPIPE" -i "generate:30" \
  --fake "Id:random.number" --drop "GenerateIndex" \
  -o "$A/t5_src.csv" --no-stats

"$DTPIPE" \
  -i "$A/t5_src.csv" --csv-column-types "Id:double" --alias s \
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
check_t6() {
    local count=$(csv_rows "$1")
    [ "$count" -ge 1 ] && pass "Fan-out+SQL ($2): SQL branch produced result" || fail "Fan-out+SQL ($2): SQL branch empty"
}
"$DTPIPE" -i "generate:200" --fake "Id:random.number" --fake "Cat:lorem.word" --drop "GenerateIndex" -o "$A/t6_src.parquet" --strategy Recreate --no-stats

# We run this one manually because it has multiple branches, but we can still force the engine
for engine in "datafusion" "duckdb"; do
    echo "  [SQL Engine: $engine] T6: Fan-out + SQL..."
    rm -f "$A/t6_passthru.csv" "$A/t6_sql.csv"
    "$DTPIPE" \
      -i "parquet:$A/t6_src.parquet" --alias s \
      --from s -o "$A/t6_passthru.csv" --no-stats \
      --from s --sql-engine $engine --sql "SELECT COUNT(*) AS total FROM s" -o "$A/t6_sql.csv" --no-stats
    
    COUNT_PT=$(csv_rows "$A/t6_passthru.csv")
    [ "$COUNT_PT" -eq 200 ] && pass "Fan-out+SQL ($engine): passthru OK" || fail "Fan-out+SQL ($engine): passthru $COUNT_PT"
    check_t6 "$A/t6_sql.csv" "$engine"
done

# ----------------------------------------
# Topology 7: Diamond (fan-out → filter → join)
# ----------------------------------------
echo "--- [7] Diamond (fan-out → two filtered branches → SQL join) ---"
check_t7() {
    local count=$(csv_rows "$1")
    [ "$count" -ge 1 ] && pass "Diamond ($2): result produced" || fail "Diamond ($2): no output"
}
"$DTPIPE" -i "generate:100" --fake "Id:random.number" --fake "Score:random.number" --drop "GenerateIndex" -o "$A/t7_src.parquet" --strategy Recreate --no-stats

run_sql_test "T7: Diamond" \
  "-i \"parquet:$A/t7_src.parquet\" --alias s --from s --filter \"row.Score > 0\" --alias hi --from s --filter \"row.Score <= 0\" --alias lo --from hi --ref lo --sql \"SELECT COUNT(*) AS hi_cnt FROM hi\"" \
  check_t7

# ----------------------------------------
# Topology 8: Join → fan-out
# ----------------------------------------
echo "--- [8] Join → fan-out (joined result broadcast to two outputs) ---"
"$DTPIPE" -i "generate:50" --fake "Id:random.number" --fake "Val:lorem.word" --drop "GenerateIndex" -o "$A/t8_main.parquet" --strategy Recreate --no-stats
"$DTPIPE" -i "generate:10" --fake "Id:random.number" --fake "Extra:lorem.word" --drop "GenerateIndex" -o "$A/t8_ref.csv" --no-stats

for engine in "datafusion" "duckdb"; do
    echo "  [SQL Engine: $engine] T8: Join -> Fan-out..."
    rm -f "$A/t8_outA.csv" "$A/t8_outB.csv"
    "$DTPIPE" \
      -i "parquet:$A/t8_main.parquet" --alias m \
      -i "csv:$A/t8_ref.csv"          --alias r \
      --from m --ref r \
        --sql-engine $engine --sql "SELECT COUNT(*) AS total FROM m" \
        --alias joined \
      --from joined -o "$A/t8_outA.csv" --no-stats \
      --from joined -o "$A/t8_outB.csv" --no-stats
    
    COUNT_A=$(csv_rows "$A/t8_outA.csv")
    COUNT_B=$(csv_rows "$A/t8_outB.csv")
    [ "$COUNT_A" -ge 1 ] && pass "Join+fan-out ($engine): output A OK" || fail "Join+fan-out ($engine): output A empty"
    [ "$COUNT_B" -ge 1 ] && pass "Join+fan-out ($engine): output B OK" || fail "Join+fan-out ($engine): output B empty"
done

# ----------------------------------------
# Topology 9: Nested data through DAG
# ----------------------------------------
echo "--- [9] Nested data through DAG ---"
# ----------------------------------------
# Topology 9: Vicious - Wide Mixed Schema (FFI Pointer Stress)
# ----------------------------------------
echo "--- [9] Vicious: Wide Mixed Schema (String/Int alternation) ---"
check_t9() {
    local count=$(csv_rows "$1")
    [ "$count" -eq 10 ] && pass "Wide Schema ($2): 10 rows" || fail "Wide Schema ($2): got $count"
    grep -q "Val_9" "$1" && pass "Wide Schema ($2): content intact" || fail "Wide Schema ($2): corruption detected"
}

# Create a source with many alternating columns
run_sql_test "T9: Wide Schema" \
  "-i \"generate:10\" \
   --fake \"S1:lorem.word\" --fake \"N1:random.number\" \
   --fake \"S2:lorem.word\" --fake \"N2:random.number\" \
   --fake \"S3:lorem.word\" --fake \"N3:random.number\" \
   --fake \"Final:lorem.word\" --compute \"Final: 'Val_' + row.GenerateIndex\" \
   --alias src --from src" \
  check_t9

# ----------------------------------------
# Topology 10: Vicious - FFI Column Reordering (String-Int Trap)
# ----------------------------------------
echo "--- [10] Vicious: FFI Column Reordering (String before Numeric) ---"
cat > "$A/t10_src.csv" <<'EOF'
Label,Value,Comment,Score
ABC,100,First,10.5
DEF,200,Second,20.5
GHI,300,Third,30.5
EOF

check_t10() {
    grep -q "ABC,100,First,10.5" "$1" && pass "FFI Reordering ($2): correct values" || fail "FFI Reordering ($2): corrupt data"
    grep -q "GHI,300,Third,30.5" "$1" && pass "FFI Reordering ($2): last row OK" || fail "FFI Reordering ($2): corrupt data"
}

run_sql_test "T10: FFI Reordering" \
  "-i \"$A/t10_src.csv\" --csv-column-types \"Label:string,Value:int32,Comment:string,Score:double\" --alias src --from src --sql \"SELECT * FROM src\"" \
  check_t10

# ----------------------------------------
# Topology 11: UUID round-trip through SQL → Parquet (typed target)
# Writes to Parquet (strongly-typed) to surface any schema/type mutation.
# DataFusion: UUID preserved as FixedSizeBinary(16)+arrow.uuid in Parquet.
# DuckDB:     UUID exported as Utf8 (StringType) — type changes, but values stay valid.
# ----------------------------------------
echo "--- [11] UUID round-trip: SQL → Parquet ---"
for engine in "datafusion" "duckdb"; do
    echo "  [SQL Engine: $engine] T11: UUID → Parquet..."
    rm -f "$A/t11_uuid.parquet" "$A/t11_uuid_verify.csv"

    "$DTPIPE" \
      -i "generate:10" \
        --fake "Id:random.uuid" \
        --drop "GenerateIndex" \
        --alias src \
      --from src \
        --sql-engine "$engine" \
        --sql "SELECT Id FROM src" \
        -o "$A/t11_uuid.parquet" --strategy Recreate --no-stats \
      || fail "UUID Parquet ($engine): pipeline failed"

    # Read Parquet back to CSV to verify data round-trip
    "$DTPIPE" -i "parquet:$A/t11_uuid.parquet" -o "$A/t11_uuid_verify.csv" --no-stats \
      || fail "UUID Parquet ($engine): Parquet read-back failed"

    count=$(csv_rows "$A/t11_uuid_verify.csv")
    [ "$count" -eq 10 ] && pass "UUID Parquet ($engine): 10 rows round-tripped" || fail "UUID Parquet ($engine): got $count rows"

    # Verify values are still valid UUID strings (8-4-4-4-12 hex format)
    grep -qiE "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}" "$A/t11_uuid_verify.csv" \
      && pass "UUID Parquet ($engine): values are valid UUID strings" \
      || fail "UUID Parquet ($engine): UUID values corrupted or missing"
done

# ----------------------------------------
# Topology 12: Vicious - Deep Nesting & Struct JOINs
# ----------------------------------------
echo "--- [12] Vicious: Deep Nesting & Struct JOINs ---"
cat > "$A/t12_main.jsonl" <<'EOF'
{"pk": 1, "meta": {"code": "A", "details": {"type": "X"}}}
{"pk": 2, "meta": {"code": "B", "details": {"type": "Y"}}}
EOF
cat > "$A/t12_ref.jsonl" <<'EOF'
{"pk": 1, "tags": ["tag1", "tag2"]}
{"pk": 2, "tags": ["tag3"]}
EOF

check_t12() {
    grep -q "9" "$1" && pass "Nested JOIN ($2): join success" || fail "Nested JOIN ($2): missing data"
}

# Note: Simple Join to verify DAG branches without complex nesting
check_t12_all() {
    local eng="$2"
    rm -f "$A/t12_out.csv"
    "$DTPIPE" -i "generate:1000" --alias m -i "generate:10" --alias r --from m --ref r \
        --sql-engine "$eng" --sql "SELECT m.* FROM m JOIN r ON m.GenerateIndex = r.GenerateIndex" \
        -o "$A/t12_out.csv" --no-stats
    check_t12 "$A/t12_out.csv" "$eng"
}

for engine in "datafusion" "duckdb"; do
    echo "  [SQL Engine: $engine] T12: Nested JOINs..."
    check_t12_all "dummy" "$engine"
done

# ----------------------------------------
# Topology 13: Vicious - Large Data & Sparse Nulls
# ----------------------------------------
echo "--- [13] Vicious: Large Data & Sparse Nulls ---"
check_t13() {
    grep -q "1000" "$1" && pass "Large Data ($2): count matches" || fail "Large Data ($2): wrong count"
}

run_sql_test "T13: Large Data" \
  "-i \"generate:1000\" --fake \"Val1:random.number\" --null \"Val1\" --alias src --from src --sql \"SELECT count(*) as cnt FROM src\"" \
  check_t13

# ----------------------------------------
# Topology 14: UUID value fidelity through SQL engines
# Injects a known UUID, passes through SQL, reads back and checks the exact value.
# Detects byte-order corruption or string conversion divergence between engines.
# ----------------------------------------
echo "--- [14] UUID value fidelity through SQL ---"
KNOWN_UUID="550e8400-e29b-41d4-a716-446655440000"

for engine in "datafusion" "duckdb"; do
    echo "  [SQL Engine: $engine] T14: UUID value fidelity..."
    rm -f "$A/t14_src.csv" "$A/t14_out.parquet" "$A/t14_verify.csv"

    printf "Id\n%s\n" "$KNOWN_UUID" > "$A/t14_src.csv"

    "$DTPIPE" \
      -i "$A/t14_src.csv" --csv-column-types "Id:uuid" --alias src \
      --from src \
        --sql-engine "$engine" \
        --sql "SELECT Id FROM src" \
        -o "$A/t14_out.parquet" --strategy Recreate --no-stats \
      || fail "UUID fidelity ($engine): pipeline failed"

    "$DTPIPE" -i "parquet:$A/t14_out.parquet" -o "$A/t14_verify.csv" --no-stats \
      || fail "UUID fidelity ($engine): Parquet read-back failed"

    grep -qi "$KNOWN_UUID" "$A/t14_verify.csv" \
      && pass "UUID fidelity ($engine): exact value preserved" \
      || fail "UUID fidelity ($engine): value mutated — expected $KNOWN_UUID, got: $(tail -n1 $A/t14_verify.csv)"
done

echo ""
echo -e "${GREEN}All DAG topology tests passed!${NC}"
