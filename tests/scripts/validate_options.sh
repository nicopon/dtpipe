#!/bin/bash
set -e

# validate_options.sh
# Tests: provider option scoping (global vs writer-only vs YAML), sampling (rate + seed).

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
ARTIFACTS_DIR="$SCRIPT_DIR/artifacts/options"
mkdir -p "$ARTIFACTS_DIR"

DTPIPE="$PROJECT_ROOT/dist/release/dtpipe"
export DTPIPE_NO_TUI=1

GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

pass() { echo -e "${GREEN}  OK: $1${NC}"; }
fail() { echo -e "${RED}  FAIL: $1${NC}"; exit 1; }

echo "========================================"
echo "    DtPipe Options Validation"
echo "========================================"

if [ ! -f "$DTPIPE" ]; then
    echo "Building release..."
    "$PROJECT_ROOT/build.sh" > /dev/null
fi

# ----------------------------------------
# Test data setup
# ----------------------------------------
cat > "$ARTIFACTS_DIR/in_comma.csv" <<'EOF'
id,val,score
1,A,100
2,B,200
EOF

cat > "$ARTIFACTS_DIR/in_pipe.csv" <<'EOF'
id.val.score
1.A.100
2.B.200
EOF

# ----------------------------------------
# 1. Global option scoping (flag BEFORE -o → applies to both reader and writer)
# ----------------------------------------
echo "--- [1] Global option scoping (--csv-separator before -o) ---"
"$DTPIPE" \
  --csv-separator . \
  -i "$ARTIFACTS_DIR/in_pipe.csv" \
  -o "csv:$ARTIFACTS_DIR/out_global.csv" --no-stats

grep -q "\." "$ARTIFACTS_DIR/out_global.csv" \
  && pass "Global separator '.' applied to writer" \
  || fail "Global separator not applied to writer"

# ----------------------------------------
# 2. Writer-only scoping (flag AFTER -o → applies only to writer)
# ----------------------------------------
echo "--- [2] Writer-only scoping (--csv-separator after -o) ---"
"$DTPIPE" \
  -i "$ARTIFACTS_DIR/in_comma.csv" \
  -o "csv:$ARTIFACTS_DIR/out_scoped.csv" \
  --csv-separator . --no-stats

grep -q "\." "$ARTIFACTS_DIR/out_scoped.csv" \
  && pass "Scoped separator '.' applied to writer" \
  || fail "Scoped separator not applied to writer"

grep -q "100" "$ARTIFACTS_DIR/out_scoped.csv" \
  && pass "Reader defaulted to comma (value '100' present)" \
  || fail "Reader incorrectly used writer-scoped separator"

# ----------------------------------------
# 3. YAML provider-options scoping
# ----------------------------------------
echo "--- [3] YAML provider-options scoping ---"
cat > "$ARTIFACTS_DIR/job_config.yaml" <<EOF
input: "$ARTIFACTS_DIR/in_comma.csv"
output: "csv:$ARTIFACTS_DIR/out_yaml.csv"
provider-options:
  csv-writer:
    separator: ";"
EOF

"$DTPIPE" --job "$ARTIFACTS_DIR/job_config.yaml" --no-stats

grep -q ";" "$ARTIFACTS_DIR/out_yaml.csv" \
  && pass "YAML scoped csv-writer separator ';' applied" \
  || fail "YAML scoped separator not applied"

# ----------------------------------------
# 4. Sampling rate and seed
# ----------------------------------------
echo "--- [4] Sampling (10% of 100 rows) ---"
"$DTPIPE" -i "generate:100" \
  --fake "Id:random.number" \
  --fake "Name:name.fullName" \
  --drop "GenerateIndex" \
  -o "csv:$ARTIFACTS_DIR/sampling.csv" \
  --sampling-rate 0.1 \
  --sampling-seed 12345 --no-stats

ROW_COUNT=$(wc -l < "$ARTIFACTS_DIR/sampling.csv" | tr -d ' ')
ROW_COUNT=$((ROW_COUNT - 1))  # subtract header

[ "$ROW_COUNT" -gt 0 ] && [ "$ROW_COUNT" -lt 30 ] \
  && pass "Sampling: got $ROW_COUNT rows (expected ~10)" \
  || fail "Sampling: unexpected row count $ROW_COUNT (expected 1-30)"

# ----------------------------------------
# 5. Sampling determinism (same seed → same count)
# ----------------------------------------
echo "--- [5] Sampling determinism ---"
"$DTPIPE" -i "generate:100" \
  --fake "Id:random.number" --drop "GenerateIndex" \
  -o "csv:$ARTIFACTS_DIR/sampling_a.csv" \
  --sampling-rate 0.1 --sampling-seed 42 --no-stats

"$DTPIPE" -i "generate:100" \
  --fake "Id:random.number" --drop "GenerateIndex" \
  -o "csv:$ARTIFACTS_DIR/sampling_b.csv" \
  --sampling-rate 0.1 --sampling-seed 42 --no-stats

COUNT_A=$(wc -l < "$ARTIFACTS_DIR/sampling_a.csv" | tr -d ' ')
COUNT_B=$(wc -l < "$ARTIFACTS_DIR/sampling_b.csv" | tr -d ' ')
[ "$COUNT_A" -eq "$COUNT_B" ] \
  && pass "Sampling determinism: same seed → same count ($COUNT_A)" \
  || fail "Sampling not deterministic: $COUNT_A vs $COUNT_B"

# ----------------------------------------
# 6. YAML provider-options: sqlite writer with custom table name and strategy
# ----------------------------------------
echo "--- [6] YAML provider-options (sqlite writer) ---"
cat > "$ARTIFACTS_DIR/job_provider.yaml" <<EOF
input: "duck::memory:"
query: "SELECT 1 as id, 'Test' as name"
output: "sqlite:$ARTIFACTS_DIR/provider_opts.db"
provider-options:
  sqlite:
    table: "CustomTable"
    strategy: "Recreate"
EOF

"$DTPIPE" --job "$ARTIFACTS_DIR/job_provider.yaml" --no-stats

TABLE_COUNT=$("$DTPIPE" -i "sqlite:$ARTIFACTS_DIR/provider_opts.db" \
  --query "SELECT count(*) as cnt FROM sqlite_master WHERE type='table' AND name='CustomTable'" \
  -o csv --no-stats 2>/dev/null | tail -1 | tr -d '\r ')
[ "$TABLE_COUNT" = "1" ] \
  && pass "YAML provider-options: CustomTable created via sqlite writer" \
  || fail "YAML provider-options: CustomTable not found (got '$TABLE_COUNT')"

# ----------------------------------------
# 7. --metrics-path: verify JSON metrics are emitted
# ----------------------------------------
echo "--- [7] --metrics-path ---"
"$DTPIPE" -i "generate:50" \
  --fake "Id:random.number" \
  --drop "GenerateIndex" \
  -o "csv:$ARTIFACTS_DIR/metrics_out.csv" \
  --metrics-path "$ARTIFACTS_DIR/metrics.json" --no-stats

[ -f "$ARTIFACTS_DIR/metrics.json" ] \
  && pass "--metrics-path: metrics.json created" \
  || fail "--metrics-path: metrics.json not created"

grep -qi "ReadCount" "$ARTIFACTS_DIR/metrics.json" \
  && pass "--metrics-path: ReadCount present in JSON" \
  || fail "--metrics-path: ReadCount missing from JSON"

# ----------------------------------------
# Cleanup
# ----------------------------------------
rm -f "$ARTIFACTS_DIR"/*.csv "$ARTIFACTS_DIR"/*.yaml "$ARTIFACTS_DIR"/*.db "$ARTIFACTS_DIR"/*.json

echo ""
echo -e "${GREEN}Options validation complete!${NC}"
