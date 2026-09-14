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
# 1. Strict option scoping (flag must be repeated for reader and writer if needed)
# ----------------------------------------
echo "--- [1] Strict option scoping (--csv-separator for reader and writer) ---"
"$DTPIPE" \
  -i "$ARTIFACTS_DIR/in_pipe.csv" \
  --csv-separator . \
  -o "csv:$ARTIFACTS_DIR/out_global.csv" \
  --csv-separator . \
  --no-stats

grep -q "\." "$ARTIFACTS_DIR/out_global.csv" \
  && pass "Strict separator '.' applied to writer" \
  || fail "Strict separator not applied to writer"

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
main:
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
main:
  input: "duck::memory:"
  output: "sqlite:$ARTIFACTS_DIR/provider_opts.db"
  provider-options:
    duck:
      query: "SELECT 1 as id, 'Test' as name"
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
# [8] --strict-bindings surfaces unknown flags
# ----------------------------------------
echo "--- [8] --strict-bindings surfaces unknown flags ---"
set +e
OUT=$("$DTPIPE" --unknown-flag -i generate:5 -o "$ARTIFACTS_DIR/strict.csv" --strict-bindings --no-stats 2>&1)
EXIT=$?
set -e
[ "$EXIT" -ne 0 ] \
  && pass "--strict-bindings rejects unknown flag (exit $EXIT)" \
  || fail "--strict-bindings accepted unknown flag (exit 0)"
echo "$OUT" | grep -q "unknown-flag" \
  && pass "--strict-bindings error names offending flag" \
  || fail "--strict-bindings error does not name offending flag"

# Default (lenient) behavior unchanged: unknown flag is skipped with a warning, run succeeds.
set +e
OUT=$("$DTPIPE" -i generate:5 --unknown-flag -o "$ARTIFACTS_DIR/lenient.csv" --no-stats 2>&1)
EXIT=$?
set -e
[ "$EXIT" -eq 0 ] \
  && pass "default behavior still tolerates unknown flags" \
  || fail "unknown flag broke default lenient run (exit $EXIT)"

# ----------------------------------------
# [9] --strict-bindings on an ordinary command line
# ----------------------------------------
# The stage slice a component is bound from opens with the boundary token that defined it, so
# judging it against that component's own flags alone made this exit 1 on every line there is.
echo "--- [9] --strict-bindings on an ordinary command line ---"
for LINE in "-i;$ARTIFACTS_DIR/in_comma.csv;-o;csv:$ARTIFACTS_DIR/strict_ok.csv" \
            "-i;generate:5;-o;null:" \
            "-i;$ARTIFACTS_DIR/in_comma.csv;--limit;1;-o;csv:$ARTIFACTS_DIR/strict_ok2.csv"; do
  IFS=';' read -r -a ARGS <<< "$LINE"
  set +e
  OUT=$("$DTPIPE" "${ARGS[@]}" --strict-bindings --no-stats 2>&1)
  EXIT=$?
  set -e
  [ "$EXIT" -eq 0 ] \
    && pass "--strict-bindings: '${ARGS[*]}' exits 0" \
    || fail "--strict-bindings rejected an ordinary line '${ARGS[*]}' (exit $EXIT): $OUT"
done

# ----------------------------------------
# [10] A provider option reaches the reader (CLI ≡ YAML)
# ----------------------------------------
# --row-count is a long and --quote a char: the CLI binder assigned nothing for a type it did not
# list, so '-r 1000' wrote 100 rows and exited 0 while the same option in YAML wrote 1000.
echo "--- [10] A provider option reaches the reader ---"
for FLAG in "--row-count" "-r"; do
  "$DTPIPE" -i "generate:" "$FLAG" 1000 -o "csv:$ARTIFACTS_DIR/rowcount.csv" --no-stats > /dev/null
  COUNT=$(( $(wc -l < "$ARTIFACTS_DIR/rowcount.csv" | tr -d ' ') - 1 ))
  [ "$COUNT" -eq 1000 ] \
    && pass "generate $FLAG 1000 -> $COUNT rows" \
    || fail "generate $FLAG 1000 -> $COUNT rows (expected 1000)"
done

cat > "$ARTIFACTS_DIR/job_rowcount.yaml" <<EOF
main:
  input: "generate:"
  output: "csv:$ARTIFACTS_DIR/rowcount_yaml.csv"
  provider-options:
    generate:
      row-count: 1000
EOF
"$DTPIPE" --job "$ARTIFACTS_DIR/job_rowcount.yaml" --no-stats > /dev/null
COUNT_YAML=$(( $(wc -l < "$ARTIFACTS_DIR/rowcount_yaml.csv" | tr -d ' ') - 1 ))
[ "$COUNT_YAML" -eq "$COUNT" ] \
  && pass "the command line and the job file agree on row-count ($COUNT)" \
  || fail "row-count: CLI gave $COUNT rows, YAML gave $COUNT_YAML"

"$DTPIPE" -i "$ARTIFACTS_DIR/in_comma.csv" -o "csv:$ARTIFACTS_DIR/quoted.csv" \
  --csv-quote "'" --export-job "$ARTIFACTS_DIR/job_quote.yaml" > /dev/null
grep -q "quote:" "$ARTIFACTS_DIR/job_quote.yaml" \
  && pass "--csv-quote (char) binds and survives --export-job" \
  || fail "--csv-quote did not bind (absent from the exported job file)"

# ----------------------------------------
# [11] --cursor and --state are a pair
# ----------------------------------------
# Half a pair installed no tracking decorator, wrote no state file and said nothing: exit 0, and
# the next run reloaded everything.
echo "--- [11] --cursor and --state are a pair ---"
for HALF in "--cursor;id" "--state;$ARTIFACTS_DIR/half.sync"; do
  IFS=';' read -r -a PAIR <<< "$HALF"
  set +e
  OUT=$("$DTPIPE" -i "generate:5" "${PAIR[@]}" -o "csv:$ARTIFACTS_DIR/cursor_half.csv" --no-stats 2>&1)
  EXIT=$?
  set -e
  [ "$EXIT" -ne 0 ] \
    && pass "${PAIR[0]} alone is refused (exit $EXIT)" \
    || fail "${PAIR[0]} alone ran and reported success"
done

set +e
"$DTPIPE" -i "generate:5" --cursor GenerateIndex --state "$ARTIFACTS_DIR/full.sync" \
  -o "csv:$ARTIFACTS_DIR/cursor_full.csv" --no-stats > /dev/null 2>&1
EXIT=$?
set -e
[ "$EXIT" -eq 0 ] \
  && pass "the complete pair runs" \
  || fail "a complete --cursor/--state pair was refused (exit $EXIT)"

# ----------------------------------------
# [12] inspect says nothing about options it never expected
# ----------------------------------------
echo "--- [12] inspect emits no missing-options warning ---"
set +e
ERR=$("$DTPIPE" inspect -i "$ARTIFACTS_DIR/in_comma.csv" 2>&1 >/dev/null)
set -e
echo "$ERR" | grep -q "no options of type" \
  && fail "inspect still warns about options nothing was meant to bind: $ERR" \
  || pass "inspect emits no missing-options warning"

# ----------------------------------------
# Cleanup
# ----------------------------------------
rm -f "$ARTIFACTS_DIR"/*.csv "$ARTIFACTS_DIR"/*.yaml "$ARTIFACTS_DIR"/*.db "$ARTIFACTS_DIR"/*.json "$ARTIFACTS_DIR"/*.sync

echo ""
echo -e "${GREEN}Options validation complete!${NC}"
