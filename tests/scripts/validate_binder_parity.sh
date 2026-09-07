#!/bin/bash
set -e

# validate_binder_parity.sh
# F8 — binder parity: five scenarios executed twice each (direct CLI vs exported YAML
# job run through OptionBinder.BindYaml), outputs diffed byte-for-byte.
# Scenario (e) exercises arity-driven value consumption on dash-leading/negative tokens.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
ARTIFACTS_DIR="$SCRIPT_DIR/artifacts/binder_parity"
mkdir -p "$ARTIFACTS_DIR"

DTPIPE="$PROJECT_ROOT/dist/release/dtpipe"
export DTPIPE_NO_TUI=1

GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

pass() { echo -e "  ${GREEN}OK: $1${NC}"; }
fail() { echo -e "  ${RED}FAIL: $1${NC}"; exit 1; }

echo "========================================"
echo "    DtPipe Binder Parity Validation"
echo "========================================"

if [ ! -f "$DTPIPE" ]; then
    echo "Building release..."
    "$PROJECT_ROOT/build.sh" > /dev/null
fi

A="$ARTIFACTS_DIR"

cleanup() { rm -rf "$A"; }
trap cleanup EXIT

# run_pair NAME OUTFILE ARGS...
# ARGS must contain -o OUTFILE (it is part of both runs and of the exported YAML).
# 1) export YAML   2) run CLI directly -> copy output   3) run YAML job -> copy output   4) diff
run_pair() {
    local name="$1"; local outfile="$2"; shift 2
    rm -f "$A/${name}.yaml" "$A/${name}_cli.csv" "$A/${name}_yaml.csv" "$outfile"

    "$DTPIPE" "$@" --export-job "$A/${name}.yaml" > /dev/null 2>&1 || fail "[$name] export failed"
    [ -s "$A/${name}.yaml" ] || fail "[$name] exported YAML is empty"

    "$DTPIPE" "$@" > /dev/null 2>&1 || fail "[$name] direct CLI run failed"
    [ -s "$outfile" ] || fail "[$name] direct CLI run produced no output"
    cp "$outfile" "$A/${name}_cli.csv"

    rm -f "$outfile"
    "$DTPIPE" --job "$A/${name}.yaml" > /dev/null 2>&1 || fail "[$name] YAML job run failed"
    [ -s "$outfile" ] || fail "[$name] YAML job run produced no output"
    cp "$outfile" "$A/${name}_yaml.csv"

    if diff -q "$A/${name}_cli.csv" "$A/${name}_yaml.csv" > /dev/null; then
        pass "[$name] CLI ≡ YAML ($(tail -n +2 "$A/${name}_cli.csv" | wc -l | tr -d ' ') rows)"
    else
        fail "[$name] CLI and YAML outputs differ"
    fi
}

cat > "$A/src.csv" <<EOF
Id,Val
1,a
2,b
3,c
4,d
5,e
EOF

# ----------------------------------------
echo "--- [a] seeded fake + filter ---"
run_pair fake_filter \
    "$A/a_out.csv" \
    -i generate:20 \
    --fake "Name:name.firstName" --fake-seed 42 \
    --filter "row.GenerateIndex != null" \
    -o "$A/a_out.csv" --no-stats

# ----------------------------------------
echo "--- [b] reader vs writer --csv-separator scoping ---"
cp "$A/src.csv" "$A/in_semicolon.csv"
sed -i '' 's/,/;/g' "$A/in_semicolon.csv" 2>/dev/null || sed -i 's/,/;/g' "$A/in_semicolon.csv"
run_pair csv_scoping \
    "$A/b_out.csv" \
    -i "csv:$A/in_semicolon.csv" --csv-separator ";" \
    --project "Id,Val" \
    -o "$A/b_out.csv" --csv-separator "|" --no-stats

# ----------------------------------------
echo "--- [c] --duck-init + --sql ---"
run_pair duck_init_sql \
    "$A/c_out.csv" \
    -i "csv:$A/src.csv" --column-types "Id:int32" --duck-init "SELECT 1;" --alias s \
    --from s --sql "SELECT Id, Val FROM s WHERE Id >= 2" \
    -o "$A/c_out.csv" --no-stats

# ----------------------------------------
echo "--- [d] --strategy Upsert --key id (sqlite round-trip) ---"
dump_table() { # dbfile outfile
    rm -f "$2"
    "$DTPIPE" -i "sqlite:$1" --query "SELECT Id, Val FROM upsert_t ORDER BY Id" -o "$2" --no-stats > /dev/null 2>&1
}

# CLI reference run: pre-create the table WITH a primary key (required by
# ON CONFLICT upsert), then run Upsert twice so key conflicts are exercised.
rm -f "$A/d_target_cli.db"
sqlite3 "$A/d_target_cli.db" "CREATE TABLE upsert_t (Id INTEGER PRIMARY KEY, Val TEXT);"
"$DTPIPE" -i "csv:$A/src.csv" -o "sqlite:$A/d_target_cli.db" --table upsert_t --strategy Upsert --key Id --no-stats > /dev/null 2>&1 \
    || fail "[upsert_key] CLI upsert pass 1 failed"
"$DTPIPE" -i "csv:$A/src.csv" -o "sqlite:$A/d_target_cli.db" --table upsert_t --strategy Upsert --key Id --no-stats > /dev/null 2>&1 \
    || fail "[upsert_key] CLI upsert pass 2 failed"

# YAML run: identical steps through exported jobs.
"$DTPIPE" -i "csv:$A/src.csv" -o "sqlite:$A/d_target_yaml.db" --table upsert_t --strategy Upsert --key Id --export-job "$A/d.yaml" > /dev/null 2>&1 \
    || fail "[upsert_key] YAML export failed"
rm -f "$A/d_target_yaml.db"
sqlite3 "$A/d_target_yaml.db" "CREATE TABLE upsert_t (Id INTEGER PRIMARY KEY, Val TEXT);"
"$DTPIPE" --job "$A/d.yaml" > /dev/null 2>&1 || fail "[upsert_key] YAML upsert pass 1 failed"
"$DTPIPE" --job "$A/d.yaml" > /dev/null 2>&1 || fail "[upsert_key] YAML upsert pass 2 failed"

dump_table "$A/d_target_cli.db" "$A/d_cli.csv"
dump_table "$A/d_target_yaml.db" "$A/d_yaml.csv"
if diff -q "$A/d_cli.csv" "$A/d_yaml.csv" > /dev/null; then
    pass "[upsert_key] CLI ≡ YAML ($(tail -n +2 "$A/d_cli.csv" | wc -l | tr -d ' ') rows)"
else
    fail "[upsert_key] table contents differ between runs"
fi

# ----------------------------------------
echo "--- [e] scalar-edge values: negative seed, dash-leading separator ---"
run_pair edge_values \
    "$A/e_out.csv" \
    -i generate:10 \
    --sampling-seed -42 --sampling-rate 0.5 \
    -o "$A/e_out.csv" --csv-separator "-" --no-stats

# ----------------------------------------
echo "--- [f] reader-side --table auto-builds SELECT ---"
# Regression guard for the F13 capability refactor: QueryableReaderOptions must keep
# honoring --table (auto-build "SELECT * FROM <table>" when no --query is given),
# both on the CLI path and through the exported-YAML round-trip.
rm -f "$A/f_src.db"
sqlite3 "$A/f_src.db" "CREATE TABLE src_t (Id INTEGER, Val TEXT); INSERT INTO src_t VALUES (1,'a'),(2,'b'),(3,'c');"
run_pair reader_table \
    "$A/f_out.csv" \
    -i "sqlite:$A/f_src.db" --table src_t \
    -o "$A/f_out.csv" --no-stats

# ----------------------------------------
echo "--- [g] transformer options block: --compute-types ---"
# A transformer's YAML "options:" keys were read by a hand-written list in each factory, so
# --compute-types bound on the CLI and silently bound nothing in YAML: the exported job produced
# a String column where the direct run produced a Double.
run_pair compute_types \
    "$A/g_out.csv" \
    -i "csv:$A/src.csv" \
    --compute "Doubled:parseInt(row.Id) * 2" --compute-types "Doubled:double" \
    -o "$A/g_out.csv" --no-stats

echo ""
echo "All binder parity checks passed."
