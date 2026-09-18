#!/bin/bash
set -e

# validate_export_job.sh
# F3 — --export-job round-trip invariant.
# For three pipelines (fake+filter linear, SQL DAG, incremental cursor):
#   1. export the CLI pipeline to YAML (--export-job)
#   2. run the CLI pipeline directly (--metrics-path cli.json)
#   3. run the exported YAML (--metrics-path yaml.json)
#   4. assert identical ReadCount/WriteCount (wall-clock/memory fields ignored)

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
ARTIFACTS_DIR="$SCRIPT_DIR/artifacts/export_job"
mkdir -p "$ARTIFACTS_DIR"

DTPIPE="$PROJECT_ROOT/dist/release/dtpipe"
export DTPIPE_NO_TUI=1

GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

pass() { echo -e "  ${GREEN}OK: $1${NC}"; }
fail() { echo -e "  ${RED}FAIL: $1${NC}"; exit 1; }

echo "========================================"
echo "    DtPipe Export-Job Round-Trip"
echo "========================================"

if [ ! -f "$DTPIPE" ]; then
    echo "Building release..."
    "$PROJECT_ROOT/build.sh" > /dev/null
fi

A="$ARTIFACTS_DIR"

cleanup() {
    rm -rf "$A"
    # Case 6 stores one alias in the fake keyring; a second `trap … EXIT` would
    # replace this one rather than add to it, so the removal lives here.
    [ -n "${REFERENCE_ALIAS:-}" ] && "$DTPIPE" secret delete "$REFERENCE_ALIAS" > /dev/null 2>&1
    return 0
}
trap cleanup EXIT

metric_value() { # file, key
    grep "\"$2\"" "$1" | head -1 | sed -E 's/[^0-9]*([0-9]+).*/\1/'
}

assert_same_metrics() { # name, cli.json, yaml.json
    local name="$1"
    for key in ReadCount WriteCount; do
        local cli_val yaml_val
        cli_val=$(metric_value "$2" "$key")
        yaml_val=$(metric_value "$3" "$key")
        if [ -z "$cli_val" ] || [ -z "$yaml_val" ]; then
            fail "[$name] metric '$key' missing (cli='$cli_val' yaml='$yaml_val')"
        fi
        if [ "$cli_val" != "$yaml_val" ]; then
            fail "[$name] $key differs: cli=$cli_val yaml=$yaml_val"
        fi
    done
    pass "[$name] metrics identical (read=$(metric_value "$2" ReadCount), write=$(metric_value "$2" WriteCount))"
}

# Run a pipeline twice (direct CLI vs exported YAML) and compare results.
# Comparison modes:
#   metrics — compare --metrics-path JSON ReadCount/WriteCount (single-branch pipelines;
#             in a DAG every branch writes the same metrics file, last writer wins)
#   output  — byte-compare the -o output of both runs (deterministic pipelines)
run_pair() { # name, mode, args...
    local name="$1"; local mode="$2"; shift 2
    rm -f "$A/${name}_cli.json" "$A/${name}_yaml.json"
    # 0. Snapshot previous outputs so both runs are compared independently.
    rm -f "$A/${name}_out_cli.csv" "$A/${name}_out_yaml.csv"
    # 1. Export to YAML
    "$DTPIPE" "$@" --export-job "$A/${name}.yaml" 2>/dev/null
    [ -s "$A/${name}.yaml" ] || fail "[$name] exported YAML is empty"
    # 2. Run CLI directly
    "$DTPIPE" "$@" --metrics-path "$A/${name}_cli.json" > /dev/null 2>&1 \
        || fail "[$name] direct CLI run failed"
    if [ -n "${OUTPUT_FILE:-}" ]; then cp "$OUTPUT_FILE" "$A/${name}_out_cli.csv"; fi
    # 3. Run via YAML job
    "$DTPIPE" --job "$A/${name}.yaml" --metrics-path "$A/${name}_yaml.json" > /dev/null 2>&1 \
        || fail "[$name] YAML job run failed"
    if [ -n "${OUTPUT_FILE:-}" ]; then cp "$OUTPUT_FILE" "$A/${name}_out_yaml.csv"; fi
    # 4. Compare
    if [ "$mode" = "metrics" ]; then
        assert_same_metrics "$name" "$A/${name}_cli.json" "$A/${name}_yaml.json"
    else
        if diff -q "$A/${name}_out_cli.csv" "$A/${name}_out_yaml.csv" > /dev/null; then
            pass "[$name] outputs byte-identical ($(wc -l < "$A/${name}_out_cli.csv" | tr -d ' ') lines)"
        else
            fail "[$name] outputs differ between CLI and YAML runs"
        fi
    fi
}

# ----------------------------------------
echo "--- [1] Linear pipeline with transformers ---"
run_pair linear_fake_filter metrics \
    -i generate:50 \
    --fake "Name:name.firstName" \
    --filter "row.GenerateIndex != null" \
    -o "$A/lin_out.csv" --no-stats

# ----------------------------------------
echo "--- [2] DAG with SQL processor ---"
cat > "$A/src.csv" <<EOF
Id,Val
1,a
2,b
3,c
4,d
5,e
EOF
OUTPUT_FILE="$A/sql_out.csv" run_pair dag_sql output \
    -i "csv:$A/src.csv" --column-types "Id:int32" --alias s \
    --from s --sql "SELECT Id, Val FROM s WHERE Id >= 2" \
    -o "$A/sql_out.csv" --no-stats

# ----------------------------------------
echo "--- [3] Incremental cursor ---"
cat > "$A/events.csv" <<EOF
Id,Kind
1,x
2,y
3,z
EOF
rm -f "$A/state.json"
OUTPUT_FILE="$A/cursor_out.csv" run_pair incremental_cursor output \
    -i "csv:$A/events.csv" --cursor Id --state "$A/state.json" \
    -o "$A/cursor_out.csv" --no-stats

# ----------------------------------------------------------------------------
# What enters as a reference leaves as a reference.
#
# The round-trip cases above compare what a job DOES. These compare what the file SAYS, because a
# ${{…}} token exists to keep a value out of it — and an exported job is bound for a repository,
# which is the point of exporting one.
#
# The two entry points disagreed here, and the asymmetry is what hid it. From a command line
# nothing interpolates before the writer, so the reference survived and the behaviour read as
# correct. Through --job every string scalar was resolved as it was read, so an env var and a
# keyring alias that RESOLVED were written out in clear. An alias that did NOT resolve survived,
# which looks exactly like the guarantee — hence case 9, which is here to say it proves nothing.
# ----------------------------------------------------------------------------
echo ""
echo "--- [4] a reference survives the export ---"

SECRET='S3cr3t!Pa55'
ALIAS_SECRET='An0th3rSecret'
export DTPIPE_TEST_EXPORT_PWD="$SECRET"
export DTPIPE_UNSAFE_INSECURE_FAKE_KEYRING=1

# The fake keyring's path comes from the platform's application-data folder, which does NOT follow
# $HOME on macOS — setting it reads as isolation and gives none. So this owns one alias and removes
# it in cleanup, rather than owning the file.
REFERENCE_ALIAS=validate-export-job
"$DTPIPE" secret set "$REFERENCE_ALIAS" "$ALIAS_SECRET" > /dev/null 2>&1 \
    || fail "could not store the alias the keyring case needs"

printf 'a,b\n1,2\n' > "$A/ref-in.csv"

cat > "$A/ref-env.yaml" <<'YAML'
main:
  input: "csv:ref-in.csv"
  output: "mssql:Server=srvB;Database=B;User Id=u;Password=${{DTPIPE_TEST_EXPORT_PWD}}"
YAML
"$DTPIPE" --job "$A/ref-env.yaml" --export-job "$A/ref-env-out.yaml" > /dev/null 2>&1 \
    || fail "[reference] the env export failed"
grep -qF "$SECRET" "$A/ref-env-out.yaml" && fail "[reference] the env var was resolved into the exported job"
grep -qF '${{DTPIPE_TEST_EXPORT_PWD}}' "$A/ref-env-out.yaml" \
    || fail "[reference] the env reference is neither resolved nor preserved — it is gone"
pass "[reference] a job file's env reference survived the export"

cat > "$A/ref-keyring.yaml" <<'YAML'
main:
  input: "csv:ref-in.csv"
  output: "mssql:Server=srvB;Password=${{keyring://validate-export-job}}"
YAML
"$DTPIPE" --job "$A/ref-keyring.yaml" --export-job "$A/ref-keyring-out.yaml" > /dev/null 2>&1 \
    || fail "[reference] the keyring export failed"
grep -qF "$ALIAS_SECRET" "$A/ref-keyring-out.yaml" \
    && fail "[reference] the keyring alias was resolved into the exported job"
grep -qF '${{keyring://validate-export-job}}' "$A/ref-keyring-out.yaml" \
    || fail "[reference] the keyring reference is neither resolved nor preserved — it is gone"
pass "[reference] a RESOLVABLE keyring alias survived the export"

"$DTPIPE" -i csv:"$A/ref-in.csv" \
          -o 'mssql:Server=srvB;Password=${{DTPIPE_TEST_EXPORT_PWD}}' \
          --export-job "$A/ref-cli-out.yaml" > /dev/null 2>&1 \
    || fail "[reference] the command-line export failed"
grep -qF "$SECRET" "$A/ref-cli-out.yaml" && fail "[reference] the command-line path now resolves too"
pass "[reference] the command-line path is unchanged"

cat > "$A/ref-absent.yaml" <<'YAML'
main:
  input: "csv:ref-in.csv"
  output: "mssql:Server=srvB;Password=${{keyring://no-such-alias}}"
YAML
"$DTPIPE" --job "$A/ref-absent.yaml" --export-job "$A/ref-absent-out.yaml" > /dev/null 2>&1 \
    || fail "[reference] the absent-alias export failed"
grep -qF '${{keyring://no-such-alias}}' "$A/ref-absent-out.yaml" \
    || fail "[reference] an alias with nothing behind it did not survive either"
pass "[reference] an unresolvable alias survives — as it did while the two above were leaking"

cat > "$A/ref-run.yaml" <<'YAML'
main:
  input: "csv:${{DTPIPE_TEST_EXPORT_SRC}}"
  output: "csv:ref-run-out.csv"
YAML
( cd "$A" && DTPIPE_TEST_EXPORT_SRC=ref-in.csv "$DTPIPE" --job ref-run.yaml > /dev/null 2>&1 ) \
    || fail "[reference] the run failed — the export rule leaked into execution"
grep -q '^1,2' "$A/ref-run-out.csv" 2>/dev/null \
    || fail "[reference] the run did not resolve the reference it was given"
pass "[reference] execution still resolves, as it must"

echo ""
echo "All export-job round-trip checks passed."
