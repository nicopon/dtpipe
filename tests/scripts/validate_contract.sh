#!/bin/bash
set -e

# validate_contract.sh
# --contract-save, against the real binary.
#
# The property that matters is the one a CI depends on: the contract a DRY RUN captures is the
# contract a REAL RUN produces. A producer's pipeline gate is `--dry-run N --contract-save`, so if
# the two ever differ the published promise is not the one the nightly run keeps — and nothing on
# either side says so.
#
# Only file sources, so this runs in CI like the rest.
#
# `contract check` is a sample run over the contract's schema and no rows, so its exit code is the
# whole product: a check that cannot go red is a check nobody should trust. Every refusal below is
# therefore driven, not assumed.
#
# DuckDB is embedded, so the target side needs no container either.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
ARTIFACTS_DIR="$SCRIPT_DIR/artifacts/contract"
rm -rf "$ARTIFACTS_DIR"; mkdir -p "$ARTIFACTS_DIR"

DTPIPE="$PROJECT_ROOT/dist/release/dtpipe"
export DTPIPE_NO_TUI=1

GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

pass() { echo -e "  ${GREEN}OK: $1${NC}"; }
fail() { echo -e "  ${RED}FAIL: $1${NC}"; exit 1; }

echo "========================================"
echo "    DtPipe Contract Validation"
echo "========================================"

if [ ! -f "$DTPIPE" ]; then
    echo "Building release..."
    "$PROJECT_ROOT/build.sh" > /dev/null
fi

A="$ARTIFACTS_DIR"
printf 'id,name\n1,ana\n2,bo\n3,cyd\n' > "$A/in.csv"

hash_of() { python3 -c "import json,sys;print(json.load(open(sys.argv[1]))['hash'])" "$1"; }
field_of() { python3 -c "import json,sys;print(','.join(f['name'] for f in json.load(open(sys.argv[1]))['schema']['fields']))" "$1"; }
key_of()   { python3 -c "import json,sys;print(json.load(open(sys.argv[1])).get(sys.argv[2],''))" "$1" "$2"; }

echo ""
echo "--- Case 1: a dry run captures what a real run produces ---"
"$DTPIPE" -i csv:"$A/in.csv" -o csv:"$A/real.csv" --contract-save "$A/real.json" > "$A/real.log" 2>&1 \
    || fail "the real run failed"
"$DTPIPE" -i csv:"$A/in.csv" -o csv:"$A/never.csv" --dry-run 2 --contract-save "$A/dry.json" > "$A/dry.log" 2>&1 \
    || fail "the dry run failed"

[ -f "$A/real.json" ] || fail "the real run wrote no contract"
[ -f "$A/dry.json" ] || fail "the dry run wrote no contract"
[ "$(hash_of "$A/real.json")" = "$(hash_of "$A/dry.json")" ] \
    || fail "dry run and real run disagree — the CI gesture publishes a promise the run does not keep"
pass "same hash from both"

[ -f "$A/never.csv" ] && fail "the dry run created its target"
pass "the dry run wrote nothing to the target"

echo ""
echo "--- Case 2: the contract is the pipeline's output, not its source ---"
"$DTPIPE" -i csv:"$A/in.csv" --compute "initial:row.name.substring(0,1)" --compute-types "initial:string" \
          -o csv:"$A/computed.csv" --contract-save "$A/computed.json" > "$A/computed.log" 2>&1 \
    || fail "the computed run failed"

[ "$(field_of "$A/computed.json")" = "id,name,initial" ] \
    || fail "the contract does not carry the computed column: $(field_of "$A/computed.json")"
pass "a computed column is in the contract"

[ "$(hash_of "$A/computed.json")" != "$(hash_of "$A/real.json")" ] \
    || fail "two different output schemas hash the same"
pass "a schema change changes the hash"

echo ""
echo "--- Case 3: the hash is stable across runs ---"
"$DTPIPE" -i csv:"$A/in.csv" -o csv:"$A/again.csv" --contract-save "$A/again.json" > "$A/again.log" 2>&1 \
    || fail "the second run failed"
[ "$(hash_of "$A/again.json")" = "$(hash_of "$A/real.json")" ] \
    || fail "the same pipeline hashed differently twice — the contract is not canonical"
pass "an unchanged pipeline keeps its hash"

echo ""
echo "--- Case 4: what the run could guarantee travels with the contract ---"
[ -n "$(key_of "$A/dry.json" enforcement)" ] \
    || fail "a sample run's contract carries no enforcement level"
pass "a sample run records its read-only guarantee ($(key_of "$A/dry.json" enforcement))"

[ "$(key_of "$A/real.json" enforcement)" = "" ] \
    || fail "a real run claims a guarantee it never asked for"
pass "a real run claims none, rather than the weakest"

echo ""
echo "--- Case 5: two branches may not claim one contract file ---"
set +e
"$DTPIPE" -i csv:"$A/in.csv" --alias a --contract-save "$A/shared.json" -o csv:"$A/a.csv" \
          -i csv:"$A/in.csv" --alias b --contract-save "$A/shared.json" -o csv:"$A/b.csv" \
          > "$A/shared.log" 2>&1
rc=$?
set -e
[ "$rc" -ne 0 ] || fail "two branches sharing a contract path were accepted"
grep -q "claimed by both branch" "$A/shared.log" || fail "the refusal does not name the collision"
[ -f "$A/shared.json" ] && fail "a refused pipeline still wrote a contract"
pass "refused, named, and nothing written"

echo ""
echo "--- Case 6: the flag survives --export-job ---"
"$DTPIPE" -i csv:"$A/in.csv" -o csv:"$A/rt.csv" --contract-save "$A/rt.json" \
          --export-job "$A/job.yaml" > "$A/export.log" 2>&1 || fail "--export-job failed"
grep -q 'contract-save:' "$A/job.yaml" || fail "the exported job lost contract-save"

"$DTPIPE" --job "$A/job.yaml" > "$A/replay.log" 2>&1 || fail "replaying the exported job failed"
[ "$(hash_of "$A/rt.json")" = "$(hash_of "$A/real.json")" ] \
    || fail "the replayed job produced a different contract"
pass "exported, replayed, same contract"

echo ""
echo "--- Case 7: a consumer that accepts the contract ---"
cat > "$A/consumer.yaml" <<YAML
main:
  input: csv:$A/in.csv
  output: duck:$A/consumer.duckdb
  provider-options:
    duck-writer:
      table: landing
YAML
"$DTPIPE" contract check --job "$A/consumer.yaml" --contract "$A/real.json" > "$A/check_ok.log" 2>&1 \
    || fail "a compatible consumer was refused"
grep -q "accepts this contract" "$A/check_ok.log" || fail "the green verdict is not stated"
pass "a compatible consumer exits 0"

# The caveat is printed on the GREEN verdict too. That is where it will be misread, and a
# guarantee that is sometimes absent must never read as though it were always there.
grep -q "says nothing about rules the target" "$A/check_ok.log" \
    || fail "the green verdict does not state what it did not check"
pass "the green verdict says what it does not cover"

echo ""
echo "--- Case 8: a target column the contract cannot fill ---"
printf 'id,name,total\n1,ana,10\n' > "$A/seed.csv"
"$DTPIPE" -i csv:"$A/seed.csv" -o duck:"$A/strict.duckdb" --table landing --no-stats > /dev/null 2>&1
"$DTPIPE" -i duck:"$A/strict.duckdb" --duck-init "ALTER TABLE landing ALTER COLUMN total SET NOT NULL;" \
          --query "SELECT 1 AS x" -o null: --no-stats > /dev/null 2>&1

cat > "$A/strict.yaml" <<YAML
main:
  input: csv:$A/in.csv
  output: duck:$A/strict.duckdb
  provider-options:
    duck-writer:
      table: landing
YAML
set +e
"$DTPIPE" contract check --job "$A/strict.yaml" --contract "$A/real.json" > "$A/check_bad.log" 2>&1
rc=$?
set -e
[ "$rc" -eq 1 ] || fail "a NOT NULL column the contract cannot fill was accepted (exit $rc)"
grep -q "NOT NULL" "$A/check_bad.log" || fail "the refusal does not name the constraint"
pass "an unfillable NOT NULL column is refused, and named"

echo ""
echo "--- Case 9: a consumer that cannot even initialise ---"
cat > "$A/broken.yaml" <<YAML
main:
  input: csv:$A/in.csv
  transformers:
  - type: project
    options:
      project: id,does_not_exist
  output: duck:$A/consumer.duckdb
  provider-options:
    duck-writer:
      table: landing
YAML
set +e
"$DTPIPE" contract check --job "$A/broken.yaml" --contract "$A/real.json" > "$A/check_init.log" 2>&1
rc=$?
set -e
[ "$rc" -eq 1 ] || fail "a consumer that cannot initialise was accepted (exit $rc)"
grep -q "does_not_exist" "$A/check_init.log" || fail "the refusal does not name the column"
grep -qE '^ +at ' "$A/check_init.log" && fail "the refusal is a stack trace"
pass "a consumer that cannot initialise is refused, by name and without a stack trace"

echo ""
echo "--- Case 10: a malformed job file is a message, not a stack trace ---"
printf 'main:\n  input: [unclosed\n' > "$A/malformed.yaml"
set +e
"$DTPIPE" contract check --job "$A/malformed.yaml" --contract "$A/real.json" > "$A/check_yaml.log" 2>&1
rc=$?
set -e
[ "$rc" -eq 1 ] || fail "a malformed job file was accepted"
grep -qE '^ +at YamlDotNet' "$A/check_yaml.log" && fail "YamlDotNet's stack trace reached the user"
pass "a malformed job file is reported, not dumped"

echo ""
echo "--- Case 11: contract show reads back what was written ---"
"$DTPIPE" contract show "$A/computed.json" > "$A/show.log" 2>&1 || fail "contract show failed"
grep -q "initial" "$A/show.log" || fail "contract show does not list the columns"
pass "contract show lists the schema"

echo ""
echo "--- Case 12: diff says nothing when nothing changed ---"
"$DTPIPE" contract diff "$A/real.json" "$A/again.json" > "$A/diff_same.log" 2>&1 \
    || fail "two identical contracts were reported as a break"
pass "identical contracts compare clean"

echo ""
echo "--- Case 13: a dropped column breaks consumers ---"
printf 'id\n1\n2\n3\n' > "$A/narrowed.csv"
"$DTPIPE" -i csv:"$A/narrowed.csv" -o csv:"$A/narrowed_out.csv" \
          --contract-save "$A/narrowed.json" > /dev/null 2>&1 || fail "the narrowed run failed"

set +e
"$DTPIPE" contract diff "$A/real.json" "$A/narrowed.json" > "$A/diff_break.log" 2>&1
rc=$?
set -e
[ "$rc" -eq 1 ] || fail "dropping a column exited $rc, expected 1"
grep -q "Removed" "$A/diff_break.log" || fail "the diff does not name the removal"
grep -q "name" "$A/diff_break.log" || fail "the diff does not name the column"
pass "a dropped column exits 1 and names the column"

echo ""
echo "--- Case 14: an added column does not ---"
set +e
"$DTPIPE" contract diff "$A/real.json" "$A/computed.json" > "$A/diff_add.log" 2>&1
rc=$?
set -e
[ "$rc" -eq 0 ] || fail "adding a column exited $rc, expected 0"
grep -q "Added" "$A/diff_add.log" || fail "the diff does not report the addition"
pass "an added column exits 0"

echo ""
echo "--- Case 15: --json carries the same verdict ---"
set +e
"$DTPIPE" contract diff --json "$A/real.json" "$A/narrowed.json" > "$A/diff.json" 2>"$A/diff_json.err"
rc=$?
set -e
[ "$rc" -eq 1 ] || fail "--json disagreed with the exit code of the plain form"
python3 -c "
import json,sys
d = json.load(open(sys.argv[1]))
assert d['compatible'] is False, 'compatible should be false'
assert any(c['breaking'] and c['kind'] == 'Removed' for c in d['changes']), d
" "$A/diff.json" || fail "--json does not carry the breaking change"
pass "--json and the exit code agree"

echo ""
echo "--- Case 16: the check and the diff agree on the same break ---"
# The narrowed producer no longer supplies 'name'. A consumer whose target has it NOT NULL must
# be refused by check, exactly as diff refuses the contract change. Two commands, one break.
set +e
"$DTPIPE" contract check --job "$A/strict.yaml" --contract "$A/narrowed.json" > "$A/check_narrow.log" 2>&1
rc=$?
set -e
[ "$rc" -eq 1 ] || fail "check accepted a contract diff calls breaking"
pass "check and diff do not disagree"

echo ""
echo -e "${GREEN}Contract validation passed.${NC}"
