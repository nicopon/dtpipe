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
# Lots B and C (`contract check`, `contract diff`) are not built yet; when they are, the cases that
# belong here are a consumer checking green, then a column dropped from the producer's query and
# both commands exiting 1.

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
echo -e "${GREEN}Contract validation passed.${NC}"
