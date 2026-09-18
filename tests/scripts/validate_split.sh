#!/bin/bash
set -e

# validate_split.sh
# `dtpipe split` offers the cut points of a job, against the real binary.
#
# The candidates are read off a SAMPLE RUN of the job — the real reader, the real transformers, the
# writer neutralised — not off the transformer list. The difference is decidable, and case 4 is
# what decides it: a column a --compute creates appears from ITS stage on and not before, and its
# name is nowhere in the job file except inside a JavaScript expression nobody parsed. A list built
# by walking the transformer configs cannot produce it. That is the same property `--dry-run` has,
# for the same reason (CLAUDE.md › "Sample mode — there is no second engine").
#
# Case 5 is the safety claim: `split` runs the pipeline, so it has to be shown writing nothing.
#
# Only file sources and embedded DuckDB, so this runs in CI like the rest.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
ARTIFACTS_DIR="$SCRIPT_DIR/artifacts/split"
rm -rf "$ARTIFACTS_DIR"; mkdir -p "$ARTIFACTS_DIR"

DTPIPE="$PROJECT_ROOT/dist/release/dtpipe"
export DTPIPE_NO_TUI=1

GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

pass() { echo -e "  ${GREEN}OK: $1${NC}"; }
fail() { echo -e "  ${RED}FAIL: $1${NC}"; exit 1; }

echo "========================================"
echo "      DtPipe Split Validation"
echo "========================================"

if [ ! -f "$DTPIPE" ]; then
    echo "Building release..."
    "$PROJECT_ROOT/build.sh" > /dev/null
fi

A="$ARTIFACTS_DIR"
cd "$A"
printf 'id,name,email,salary\n1,Ana,ana@corp.com,50000\n2,Bo,bo@corp.com,61000\n3,Cyd,cyd@corp.com,74000\n' > people.csv

echo ""
echo "--- Case 1: one candidate per stage, in the order they run ---"
"$DTPIPE" -i csv:people.csv \
          --compute "domain:row.email.split('@')[1]" \
          --fake "name:name.firstName" \
          -o csv:anon.csv --export-job two-steps.yaml > /dev/null 2>&1 \
    || fail "could not export the job to cut"
"$DTPIPE" split two-steps.yaml > two-steps.out 2>&1 || fail "split failed on a job that runs"

# Three stages: the reader, then each transformer.
for expected in '│ 0 │' '│ 1 │' '│ 2 │'; do
    grep -qF "$expected" two-steps.out || fail "no candidate numbered ${expected//[│ ]/}"
done
grep -qF '│ 3 │' two-steps.out && fail "a fourth candidate appeared for a two-transformer pipeline"
grep -q '0 .*the reader' two-steps.out || fail "candidate 0 is not the reader"
pass "three candidates for a reader and two transformers"

echo ""
echo "--- Case 2: they are named, and in pipeline order ---"
compute_line=$(grep -n 'Compute' two-steps.out | head -1 | cut -d: -f1)
fake_line=$(grep -n 'Fake' two-steps.out | head -1 | cut -d: -f1)
[ -n "$compute_line" ] || fail "the --compute stage is not named"
[ -n "$fake_line" ] || fail "the --fake stage is not named"
[ "$compute_line" -lt "$fake_line" ] || fail "the stages are not listed in pipeline order"
pass "each stage is named, --compute before --fake"

echo ""
echo "--- Case 3: the link cost is read off the run, not assumed ---"
# A CSV reader is row-mode, so the link bridges into Arrow and back out. A Parquet reader is
# already columnar, so the same cut costs nothing. Same cut index, same question, two answers —
# which is only possible because the mode comes from the execution.
grep -E '^\│ 0 \│.*(two bridges)' two-steps.out > /dev/null \
    || fail "a row-mode reader was not reported as costing two bridges"
"$DTPIPE" -i csv:people.csv -o parquet:people.parquet > /dev/null 2>&1 || fail "could not write the parquet fixture"
"$DTPIPE" -i parquet:people.parquet --fake "name:name.firstName" -o csv:p2.csv \
          --export-job columnar.yaml > /dev/null 2>&1 || fail "could not export the columnar job"
"$DTPIPE" split columnar.yaml > columnar.out 2>&1 || fail "split failed on the columnar job"
grep -E '^\│ 0 \│.*free' columnar.out > /dev/null \
    || fail "a columnar reader was not reported as a free link"
pass "row-mode costs two bridges, columnar costs nothing"

echo ""
echo "--- Case 4: a computed column appears from its own stage on ---"
# 'domain' exists nowhere in the job file but inside a JavaScript expression. A proposal derived
# from the transformer configs cannot name it, and cannot know which stage it starts at.
grep -E '^\│ 0 \│' two-steps.out | grep -q 'domain' \
    && fail "the reader's output already carries a column its transformer creates"
grep -E '^\│ 1 \│' two-steps.out | grep -q 'domain' \
    || fail "the computed column is missing from the stage that creates it"
grep -E '^\│ 2 \│' two-steps.out | grep -q 'domain' \
    || fail "the computed column vanished downstream"
pass "'domain' starts at the stage that computes it"

echo ""
echo "--- Case 5: proposing writes nothing ---"
rm -f neverwritten.csv
cat > write.yaml <<'EOF'
main:
  input: "csv:people.csv"
  output: "csv:neverwritten.csv"
EOF
"$DTPIPE" split write.yaml > /dev/null 2>&1 || fail "split failed on a file target"
[ -f neverwritten.csv ] && fail "split wrote its target"

rm -f target.duckdb
cat > write-db.yaml <<'EOF'
main:
  input: "csv:people.csv"
  output: "duck:target.duckdb"
  provider-options:
    duck-writer:
      table: "loaded"
EOF
"$DTPIPE" split write-db.yaml > /dev/null 2>&1 || fail "split failed on a database target"
if "$DTPIPE" -i duck:target.duckdb -q "SELECT count(*) FROM loaded" -o jsonl:- --no-stats > /dev/null 2>&1; then
    fail "split created the target table"
fi
pass "neither a file nor a table was written"

echo ""
echo "--- Case 6: it refuses, and says what the job has ---"
if "$DTPIPE" split no-such-file.yaml > missing.out 2>&1; then
    fail "split accepted a job file that does not exist"
fi
grep -qi "no job file" missing.out || fail "a missing job file is not named as one"

if "$DTPIPE" split two-steps.yaml --branch nope > branch.out 2>&1; then
    fail "split accepted a branch the job does not have"
fi
grep -qF "main" branch.out || fail "the refusal does not name the branches the job does have"
pass "a missing file and an unknown branch are both refused by name"

echo ""
echo "--- Case 7: a DAG with two readers asks which one ---"
"$DTPIPE" -i csv:people.csv --alias a \
          -i csv:people.csv --alias b \
          --from a,b --merge -o csv:merged.csv \
          --export-job dag.yaml > /dev/null 2>&1 || fail "could not export the DAG job"
if "$DTPIPE" split dag.yaml > dag.out 2>&1; then
    fail "split picked a branch on its own"
fi
grep -qF -- "--branch" dag.out || fail "the refusal does not say how to name the branch"
pass "it asks rather than choosing"

echo ""
echo -e "${GREEN}All split checks passed.${NC}"
