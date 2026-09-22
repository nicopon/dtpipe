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
# Cases 8 onwards cut. Case 12 is the one that matters: the two halves REPLAYED must produce what
# the monolithic run produced, byte for byte. It is the only check here that exercises the link
# rather than describing it, and it is what caught the `arrow:` reader handing downstream batches
# that do not obey the ownership contract — --mask over the link read freed memory. Its pipeline is
# deliberately deterministic (--compute + --mask, no unseeded --fake) so the comparison can be an
# exact one, and deliberately built on an ALIASING transformer so a regression of that defect fails
# here and not only in the unit suite.
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
echo "--- Case 8: --at and --out go together ---"
if "$DTPIPE" split two-steps.yaml --at 1 > lonely.out 2>&1; then
    fail "--at was accepted without --out"
fi
grep -qF -- "--out" lonely.out || fail "the refusal does not say what is missing"
if "$DTPIPE" split two-steps.yaml --out half > lonely2.out 2>&1; then
    fail "--out was accepted without --at"
fi
grep -qF -- "--at" lonely2.out || fail "the refusal does not say what is missing"
pass "neither half of the pair is accepted alone"

echo ""
echo "--- Case 9: a stage that does not exist is refused, by range ---"
if "$DTPIPE" split two-steps.yaml --at 9 --out bad > range.out 2>&1; then
    fail "a cut after a stage the branch does not have was accepted"
fi
grep -qE "0 to 2" range.out || fail "the refusal does not name the range the branch has"
[ -f bad-producer.yaml ] && fail "a refused cut still wrote a file"
pass "out of range is refused, and names 0 to 2"

echo ""
echo "--- Case 10: without --acknowledge nothing is written ---"
rm -f gate-producer.yaml gate-consumer.yaml
if "$DTPIPE" split two-steps.yaml --at 1 --out gate > gate.out 2>&1; then
    fail "the cut wrote its files without being acknowledged"
fi
[ -f gate-producer.yaml ] && fail "the producer half was written anyway"
[ -f gate-consumer.yaml ] && fail "the consumer half was written anyway"
grep -qF -- "--acknowledge" gate.out || fail "the gate does not say how to pass it"
# The gate has to state what it is NOT claiming, or it reads as a clean bill of health.
grep -qi "redistribut" gate.out || fail "the gate does not say a cut redistributes a secret"
pass "the two files are withheld, and the gate says what it does not prove"

echo ""
echo "--- Case 11: acknowledged, both halves are written and meet on the link ---"
"$DTPIPE" split two-steps.yaml --at 1 --out half --acknowledge > cut.out 2>&1 \
    || fail "an acknowledged cut failed"
[ -f half-producer.yaml ] || fail "the producer half was not written"
[ -f half-consumer.yaml ] || fail "the consumer half was not written"
grep -qF 'output: arrow:-' half-producer.yaml || fail "the producer does not write to the link"
grep -qF 'input: arrow:-' half-consumer.yaml || fail "the consumer does not read the link"
# Cut 1 is after the first transformer: compute upstream, fake downstream.
grep -qF 'type: compute' half-producer.yaml || fail "the upstream transformer is not on the producer"
grep -qF 'type: fake' half-producer.yaml && fail "a downstream transformer stayed on the producer"
grep -qF 'type: fake' half-consumer.yaml || fail "the downstream transformer is not on the consumer"
grep -qF 'type: compute' half-consumer.yaml && fail "an upstream transformer crossed to the consumer"
grep -qF 'csv:people.csv' half-producer.yaml || fail "the producer lost the source"
grep -qF 'csv:anon.csv' half-consumer.yaml || fail "the consumer lost the target"
pass "each half carries its own end, its own stages, and the link"

echo ""
echo "--- Case 12: the halves replayed produce what the monolithic run produced ---"
"$DTPIPE" -i csv:people.csv \
          --compute "domain:row.email.split('@')[1]" \
          --mask email \
          -o csv:whole.csv --export-job whole.yaml > /dev/null 2>&1 \
    || fail "could not export the deterministic job"
"$DTPIPE" --job whole.yaml --no-stats > /dev/null 2>&1 || fail "the monolithic job did not run"
[ -f whole.csv ] || fail "the monolithic job wrote nothing"

# Every cut point, not just a convenient one: the link has to carry the stream wherever it is put.
for at in 0 1 2; do
    rm -f whole.csv.rt "rt$at-producer.yaml" "rt$at-consumer.yaml"
    "$DTPIPE" split whole.yaml --at "$at" --out "rt$at" --acknowledge > /dev/null 2>&1 \
        || fail "the cut at $at failed"
    # Retarget the consumer so the two runs do not write the same file.
    sed "s|csv:whole.csv|csv:replayed$at.csv|" "rt$at-consumer.yaml" > "rt$at-consumer-out.yaml"
    rm -f "replayed$at.csv"
    if ! "$DTPIPE" --job "rt$at-producer.yaml" --no-stats 2>/dev/null \
       | "$DTPIPE" --job "rt$at-consumer-out.yaml" --no-stats > /dev/null 2>&1; then
        fail "the two halves cut at $at did not run through the link"
    fi
    [ -f "replayed$at.csv" ] || fail "the consumer cut at $at wrote nothing"
    diff whole.csv "replayed$at.csv" > /dev/null \
        || fail "cut at $at: the halves produced something other than the monolithic run"
done
pass "cut at 0, 1 and 2 each replay to the monolithic result"

echo ""
echo "--- Case 13: a credential written in full stops the cut ---"
cat > literal.yaml <<'EOF'
main:
  input: "csv:people.csv"
  output: "pg:Host=db;Username=u;Password=hunter2"
EOF
if "$DTPIPE" split literal.yaml --at 0 --out leak --acknowledge > literal.out 2>&1; then
    fail "a job spelling a credential out was cut anyway"
fi
[ -f leak-producer.yaml ] && fail "a refused cut still wrote a half"
grep -qF "main.output" literal.out || fail "the refusal does not name where the credential is"
grep -qF '${{ENV_VAR}}' literal.out || fail "the refusal does not say how to parameterise it"
pass "the literal is named, and blanking it is not offered"

echo ""
echo "--- Case 14: a reference is not a credential, and survives the cut ---"
# The job that RAN had its ${{…}} resolved, as it must to connect. Writing that copy would turn a
# reference its author placed to keep a value out of a file into the value itself, in two files.
export SPLIT_TEST_DELIM=","
cat > ref.yaml <<'EOF'
main:
  input: "csv:people.csv"
  output: "csv:refout.csv"
  provider-options:
    csv-reader:
      delimiter: "${{SPLIT_TEST_DELIM}}"
EOF
"$DTPIPE" split ref.yaml --at 0 --out ref --acknowledge > ref.out 2>&1 \
    || fail "a job using a reference was refused"
grep -qF '${{SPLIT_TEST_DELIM}}' ref-producer.yaml \
    || fail "the cut resolved the reference instead of carrying it"
grep -qF 'csv-reader' ref-producer.yaml || fail "the reader's options did not follow the producer"
grep -qF 'csv-reader' ref-consumer.yaml && fail "the reader's options crossed to the consumer"
pass "the reference is carried verbatim, on the half that owns it"

echo ""
echo "--- Case 15: a block neither end owns is not guessed at ---"
cat > orphan.yaml <<'EOF'
main:
  input: "csv:people.csv"
  output: "csv:orphanout.csv"
  provider-options:
    duck:
      init-sql: "ATTACH 'other.duckdb'"
EOF
if "$DTPIPE" split orphan.yaml --at 0 --out orphan --acknowledge > orphan.out 2>&1; then
    fail "a block belonging to neither end was placed on a guess"
fi
[ -f orphan-producer.yaml ] && fail "a refused cut still wrote a half"
grep -qF "duck" orphan.out || fail "the refusal does not name the block it cannot place"
pass "it names the block rather than choosing a side"

echo ""
echo -e "${GREEN}All split checks passed.${NC}"
