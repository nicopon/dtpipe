#!/bin/bash
set -e

# validate_bind.sh
# --bind-input/--bind-output wire a run-specific location into a fragment's 'arrow:-' boundary
# (dtpipe split), against the real binary and real named pipes.
#
# Case 1 is the shape a single shell pipe cannot carry: a fragment with TWO links (a lone stage
# between a producer and a consumer), so one process's stdin and stdout would each have to be "the
# pipe" at once. Two real FIFOs stand in for the two neighbours split would produce.
#
# Case 2 is the join: two bound inputs feed a --sql join, and the result must equal a monolithic
# witness's — compared as a set, sorted, since DuckDB does not guarantee row order (CLAUDE.md).
#
# Cases 3-4 spot-check the CLI wiring for two of the named refusals AliasBindingApplierTests and
# PipelineValidatorTests cover directly; case 4 is the only coverage --export-job's refusal has,
# since it lives in Program.cs ahead of PipelineValidator.
#
# Only file sources and FIFOs, so this runs in CI like the rest.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
ARTIFACTS_DIR="$SCRIPT_DIR/artifacts/bind"
rm -rf "$ARTIFACTS_DIR"; mkdir -p "$ARTIFACTS_DIR"

DTPIPE="$PROJECT_ROOT/dist/release/dtpipe"
export DTPIPE_NO_TUI=1

GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

pass() { echo -e "  ${GREEN}OK: $1${NC}"; }
fail() { echo -e "  ${RED}FAIL: $1${NC}"; exit 1; }

echo "========================================"
echo "      DtPipe Alias Binding Validation"
echo "========================================"

if [ ! -f "$DTPIPE" ]; then
    echo "Building release..."
    "$PROJECT_ROOT/build.sh" > /dev/null
fi

A="$ARTIFACTS_DIR"
cd "$A"

echo ""
echo "--- Case 1: a fragment with two links, run for real over two FIFOs ---"
printf 'id,name,email\n1,Ana,ana@corp.com\n2,Bo,bo@corp.com\n3,Cyd,cyd@corp.com\n' > people.csv
mkfifo in.fifo out.fifo

"$DTPIPE" -i csv:people.csv -o arrow:in.fifo --no-stats &
PRODUCER_PID=$!

"$DTPIPE" -i arrow:- --fake "email:internet.email" --alias anonymiser -o arrow:- \
          --bind-input "anonymiser=$A/in.fifo" --bind-output "anonymiser=$A/out.fifo" --no-stats &
ANON_PID=$!

"$DTPIPE" -i arrow:out.fifo -o csv:anon.csv --no-stats

wait "$PRODUCER_PID" || fail "the producer failed"
wait "$ANON_PID" || fail "the anonymiser failed"

[ -f anon.csv ] || fail "the consumer wrote nothing"
[ "$(tail -n +2 anon.csv | wc -l | tr -d ' ')" = "3" ] || fail "row count changed crossing the two FIFOs"
grep -q "ana@corp.com" anon.csv && fail "the original email survived the anonymiser"
pass "three rows crossed two real FIFOs, --fake ran on the middle process"

echo ""
echo "--- Case 2: a join fed by two bound inputs equals the monolithic witness ---"
printf 'id,customer_id,amount\n1,1,10\n2,2,20\n3,1,30\n' > orders.csv
printf 'id,name\n1,Ana\n2,Bo\n' > customers.csv

JOIN_SQL="SELECT o.id, c.name, o.amount FROM orders o JOIN customers c ON o.customer_id = c.id"

"$DTPIPE" -i csv:orders.csv --alias orders \
          -i csv:customers.csv --alias customers \
          --from orders --ref customers --sql "$JOIN_SQL" --alias joined \
          -o csv:witness.csv --no-stats > /dev/null 2>&1 \
    || fail "the monolithic witness did not run"
[ -f witness.csv ] || fail "the witness wrote nothing"

mkfifo orders.fifo customers.fifo

"$DTPIPE" -i csv:orders.csv -o arrow:orders.fifo --no-stats &
ORDERS_PID=$!
"$DTPIPE" -i csv:customers.csv -o arrow:customers.fifo --no-stats &
CUSTOMERS_PID=$!

"$DTPIPE" -i arrow:- --alias orders \
          -i arrow:- --alias customers \
          --from orders --ref customers --sql "$JOIN_SQL" --alias joined \
          -o csv:joined.csv \
          --bind-input "orders=$A/orders.fifo,customers=$A/customers.fifo" --no-stats \
    || fail "the bound join did not run"

wait "$ORDERS_PID" || fail "the orders producer failed"
wait "$CUSTOMERS_PID" || fail "the customers producer failed"

[ -f joined.csv ] || fail "the bound join wrote nothing"
sort witness.csv > witness.sorted
sort joined.csv > joined.sorted
diff witness.sorted joined.sorted > /dev/null \
    || fail "the bound join's result differs from the monolithic witness (as a set)"
pass "a join fed by two bound inputs equals the monolithic witness, compared as a set"

echo ""
echo "--- Case 3: an unknown bound alias is refused by name ---"
if "$DTPIPE" -i csv:people.csv -o csv:case3.csv --bind-input "nosuchbranch=/tmp/x" --no-stats > case3.out 2>&1; then
    fail "an unknown bound alias was accepted"
fi
grep -qF "nosuchbranch" case3.out || fail "the refusal does not name the unknown alias"
pass "an unknown bound alias is refused by name"

echo ""
echo "--- Case 4: --export-job combined with a binding is refused ---"
if "$DTPIPE" -i arrow:- -o csv:case4.csv --alias main --bind-input "main=/tmp/x" --export-job leak.yaml > case4.out 2>&1; then
    fail "--export-job with a binding was accepted"
fi
[ -f leak.yaml ] && fail "a refused export-job still wrote a file"
grep -qF "bind-input" case4.out || fail "the refusal does not name the binding flag"
pass "--export-job combined with a binding is refused"

echo ""
echo -e "${GREEN}All alias binding checks passed.${NC}"
