#!/bin/bash
set -e

# validate_bind.sh
# --bind-input/--bind-output wire a run-specific location into a fragment's 'arrow:-' boundary
# (dtpipe split), against the real binary and real named pipes.
#
# Case 1 is the shape a single shell pipe cannot carry: a fragment with TWO links (a lone stage
# between a producer and a consumer), so one process's stdin and stdout would each have to be "the
# pipe" at once. Two named pipes stand in for the two neighbours split would produce.
#
# Case 2 is the join: two bound inputs feed a --sql join, and the result must equal a monolithic
# witness's - compared as a set, sorted, since DuckDB does not guarantee row order (CLAUDE.md).
#
# Cases 3-4 spot-check the CLI wiring for two of the named refusals AliasBindingApplierTests and
# PipelineValidatorTests cover directly; case 4 is the only coverage --export-job's refusal has,
# since it lives in Program.cs ahead of PipelineValidator.
#
# Case 5 binds a dry run to a pipe nobody serves: the writer is neutralised, so nothing may dial it.
#
# Case 6 drives arrow:pipe://<name> - client-only by design - through both directions against the
# real binary.
#
# The server role every case needs (DtPipe.PipelineNode's own job, out of DtPipe.sln) is stood in
# for by tools/ArrowPipeServer.cs, a file-based dotnet app.
#
# File sources and named pipes only (no database), so this runs in CI like the rest.

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

PIPE_TOOL="$SCRIPT_DIR/tools/ArrowPipeServer.cs"
# Compile the server once: the cases below start several at the same time.
dotnet run "$PIPE_TOOL" > /dev/null 2>&1 || true

# serve <pipe> <relay-out|relay-in> <file> <log>: a pipe server in the background, pid in $SERVER_PID.
serve() {
    dotnet run "$PIPE_TOOL" -- "$1" "$2" "$3" 20 > "$4" 2>&1 &
    SERVER_PID=$!
}

echo ""
echo "--- Case 1: a fragment with two links, run for real over two named pipes ---"
printf 'id,name,email\n1,Ana,ana@corp.com\n2,Bo,bo@corp.com\n3,Cyd,cyd@corp.com\n' > people.csv
"$DTPIPE" -i csv:people.csv -o arrow:people.ipcbytes --no-stats > /dev/null \
    || fail "producing the stream fixture failed"

C1_IN="dtpipe-validate-bind-c1-in-$$"
C1_OUT="dtpipe-validate-bind-c1-out-$$"
serve "$C1_IN" relay-out people.ipcbytes c1_in.log;  PRODUCER_PID=$SERVER_PID
serve "$C1_OUT" relay-in anon.ipcbytes c1_out.log;   CONSUMER_PID=$SERVER_PID
sleep 1

"$DTPIPE" -i arrow:- --fake "email:internet.email" --alias anonymiser -o arrow:- \
          --bind-input "anonymiser=pipe://$C1_IN" --bind-output "anonymiser=pipe://$C1_OUT" --no-stats \
    || fail "the anonymiser failed: $(cat c1_in.log c1_out.log)"

wait "$PRODUCER_PID" || fail "the producer's pipe server failed: $(cat c1_in.log)"
wait "$CONSUMER_PID" || fail "the consumer's pipe server failed: $(cat c1_out.log)"

"$DTPIPE" -i arrow:anon.ipcbytes -o csv:anon.csv --no-stats > /dev/null \
    || fail "reading the anonymised stream back failed"
[ "$(tail -n +2 anon.csv | wc -l | tr -d ' ')" = "3" ] || fail "row count changed crossing the two pipes"
grep -q "ana@corp.com" anon.csv && fail "the original email survived the anonymiser"
pass "three rows crossed two named pipes, --fake ran on the middle process"

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

"$DTPIPE" -i csv:orders.csv -o arrow:orders.ipcbytes --no-stats > /dev/null \
    || fail "producing the orders fixture failed"
"$DTPIPE" -i csv:customers.csv -o arrow:customers.ipcbytes --no-stats > /dev/null \
    || fail "producing the customers fixture failed"

C2_ORDERS="dtpipe-validate-bind-c2-orders-$$"
C2_CUSTOMERS="dtpipe-validate-bind-c2-customers-$$"
serve "$C2_ORDERS" relay-out orders.ipcbytes c2_orders.log;           ORDERS_PID=$SERVER_PID
serve "$C2_CUSTOMERS" relay-out customers.ipcbytes c2_customers.log;  CUSTOMERS_PID=$SERVER_PID
sleep 1

"$DTPIPE" -i arrow:- --alias orders \
          -i arrow:- --alias customers \
          --from orders --ref customers --sql "$JOIN_SQL" --alias joined \
          -o csv:joined.csv \
          --bind-input "orders=pipe://$C2_ORDERS,customers=pipe://$C2_CUSTOMERS" --no-stats \
    || fail "the bound join did not run: $(cat c2_orders.log c2_customers.log)"

wait "$ORDERS_PID" || fail "the orders pipe server failed: $(cat c2_orders.log)"
wait "$CUSTOMERS_PID" || fail "the customers pipe server failed: $(cat c2_customers.log)"

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
echo "--- Case 5: --dry-run bound to a pipe nobody serves neither dials it nor hangs ---"
# A dry run neutralises the writer. A client that dialled the pipe would wait out its connect
# timeout (10s) and fail, so finishing sooner than that, and cleanly, is the proof it never did.
"$DTPIPE" -i "generate:1000" --fake "Email:internet.email" -o "arrow:pipe://dtpipe-validate-bind-nobody-$$" \
    --dry-run 5 --no-stats > dryrun.out 2>&1 &
DRYRUN_PID=$!

SECS=0
while kill -0 "$DRYRUN_PID" 2>/dev/null && [ "$SECS" -lt 8 ]; do
    sleep 1
    SECS=$((SECS + 1))
done

if kill -0 "$DRYRUN_PID" 2>/dev/null; then
    kill -9 "$DRYRUN_PID" 2>/dev/null
    fail "a dry-run bound to an unserved pipe did not complete within 8s (it dialled the pipe)"
fi
wait "$DRYRUN_PID" || fail "the dry-run bound to an unserved pipe exited non-zero: $(cat dryrun.out)"
pass "a dry-run bound to an unserved pipe completes in at most ${SECS}s without dialling it"

echo ""
echo "--- Case 6: arrow:pipe://<name>, both directions, against a real named-pipe server ---"
PIPE_IN="dtpipe-validate-bind-in-$$"
PIPE_OUT="dtpipe-validate-bind-out-$$"

"$DTPIPE" -i csv:people.csv -o arrow:stream.ipcbytes --no-stats > /dev/null \
    || fail "producing the stream-format fixture failed"

dotnet run "$PIPE_TOOL" -- "$PIPE_IN" relay-out stream.ipcbytes 15 > pipe_reader_server.out 2>&1 &
READER_SERVER_PID=$!
sleep 2
"$DTPIPE" -i "arrow:pipe://$PIPE_IN" -o csv:pipe_read.csv --no-stats \
    || fail "dtpipe reading from arrow:pipe:// failed: $(cat pipe_reader_server.out)"
wait "$READER_SERVER_PID" || fail "the pipe server (relay-out) failed: $(cat pipe_reader_server.out)"

# Content, not byte-for-byte: the CSV writer's own line ending (CRLF, RFC 4180) is unrelated to
# the pipe - stripped so this checks what crossed the pipe, not an incidental writer default.
tr -d '\r' < people.csv | sort > people.sorted
tr -d '\r' < pipe_read.csv | sort > pipe_read.sorted
diff people.sorted pipe_read.sorted > /dev/null \
    || fail "rows read over arrow:pipe:// differ from the source"

dotnet run "$PIPE_TOOL" -- "$PIPE_OUT" relay-in pipe_written.ipcbytes 15 > pipe_writer_server.out 2>&1 &
WRITER_SERVER_PID=$!
sleep 2
"$DTPIPE" -i csv:people.csv -o "arrow:pipe://$PIPE_OUT" --no-stats \
    || fail "dtpipe writing to arrow:pipe:// failed: $(cat pipe_writer_server.out)"
wait "$WRITER_SERVER_PID" || fail "the pipe server (relay-in) failed: $(cat pipe_writer_server.out)"

cmp stream.ipcbytes pipe_written.ipcbytes \
    || fail "bytes written over arrow:pipe:// differ from a direct file write of the same source"
pass "arrow:pipe://<name> round-trips both directions against a real named-pipe server"

echo ""
echo -e "${GREEN}All alias binding checks passed.${NC}"
