#!/usr/bin/env bash
# Step two: a local coordinator and runner, and the service as a node of its own (it hosts the node in its process and
# runs the dtpipe tool package its build downloaded). Two bricks are read: the service's Arrow socket and dtpipe's own
# `generate:1000`. Each has a pipeline armed and run twice with no restart, then once more after the service restarts.
#
#   samples/arrow-socket-source/spike-node.sh            # work directory under mktemp
#   SPIKE_WORK=/some/dir PORT=18080 samples/arrow-socket-source/spike-node.sh
#   DTPIPE_DLL=src/DtPipe/bin/Debug/net10.0/DtPipe.dll ...    # the node runs a local build instead of the package
#
# Needs dist/release/dtpipe and dist/release/coordinator/ (./build.sh), dotnet, curl and jq. The first build downloads
# the dtpipe tool package from nuget.org.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
DTPIPE="${DTPIPE:-$ROOT/dist/release/dtpipe}"
COORDINATOR="${COORDINATOR:-$ROOT/dist/release/coordinator/dtpipe-coordinator}"
WORK="${SPIKE_WORK:-$(mktemp -d)}"
PORT="${PORT:-18080}"
URL="http://127.0.0.1:$PORT"
SOCKETS=/tmp
PIDS=()

cleanup() { for pid in "${PIDS[@]}"; do kill "$pid" 2>/dev/null || true; done; wait 2>/dev/null || true; }
trap cleanup EXIT

say() { printf '\n== %s\n' "$*"; }
fail() { echo "FAIL: $*" >&2; exit 1; }
mkdir -p "$WORK"
cd "$WORK"
echo "Work directory: $WORK"

say "Build the service"
# The node library is not published yet: pack it into dist/node-host under a version of its own, so a restore never serves an
# older copy of the same version from the NuGet cache.
NODE_HOST_VERSION="1.16.0-local.$(date +%s)"
rm -f "$ROOT"/dist/node-host/*-local.*.nupkg
"$ROOT/tests/scripts/validate_node_host_package.sh" --version "$NODE_HOST_VERSION" >/dev/null
dotnet build "$HERE/ArrowSocketSource.csproj" -p:NodeHostVersion="$NODE_HOST_VERSION" -v q -nologo | tail -1
SERVICE_EXE="$HERE/bin/Debug/net10.0/ArrowSocketSource"
# The dtpipe the node runs, for the witness: the local build when given, else the package the build downloaded.
TOOL="${DTPIPE_DLL:-$(ls "${NUGET_PACKAGES:-$HOME/.nuget/packages}"/dtpipe/1.16.0/tools/*/any/DtPipe.dll | head -1)}"
echo "dtpipe of the node: $TOOL"
NODE_DTPIPE=()
[ -n "${DTPIPE_DLL:-}" ] && NODE_DTPIPE=(--dtpipe "$DTPIPE_DLL")

say "Keys, rights and library"
for id in admin runner-1 shop; do [ -f "$id.key" ] || "$DTPIPE" keygen "$id" --out "$id.key" >/dev/null; done
# The matrix only: identities come with their keys from the bootstrap settings below.
cat > rights-seed.json <<'EOF'
{ "identities": [], "matrix": { "data": ["runner", "data"], "runner": ["data"] } }
EOF
mkdir -p library reports
cat > library/api-orders-by-country.yaml <<'EOF'
# Orders by country and status: reads the orders brick of the service's node (a .NET source on an Arrow socket),
# aggregates on the runner, writes through the node's report brick.
orders:
  input: brick:shop/orders
by-country:
  from: orders
  provider-options:
    sql:
      query: SELECT country, status, COUNT(*) AS orders, SUM(total) AS total FROM orders GROUP BY country, status ORDER BY country, status
write:
  from: by-country
  output: brick:shop/report
EOF
cat > library/generated-numbers.yaml <<'EOF'
# Counts and sums the numbers brick (dtpipe's own generate source) on the runner.
numbers:
  input: brick:shop/numbers
total:
  from: numbers
  provider-options:
    sql:
      query: SELECT COUNT(*) AS n, SUM(GenerateIndex) AS total FROM numbers
write:
  from: total
  output: brick:shop/numbers-report
EOF
echo '{ "orders": { "mode": "strict" }, "write": { "mode": "strict" } }' > library/api-orders-by-country.bindings.json
echo '{ "numbers": { "mode": "strict" }, "write": { "mode": "strict" } }' > library/generated-numbers.bindings.json

say "Coordinator on $URL"
Coordinator__Initialize=true \
Coordinator__PublicUrl="$URL" \
Coordinator__Auth__AllowHttpKeys=true \
Coordinator__Studio__ClientId=dtpipe-studio \
Coordinator__StateDirectory="$WORK/coordinator-state" \
Coordinator__LibrarySeed="$WORK/library" \
Coordinator__RightsSeed="$WORK/rights-seed.json" \
Coordinator__Bootstrap__AdminKey="@$WORK/admin.key" \
Coordinator__Bootstrap__Identities__0__Id=runner-1 \
Coordinator__Bootstrap__Identities__0__Name=runner-1 \
Coordinator__Bootstrap__Identities__0__Groups__0=runner \
Coordinator__Bootstrap__Identities__0__Key="@$WORK/runner-1.key" \
Coordinator__Bootstrap__Identities__1__Id=shop \
Coordinator__Bootstrap__Identities__1__Name=shop \
Coordinator__Bootstrap__Identities__1__Groups__0=data \
Coordinator__Bootstrap__Identities__1__Key="@$WORK/shop.key" \
ASPNETCORE_URLS="$URL" \
  "$COORDINATOR" > coordinator.log 2>&1 &
PIDS+=($!)
for _ in $(seq 60); do curl -sf "$URL/.well-known/openid-configuration" >/dev/null && break; sleep 1; done

TOKEN=$(curl -sf "$URL/connect/token" -d grant_type=client_credentials -d client_id=admin \
  --data-urlencode "client_secret=$(cat admin.key)" | jq -r .access_token)
api() { local method=$1 path=$2; shift 2; curl -s -X "$method" "$URL/api$path" -H "Authorization: Bearer $TOKEN" \
  -H "X-Dtpipe-Csrf: 1" -H "Content-Type: application/json" "$@"; }
echo "flow matrix: $(api GET /flow-matrix | jq -c .groups)"

say "Runner"
mkdir -p runner
(cd runner && exec "$DTPIPE" node --runner --name runner-1 --coordinator "$URL" --client-secret "@$WORK/runner-1.key") > runner.log 2>&1 &
PIDS+=($!)

start_service() {
  # Started as its own executable, not through `dotnet`: the node finds the dotnet host from its runtime.
  "$SERVICE_EXE" node --coordinator "$URL" --name shop --key-file "$WORK/shop.key" --work "$WORK/shop" \
    --report-dir "$WORK/reports" --socket-dir "$SOCKETS" ${NODE_DTPIPE[@]+"${NODE_DTPIPE[@]}"} >> service.log 2>&1 &
  SERVICE=$!
  PIDS+=("$SERVICE")
}
wait_nodes() {
  for _ in $(seq 60); do
    local online; online=$(api GET /nodes | jq -r '[.[] | select(.online) | .name] | sort | join(",")')
    [ "$online" = "runner-1,shop" ] && { echo "online: $online"; return 0; }
    sleep 1
  done
  echo "nodes not online: $(api GET /nodes | jq -c '[.[] | {name, online}]')"; return 1
}
versions() { api GET /bricks | jq -c '[.[] | {id, version}] | sort_by(.id)'; }

say "Service (hosts the node 'shop')"
start_service
wait_nodes
BEFORE=$(versions)
echo "bricks: $(api GET /bricks | jq -c '[.[] | {node, id, kind, version}]')"
grep -h "runs dtpipe" service.log | sed -E 's/^[^ ]+ //'

say "Nothing of the socket reaches the coordinator"
if { api GET /nodes; api GET /bricks; api GET /library; } | grep -q "dtps-"; then fail "a socket path is visible to the coordinator"; fi
echo "no socket path in /nodes, /bricks or /library"

say "Previews"
echo "-- shop/orders"; api POST /bricks/shop/orders/preview | jq -r '.csv // .' | head -4
echo "-- shop/numbers"; api POST /bricks/shop/numbers/preview | jq -r '.csv // .' | head -4

say "Arm both pipelines"
for p in api-orders-by-country generated-numbers; do
  api POST "/library/$p/deploy" | jq -c '{pipelineId, error}' | cut -c1-300
done

run_once() {
  local label=$1 pipeline=$2 run state
  run=$(api POST /runs -d "{\"pipelineId\":\"$pipeline\"}" | jq -r .runId)
  for _ in $(seq 120); do
    state=$(api GET "/runs?pipeline=$pipeline" | jq -r --arg id "$run" '.[] | select(.runId == $id) | .state')
    case "$state" in succeeded|failed|cancelled|refused|error) break;; esac
    sleep 1
  done
  echo "$label: run $run $state"
  if [ "$state" != succeeded ]; then
    api GET "/runs?pipeline=$pipeline" | jq -c --arg id "$run" '.[] | select(.runId == $id) | {state, refusal, result}' | cut -c1-800
    return 1
  fi
}

check_orders() {
  # The same read, by the same dtpipe, straight from the service's socket.
  dotnet "$TOOL" -i "arrow:pipe://$SOCKETS/dtps-$SERVICE/orders.sock" --alias orders --from orders \
    --sql "SELECT country, status, COUNT(*) AS orders, SUM(total) AS total FROM orders GROUP BY country, status ORDER BY country, status" \
    -o "csv:$WORK/witness.csv" --no-stats >/dev/null 2>&1
  if diff -q "$WORK/witness.csv" reports/report.csv >/dev/null; then echo "$1: orders report identical to a read of the socket ($(($(wc -l < reports/report.csv) - 1)) rows)"
  else echo "$1: orders report differs from a read of the socket"; diff "$WORK/witness.csv" reports/report.csv | head -10; return 1; fi
}
check_numbers() {
  local got; got=$(tail -n +2 reports/numbers-report.csv | tr -d '\r')
  [ "$got" = "1000,499500" ] || fail "$1: numbers report is '$got', expected '1000,499500'"
  echo "$1: numbers report 1000 rows, sum 499500"
}
round() {
  rm -f reports/*.csv
  run_once "$1 orders" api-orders-by-country; check_orders "$1"
  run_once "$1 numbers" generated-numbers; check_numbers "$1"
}

say "Two rounds, no restart"
round "round 1"
round "round 2"

say "Service restarted, third round on the same armings"
kill "$SERVICE"; wait "$SERVICE" 2>/dev/null || true
sleep 2
start_service
wait_nodes
[ "$(versions)" = "$BEFORE" ] || fail "the bricks' versions changed with the restart: $BEFORE -> $(versions)"
echo "bricks' versions unchanged by the restart"
round "round 3"

say "Done"
echo "Logs: $WORK/{coordinator,runner,service}.log"
