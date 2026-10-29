#!/bin/bash
set -uo pipefail

# validate_runner_egress.sh
# The runner image's network rules and privileges (deploy/runner-entrypoint.sh), on the images a checkout builds:
#
#   DTPIPE_RUNNER_IMAGE=dtpipe-runner DTPIPE_COORDINATOR_IMAGE=dtpipe-coordinator tests/scripts/validate_runner_egress.sh
#
# Two coordinators stand in for any server, on a network of their own: the runner's list allows the first only. Checked: dtpipe
# runs as 1654 with no capability, its bounding set empty and no_new_privs set, and a user other than root cannot change the
# rules; out, the listed address answers and the other is refused at once; a name of the network resolves; nothing comes in,
# where a runner without a list answers; IPv6 the same, when the network has it; DuckDB and a file read make no connection;
# a list the runner cannot apply stops its start. Needs a Docker host that grants NET_ADMIN, and no network beyond it.

: "${DTPIPE_RUNNER_IMAGE:?set DTPIPE_RUNNER_IMAGE to an image of the runner target of deploy/Dockerfile}"
: "${DTPIPE_COORDINATOR_IMAGE:?set DTPIPE_COORDINATOR_IMAGE to an image of the coordinator target of deploy/Dockerfile}"

GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'
FAILED=0
pass() { echo -e "  ${GREEN}OK: $1${NC}"; }
fail() { echo -e "  ${RED}FAIL: $1${NC}"; FAILED=1; }
check() { local label=$1; shift; if "$@"; then pass "$label"; else fail "$label"; fi; }

P="dtpipe-egress-$$"
NET="$P-net"
cleanup() {
    docker rm -f "$P-coordinator" "$P-other" "$P-runner" "$P-open" >/dev/null 2>&1
    docker network rm "$NET" >/dev/null 2>&1
}
trap cleanup EXIT

echo "========================================"
echo "  DtPipe Runner Egress Validation"
echo "========================================"

if docker network create --ipv6 "$NET" >/dev/null 2>&1; then IPV6=1; else IPV6=0; docker network create "$NET" >/dev/null || exit 1; fi

key() { printf 'dtp_admin_%s' "$(head -c 32 /dev/urandom | base64 | tr '+/' '-_' | tr -d '=\n')"; }
for peer in coordinator other; do
    docker run -d --name "$P-$peer" --network "$NET" \
        -e Coordinator__Initialize=true -e "Coordinator__PublicUrl=http://$P-$peer:8080" \
        -e Coordinator__Auth__AllowHttpKeys=true -e "Coordinator__Bootstrap__AdminKey=$(key)" \
        "$DTPIPE_COORDINATOR_IMAGE" >/dev/null || exit 1
done
for peer in coordinator other; do
    for _ in $(seq 1 60); do docker exec "$P-$peer" curl -fs -o /dev/null http://localhost:8080/livez && break; sleep 1; done
done
addr() { docker inspect -f "{{(index .NetworkSettings.Networks \"$NET\").$2}}" "$1"; }
C4=$(addr "$P-coordinator" IPAddress)
C6=$(addr "$P-coordinator" GlobalIPv6Address)
O6=$(addr "$P-other" GlobalIPv6Address)
[ -n "$C6" ] && [ -n "$O6" ] || IPV6=0
ALLOW="$C4/32"
[ "$IPV6" = 1 ] && ALLOW="$ALLOW,$C6/128"

node_env=(-e DtPipeNode__Name=runner-1 -e "DtPipeNode__CoordinatorUrl=http://$P-coordinator:8080" -e DtPipeNode__Oidc__ClientSecret=unused)
docker run -d --init --name "$P-runner" --network "$NET" --cap-add NET_ADMIN "${node_env[@]}" \
    -e "DTPIPE_EGRESS_ALLOW=$ALLOW" "$DTPIPE_RUNNER_IMAGE" >/dev/null || exit 1
docker run -d --init --name "$P-open" --network "$NET" "${node_env[@]}" "$DTPIPE_RUNNER_IMAGE" >/dev/null || exit 1
for _ in $(seq 1 30); do docker logs "$P-runner" 2>&1 | grep -q 'Application started' && break; sleep 1; done
echo "  list: $ALLOW (IPv6: $([ "$IPV6" = 1 ] && echo yes || echo 'no, the network has none'))"

# The process whose command line is dtpipe node --runner, as the kernel reports it.
worker_status() {
    docker exec "$1" sh -c 'for p in /proc/[0-9]*; do
        if [ "$(tr "\0" " " < $p/cmdline 2>/dev/null)" = "dtpipe node --runner " ]; then cat $p/status; exit; fi; done'
}
STATUS=$(worker_status "$P-runner")
field() { echo "$STATUS" | awk -v k="$1:" '$1 == k { print $2 }'; }
check "dtpipe runs as 1654:1654" [ "$(field Uid)" = 1654 -a "$(field Gid)" = 1654 ]
check "dtpipe has no capability (CapEff, CapPrm, CapBnd all 0)" \
    [ "$(field CapEff)" = 0000000000000000 -a "$(field CapPrm)" = 0000000000000000 -a "$(field CapBnd)" = 0000000000000000 ]
check "dtpipe has no_new_privs" [ "$(field NoNewPrivs)" = 1 ]
RULES=$(docker exec "$P-runner" iptables -S)
check "1654 cannot empty the rules" sh -c "! docker exec -u 1654 '$P-runner' iptables -w -F OUTPUT >/dev/null 2>&1"
check "the rules are unchanged" [ "$(docker exec "$P-runner" iptables -S)" = "$RULES" ]

curl_rc() { docker exec -u 1654 "$1" curl -s -o /dev/null -m 5 "${@:2}"; echo $?; }
check "out: the listed coordinator answers" [ "$(curl_rc "$P-runner" "http://$P-coordinator:8080/livez")" = 0 ]
START=$(date +%s)
check "out: the other server is refused" [ "$(curl_rc "$P-runner" "http://$P-other:8080/livez")" = 7 ]
check "out: refused at once, not after a timeout" [ $(($(date +%s) - START)) -lt 3 ]
check "dns: a name of the network resolves (Docker's resolver, on the loopback)" sh -c "docker exec -u 1654 '$P-runner' getent hosts '$P-other' >/dev/null"
check "in: nothing reaches the runner (a timeout)" [ "$(curl_rc "$P-other" "http://$P-runner:8080/")" = 28 ]
check "in: a runner without a list is reached (refused by its closed port)" [ "$(curl_rc "$P-other" "http://$P-open:8080/")" = 7 ]
check "a runner without a list warns" sh -c "docker logs '$P-open' 2>&1 | grep -q '^\[WARN\] DTPIPE_EGRESS_ALLOW is empty'"
if [ "$IPV6" = 1 ]; then
    check "IPv6 out: the listed coordinator answers" [ "$(curl_rc "$P-runner" "http://[$C6]:8080/livez")" = 0 ]
    check "IPv6 out: the other server is refused" [ "$(curl_rc "$P-runner" "http://[$O6]:8080/livez")" = 7 ]
    check "IPv6 in: nothing reaches the runner" [ "$(curl_rc "$P-other" "http://[$(addr "$P-runner" GlobalIPv6Address)]:8080/")" = 28 ]
fi

rejected() { docker exec "$P-runner" sh -c 'for t in iptables ip6tables; do $t -L OUTPUT -v -n -x 2>/dev/null; done' \
    | awk '$3 == "REJECT" { n += $1 } END { print n + 0 }'; }
BEFORE=$(rejected)
check "DuckDB and a file read run" docker exec -u 1654 "$P-runner" sh -c 'cd /tmp \
    && dtpipe -i generate:100 --alias g --from g --sql "SELECT count(*) AS n FROM g" -o csv:/tmp/n.csv >/dev/null 2>&1 \
    && dtpipe -i duck:memory --query "SELECT 42 AS x" -o parquet:/tmp/x.parquet >/dev/null 2>&1 \
    && dtpipe -i parquet:/tmp/x.parquet -o csv:/tmp/x.csv >/dev/null 2>&1'
check "DuckDB and a file read attempt no connection" [ "$(rejected)" = "$BEFORE" ]

refused() {
    local expect=$1; shift
    local out
    out=$(docker run --rm --network "$NET" "${node_env[@]}" "$@" "$DTPIPE_RUNNER_IMAGE" 2>&1)
    [ $? = 1 ] && echo "$out" | grep -q "^\[ERROR\].*$expect"
}
check "start refused without NET_ADMIN" refused NET_ADMIN -e "DTPIPE_EGRESS_ALLOW=$ALLOW"
check "start refused for a list as uid 1654" refused 'uid 1654' --user 1654:1654 --cap-add NET_ADMIN -e "DTPIPE_EGRESS_ALLOW=$ALLOW"
check "start refused for a malformed mask, named" refused "'$C4' is not a CIDR mask" --cap-add NET_ADMIN -e "DTPIPE_EGRESS_ALLOW=$C4"

if [ "$FAILED" = 0 ]; then echo -e "${GREEN}The runner's egress rules hold.${NC}"; else echo -e "${RED}The runner's egress rules do not hold.${NC}"; fi
exit "$FAILED"
