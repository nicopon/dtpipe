#!/usr/bin/env bash
# Starts, stops and inspects the distributed lab: one coordinator, four data-node hosts and a
# runner, as plain local processes. Everything the lab writes lives under .state/ (databases,
# fragments, library, runs, logs).
#
#   ./lab.sh up        build, seed if needed, start everything, print the page URL
#                      (LAB_NODE_SANDBOX=1 starts the nodes in sandbox mode: see "nodes")
#   ./lab.sh down      stop every lab process
#   ./lab.sh nodes M   restart the node hosts, M = strict (a data node runs only its own bricks,
#                      the default) or sandbox (it runs any reader or writer, whatever it reads or
#                      writes: the Lab view's mode; compute goes to the runner in both)
#   ./lab.sh status    which processes run, and what the coordinator sees
#   ./lab.sh seed      rebuild the four databases (LAB_SCALE multiplies the row counts)
#   ./lab.sh smoke     every pipeline, distributed, against its monolithic witness
#   ./lab.sh ui        both pages render, and a designer scenario, its panels, its zoom and its placement pass (headless Chrome)
#   ./lab.sh logs      follow every log
#   ./lab.sh reset     down, then delete .state/
set -euo pipefail

LAB="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$LAB/../.." && pwd)"
STATE="$LAB/.state"
PORT="${LAB_PORT:-5180}"
URL="http://127.0.0.1:$PORT"
IDP_URL="http://127.0.0.1:${LAB_IDP_PORT:-5190}"
DTPIPE="${DTPIPE:-$REPO/dist/release/dtpipe}"
NODES=(node-1 node-2 node-3 node-4 runner-1)

require_dtpipe() {
    if [[ ! -x "$DTPIPE" ]]; then
        echo "dtpipe not found at $DTPIPE: run ./build.sh at the repository root first (or set DTPIPE)." >&2
        exit 1
    fi
}

build() {
    echo "Building the lab"
    dotnet build "$LAB/Lab.Idp/Lab.Idp.csproj" -c Release -o "$STATE/bin/idp" -v q -nologo >"$STATE/logs/build.log" 2>&1 \
        || { cat "$STATE/logs/build.log" >&2; exit 1; }
    # Published, not built: the server serves the wwwroot published beside it.
    dotnet publish "$REPO/src/DtPipe.Coordinator.Server/DtPipe.Coordinator.Server.csproj" -c Release -o "$STATE/bin/coordinator" -v q -nologo >>"$STATE/logs/build.log" 2>&1 \
        || { cat "$STATE/logs/build.log" >&2; exit 1; }
    dotnet build "$LAB/Lab.NodeHost/Lab.NodeHost.csproj" -c Release -o "$STATE/bin/node" -v q -nologo >>"$STATE/logs/build.log" 2>&1 \
        || { cat "$STATE/logs/build.log" >&2; exit 1; }
    dotnet build "$REPO/src/DtPipe.PipelineNode.Runner/DtPipe.PipelineNode.Runner.csproj" -c Release -o "$STATE/bin/runner" -v q -nologo >>"$STATE/logs/build.log" 2>&1 \
        || { cat "$STATE/logs/build.log" >&2; exit 1; }
}

seed() {
    require_dtpipe
    echo "Seeding the node databases"
    "$LAB/seed/seed.sh" "$DTPIPE" "$STATE" "${LAB_SCALE:-1}"
}

is_running() { [[ -f "$STATE/run/$1.pid" ]] && kill -0 "$(cat "$STATE/run/$1.pid")" 2>/dev/null; }

start() {
    local name="$1"; shift
    if is_running "$name"; then echo "  $name already running"; return; fi
    "$@" >"$STATE/logs/$name.log" 2>&1 &
    echo $! >"$STATE/run/$name.pid"
    echo "  $name started (pid $!)"
}

# The test IdP every token comes from: the nodes' client credentials, and the coordinator's trust.
# Its signing key lives in .state/idp/, so tokens survive its restart. Extra arguments are passed on
# (tools/campaign.py shortens the token lifetime with --Idp:AccessTokenLifetimeSeconds=20).
start_idp() {
    start idp dotnet "$STATE/bin/idp/Lab.Idp.dll" --urls "$IDP_URL" --Idp:Issuer="$IDP_URL" \
        --Idp:ClientsFile="$LAB/idp-clients.json" --Idp:SigningKeyFile="$STATE/idp/signing-key.pem" "$@"
    for _ in $(seq 1 60); do
        python3 -c 'import sys,urllib.request; urllib.request.urlopen(sys.argv[1] + "/healthz", timeout=2)' "$IDP_URL" 2>/dev/null && return
        sleep 0.5
    done
    echo "The IdP did not come up; see $STATE/logs/idp.log" >&2; exit 1
}

# Replaces this shell with the coordinator's server, in lab mode, on the lab's state and seeds. Extra
# arguments are passed on. The one way the server starts, for this script and for tools/campaign.py.
exec_coordinator() {
    exec dotnet "$STATE/bin/coordinator/DtPipe.Coordinator.Server.dll" --urls "$URL" \
        --Coordinator:StateDirectory="$STATE" --Coordinator:DtPipe="$DTPIPE" --Coordinator:Authority="$IDP_URL" \
        --Coordinator:LabMode=true --Coordinator:Scenarios="$LAB/pipelines" \
        --Coordinator:LibrarySeed="$LAB/library-seed" --Coordinator:RightsSeed="$LAB/rights-seed.json" \
        --Coordinator:Ui:ClientSecret=coordinator-ui-lab-secret --Coordinator:Roles:Admin="${LAB_ADMIN:-labadmin}@dtpipe.test" \
        --Coordinator:Roles:Designer=labdesigner@dtpipe.test --Coordinator:Roles:Reader=labreader@dtpipe.test "$@"
}

# Replaces this shell with node $1's host, pointed at the coordinator $2: the official runner for a
# node file whose role is runner, configured through its environment, else the lab's data node host.
# The one way a node starts, for this script and for tools/campaign.py.
exec_node() {
    local node="$1" coordinator="$2" file="$LAB/nodes/$1.json"
    if [[ "$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1])).get("role", ""))' "$file")" == runner ]]; then
        local id secret description
        { read -r id; read -r secret; read -r description; } < <(python3 -c \
            'import json,sys; n=json.load(open(sys.argv[1])); print(n.get("clientId", n["name"])); print(n["secret"]); print(n.get("description", ""))' "$file")
        export DtPipeNode__Name="$node" DtPipeNode__Description="$description" DtPipeNode__CoordinatorUrl="$coordinator" \
            DtPipeNode__DtPipe="$DTPIPE" DtPipeNode__StateDirectory="$STATE/$node" DtPipeNode__Oidc__Authority="$IDP_URL" \
            DtPipeNode__Oidc__ClientId="$id" DtPipeNode__Oidc__ClientSecret="$secret"
        exec dotnet "$STATE/bin/runner/DtPipe.PipelineNode.Runner.dll"
    fi
    exec dotnet "$STATE/bin/node/Lab.NodeHost.dll" --config "$file" --state "$STATE" --coordinator "$coordinator" --idp "$IDP_URL" --dtpipe "$DTPIPE"
}

# A data node holds its fragments to its bricks unless LAB_NODE_SANDBOX=1 is in its own environment;
# a strict host has the variable removed, whatever this shell exports. A runner runs any fragment.
start_nodes() {
    local setting=(-u LAB_NODE_SANDBOX)
    [[ "$1" == sandbox ]] && setting=(LAB_NODE_SANDBOX=1)
    for node in "${NODES[@]}"; do
        start "$node" env "${setting[@]}" "$LAB/lab.sh" exec-node "$node" "$URL"
    done
}

# Whether every node host announces the mode $1 (waiting up to $2 seconds): their own word is the only truth.
nodes_report() { python3 "$LAB/tools/nodes_report.py" "$URL" "$1" "$2" "${#NODES[@]}"; }

up() {
    require_dtpipe
    mkdir -p "$STATE/logs" "$STATE/run"
    build
    [[ -f "$STATE/node-1/crm.sqlite" ]] || seed

    echo "Starting"
    start_idp
    start coordinator "$LAB/lab.sh" exec-coordinator

    for _ in $(seq 1 60); do
        curl -fs "$URL/healthz" >/dev/null 2>&1 && break
        sleep 0.5
    done
    curl -fs "$URL/healthz" >/dev/null || { echo "The coordinator did not come up; see $STATE/logs/coordinator.log" >&2; exit 1; }

    start_nodes "$([[ "${LAB_NODE_SANDBOX:-}" == 1 ]] && echo sandbox || echo strict)"

    echo
    echo "Lab ready: $URL"
}

# The fragments a node host holds go with it, so every deployment is undone first.
nodes_mode() {
    local mode="${1:-}"
    [[ "$mode" == strict || "$mode" == sandbox ]] || { echo "usage: $0 nodes strict|sandbox" >&2; exit 1; }
    if nodes_report "$mode" 0; then return; fi

    echo "Nodes: $mode"
    python3 - "$URL" "$LAB/tools" <<'PY'
import sys
sys.path.insert(0, sys.argv[2])
from lab_session import Session
session = Session(sys.argv[1], timeout=60)
for d in session.call("/api/deployments"):
    try: session.call(f"/api/deployments/{d['pipelineId']}", method="DELETE")
    except Exception as e: print(f"  {d['pipelineId']}: not undeployed ({e})")
PY
    for node in "${NODES[@]}"; do
        if is_running "$node"; then kill -TERM "$(cat "$STATE/run/$node.pid")" 2>/dev/null || true; fi
    done
    for node in "${NODES[@]}"; do
        for _ in $(seq 1 20); do is_running "$node" || break; sleep 0.25; done
        if is_running "$node"; then kill -KILL "$(cat "$STATE/run/$node.pid")" 2>/dev/null || true; fi
        rm -f "$STATE/run/$node.pid"
    done
    start_nodes "$mode"
    nodes_report "$mode" 90 || { echo "The node hosts did not come back in $mode mode." >&2; exit 1; }
}

# The lab pass puts branches that are not bricks on data nodes, so it runs on sandbox nodes; every
# other pass runs on strict ones, which is what they prove.
smoke() {
    local pass="${1:-}"
    case "$pass" in lab|library|rights|faults|bricks|sandbox) shift ;; *) pass="" ;; esac
    local code=0
    run_pass() {
        local mode="$1" name="$2"; shift 2
        nodes_mode "$mode"
        python3 -u "$LAB/smoke.py" "$URL" "$DTPIPE" "$STATE" "$name" "$@" || code=1
    }
    if [[ -z "$pass" ]]; then
        run_pass sandbox lab "$@"
        run_pass sandbox sandbox
        run_pass strict library "$@"
        for name in rights faults bricks; do run_pass strict "$name"; done
    elif [[ "$pass" == lab || "$pass" == sandbox ]]; then
        run_pass sandbox "$pass" "$@"
    else
        run_pass strict "$pass" "$@"
    fi
    return $code
}

down() {
    local names=(coordinator idp "${NODES[@]}")
    # Nodes first, so each can tear down its own dtpipe children while the coordinator still answers.
    for name in "${NODES[@]}" coordinator idp; do
        if is_running "$name"; then
            kill -TERM "$(cat "$STATE/run/$name.pid")" 2>/dev/null || true
        fi
    done
    for name in "${names[@]}"; do
        for _ in $(seq 1 20); do is_running "$name" || break; sleep 0.25; done
        if is_running "$name"; then kill -KILL "$(cat "$STATE/run/$name.pid")" 2>/dev/null || true; fi
        rm -f "$STATE/run/$name.pid"
    done
    echo "Lab stopped"
}

status() {
    for name in idp coordinator "${NODES[@]}"; do
        if is_running "$name"; then echo "  $name  running (pid $(cat "$STATE/run/$name.pid"))"; else echo "  $name  stopped"; fi
    done
    if curl -fs "$URL/healthz" >/dev/null 2>&1; then
        echo
        python3 "$LAB/tools/lab_session.py" "$URL" /api/nodes | python3 -c "$(cat <<'EOF'
import json, sys
for n in json.load(sys.stdin):
    frags = ", ".join(f["fragment"] + ":" + f["state"] for f in n["fragments"]) or "-"
    state = "online " if n["online"] else "offline"
    print(f"  {n['name']:8} {state} {'+'.join(n['groups']):10} {frags}")
EOF
)"
    fi
}

case "${1:-}" in
    up) up ;;
    down) down ;;
    status) status ;;
    seed) mkdir -p "$STATE"; seed ;;
    nodes) shift; nodes_mode "${1:-}" ;;
    exec-node) shift; exec_node "$@" ;;
    exec-coordinator) shift; exec_coordinator "$@" ;;
    idp) shift; mkdir -p "$STATE/logs" "$STATE/run"; start_idp "$@" ;;
    smoke) shift; smoke "$@" ;;
    ui) python3 "$LAB/tools/ui_check.py" "$URL" "$STATE/ui" ;;
    logs) tail -n 20 -F "$STATE"/logs/*.log ;;
    reset) down; rm -rf "$STATE"; echo "Removed $STATE" ;;
    *) awk 'NR > 1 && !/^#/ { exit } NR > 1 { sub(/^# ?/, ""); print }' "$0"; exit 1 ;;
esac
