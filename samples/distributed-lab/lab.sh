#!/usr/bin/env bash
# Starts, stops and inspects the distributed lab: one coordinator and four pipeline-node hosts, as
# plain local processes. Everything the lab writes lives under .state/ (databases, fragments, logs).
#
#   ./lab.sh up        build, seed if needed, start everything, print the page URL
#   ./lab.sh down      stop every lab process
#   ./lab.sh status    which processes run, and what the coordinator sees
#   ./lab.sh seed      rebuild the four databases (LAB_SCALE multiplies the row counts)
#   ./lab.sh smoke     every catalog pipeline, distributed, against its monolithic witness
#   ./lab.sh ui        every catalog pipeline renders in the page (headless Chrome)
#   ./lab.sh logs      follow every log
#   ./lab.sh reset     down, then delete .state/
set -euo pipefail

LAB="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$LAB/../.." && pwd)"
STATE="$LAB/.state"
PORT="${LAB_PORT:-5180}"
URL="http://127.0.0.1:$PORT"
DTPIPE="${DTPIPE:-$REPO/dist/release/dtpipe}"
NODES=(node-1 node-2 node-3 node-4)

require_dtpipe() {
    if [[ ! -x "$DTPIPE" ]]; then
        echo "dtpipe not found at $DTPIPE: run ./build.sh at the repository root first (or set DTPIPE)." >&2
        exit 1
    fi
}

build() {
    echo "Building the lab"
    dotnet build "$LAB/Lab.Coordinator/Lab.Coordinator.csproj" -c Release -o "$STATE/bin/coordinator" -v q -nologo >"$STATE/logs/build.log" 2>&1 \
        || { cat "$STATE/logs/build.log" >&2; exit 1; }
    dotnet build "$LAB/Lab.NodeHost/Lab.NodeHost.csproj" -c Release -o "$STATE/bin/node" -v q -nologo >>"$STATE/logs/build.log" 2>&1 \
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

up() {
    require_dtpipe
    mkdir -p "$STATE/logs" "$STATE/run"
    build
    [[ -f "$STATE/node-1/crm.sqlite" ]] || seed

    echo "Starting"
    start coordinator dotnet "$STATE/bin/coordinator/Lab.Coordinator.dll" \
        --urls "$URL" --lab-root "$LAB" --state "$STATE" --dtpipe "$DTPIPE"

    for _ in $(seq 1 60); do
        curl -fs "$URL/api/nodes" >/dev/null 2>&1 && break
        sleep 0.5
    done
    curl -fs "$URL/api/nodes" >/dev/null || { echo "The coordinator did not come up; see $STATE/logs/coordinator.log" >&2; exit 1; }

    for node in "${NODES[@]}"; do
        start "$node" dotnet "$STATE/bin/node/Lab.NodeHost.dll" \
            --config "$LAB/nodes/$node.json" --state "$STATE" --coordinator "$URL" --dtpipe "$DTPIPE"
    done

    echo
    echo "Lab ready: $URL"
}

down() {
    local names=(coordinator "${NODES[@]}")
    # Nodes first, so each can tear down its own dtpipe children while the coordinator still answers.
    for name in "${NODES[@]}" coordinator; do
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
    for name in coordinator "${NODES[@]}"; do
        if is_running "$name"; then echo "  $name  running (pid $(cat "$STATE/run/$name.pid"))"; else echo "  $name  stopped"; fi
    done
    if curl -fs "$URL/api/nodes" >/dev/null 2>&1; then
        echo
        curl -fs "$URL/api/nodes" | python3 -c "$(cat <<'EOF'
import json, sys
for n in json.load(sys.stdin):
    frags = ", ".join(f["fragment"] + ":" + f["state"] for f in n["fragments"]) or "-"
    state = "online " if n["online"] else "offline"
    print(f"  {n['name']:8} {state} {n['group']:10} {frags}")
EOF
)"
    fi
}

case "${1:-}" in
    up) up ;;
    down) down ;;
    status) status ;;
    seed) mkdir -p "$STATE"; seed ;;
    smoke) shift; python3 "$LAB/smoke.py" "$URL" "$DTPIPE" "$STATE" "$@" ;;
    ui) python3 "$LAB/tools/ui_check.py" "$URL" "$STATE/ui" ;;
    logs) tail -n 20 -F "$STATE"/logs/*.log ;;
    reset) down; rm -rf "$STATE"; echo "Removed $STATE" ;;
    *) awk 'NR > 1 && !/^#/ { exit } NR > 1 { sub(/^# ?/, ""); print }' "$0"; exit 1 ;;
esac
