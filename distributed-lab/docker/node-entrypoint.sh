#!/usr/bin/env bash
# A lab data node in the compose stack: seeds its own database on first start, then runs the lab's
# node host. Its directory is the only one it mounts, so it seeds nothing but itself.
#
#   node-entrypoint.sh <node>     with LAB_SCALE, LAB_COORDINATOR_URL and LAB_IDP_URL in the environment
#
# The scale is recorded beside the data: a node's rows reference the other nodes' counts, so a node
# seeded at another scale than the one asked for refuses to start rather than run on mismatched data.
set -euo pipefail

node="$1"
state=/lab/state
scale="${LAB_SCALE:-1}"
marker="$state/$node/.seeded-scale"

if [[ -f "$marker" ]]; then
    seeded="$(cat "$marker")"
    if [[ "$seeded" != "$scale" ]]; then
        echo "$node was seeded at LAB_SCALE=$seeded, not $scale: empty its directory to seed it again." >&2
        exit 1
    fi
else
    /lab/seed/seed.sh /usr/local/bin/dtpipe "$state" "$scale" "$node"
    echo "$scale" >"$marker"
fi

exec dotnet /app/Lab.NodeHost.dll --config "/lab/nodes/$node.json" --state "$state" \
    --coordinator "$LAB_COORDINATOR_URL" --idp "$LAB_IDP_URL" --dtpipe /usr/local/bin/dtpipe
