#!/bin/bash
# linux_replay.sh: what the compose stack and the product's coordinator image owe a Docker host that keeps the owner
# of a bind mount (Linux), which the macOS hosts the stack was built on do not. Played by hand, on the host itself;
# it asks for no sudo, and the first step that fails ends it with a non-zero status.
#
#   ./tools/linux_replay.sh [stack|sqlite|uid]...    no argument plays all three, in this order
#
#   stack    the Redis stack: up, smoke, down, then .stack/ removed by the user who made it
#   sqlite   the stack on the product's default store: up, smoke, restart-check restore, down, .stack/ removed
#   uid      the product's coordinator image on a volume owned by another uid, which it refuses to start on,
#            and on one owned by 1654, which it starts on
#
# The stack steps build the images of the commit HEAD names and need ports 80 and 8080 free, and a purged .stack/.

set -u
LAB="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ROOT="$(cd "$LAB/../.." && pwd)"
IMAGE="dtpipe-linux-replay/coordinator"
NAME="dtpipe-linux-replay-$$"
VOLUME="dtpipe-linux-replay-$$"

step() {
    local label="$1"; shift
    if "$@" >/dev/null 2>&1; then echo "PASS  $label"; else echo "FAIL  $label"; exit 1; fi
}

# Same as step, but shows what the command said: for the steps whose failure is the point.
step_shown() {
    local label="$1"; shift
    if "$@"; then echo "PASS  $label"; else echo "FAIL  $label"; exit 1; fi
}

cycle() {
    local store="$1"; shift
    [[ -e "$LAB/.stack" ]] && { echo "FAIL  $store: $LAB/.stack exists; remove it first" >&2; exit 1; }
    export LAB_STACK_STORE="$store"
    step_shown "$store: stack up" "$LAB/lab.sh" stack up
    step_shown "$store: stack smoke" "$LAB/lab.sh" stack smoke
    [[ "$store" == sqlite ]] && step_shown "$store: restart-check restore" "$LAB/lab.sh" stack restart-check restore
    step "$store: stack down" "$LAB/lab.sh" stack down
    step "$store: .stack removed without sudo" rm -rf "$LAB/.stack"
    [[ ! -e "$LAB/.stack" ]] || { echo "FAIL  $store: .stack still there"; exit 1; }
}

cleanup() {
    docker rm -f "$NAME" >/dev/null 2>&1
    docker volume rm "$VOLUME" >/dev/null 2>&1
}

# The product's image runs as 1654 and writes /state: a volume owned by anyone else, with no write for others, stops the
# start with the server's words about the directory; the same volume handed to 1654 starts and reports healthy. The volume
# holds a file first: Docker gives an empty volume the owner of the image's directory, which would hide the case.
uid() {
    trap cleanup EXIT
    step_shown "uid: image built" docker build -q -f "$ROOT/deploy/Dockerfile" --target coordinator \
        --build-arg SOURCE_REVISION="$(git -C "$ROOT" rev-parse HEAD)" -t "$IMAGE" "$ROOT"
    step "uid: volume made" docker volume create "$VOLUME"
    local run=(docker run --init --name "$NAME" -v "$VOLUME":/state
        -e Coordinator__Initialize=true -e Coordinator__Authority=https://idp.example
        -e Coordinator__Ui__ClientSecret=replay -e Coordinator__PublicUrl=https://coordinator.example)

    step "uid: volume owned by 2000" docker run --rm -v "$VOLUME":/state alpine sh -c 'touch /state/.keep && chown -R 2000:2000 /state' 
    local out
    out="$("${run[@]}" "$IMAGE" 2>&1)"; local rc=$?
    if [[ $rc -ne 0 && "$out" == *"writable by the user the server runs as"* ]]; then echo "PASS  uid: refused to start on a volume of uid 2000"
    else echo "FAIL  uid: exit $rc on a volume of uid 2000"; echo "$out" | tail -5; exit 1; fi
    docker rm -f "$NAME" >/dev/null 2>&1

    step "uid: volume owned by 1654" docker run --rm -v "$VOLUME":/state alpine chown -R 1654:1654 /state
    step "uid: started on a volume of uid 1654" "${run[@]}" -d "$IMAGE"
    local health=""
    for _ in $(seq 1 60); do
        health="$(docker inspect -f '{{.State.Health.Status}}' "$NAME" 2>/dev/null)"
        [[ "$health" == healthy ]] && break
        sleep 1
    done
    [[ "$health" == healthy ]] && echo "PASS  uid: healthy on a volume of uid 1654" \
        || { echo "FAIL  uid: $health on a volume of uid 1654"; docker logs "$NAME" 2>&1 | tail -5; exit 1; }
}

plays=("$@"); [[ ${#plays[@]} -gt 0 ]] || plays=(stack sqlite uid)
for play in "${plays[@]}"; do
    case "$play" in
        stack) cycle redis ;;
        sqlite) cycle sqlite ;;
        uid) uid ;;
        *) echo "usage: $0 [stack|sqlite|uid]..." >&2; exit 1 ;;
    esac
done
echo "All steps passed."
