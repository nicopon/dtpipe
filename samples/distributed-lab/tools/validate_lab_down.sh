#!/bin/bash
set -e

# validate_lab_down.sh
# Behaviour guard on how the distributed lab stops and starts (lab.sh): `down` stops every
# process of the lab, those a pid file names and those it does not, and answers "Lab stopped" only when none is left; `up`
# refuses to start over a process of the lab no pid file names, or over a port something else holds. A process of the lab
# is one whose command line names its state directory.
#
# The script runs on a throw-away state directory (LAB_STATE) with stand-in processes, so it never touches a lab that is
# running. Scope: the stop and start checks; nothing is built or started. The lab stays outside the build and its CI, so
# this is played by hand: ./tools/validate_lab_down.sh.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LAB="$(cd "$SCRIPT_DIR/.." && pwd)/lab.sh"

GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

pass() { echo -e "  ${GREEN}OK: $1${NC}"; }
fail() { echo -e "  ${RED}FAIL: $1${NC}"; exit 1; }

STATE="$(mktemp -d)"
export LAB_STATE="$STATE"
# `up` needs an executable to be told apart from "dtpipe not built"; it is never run.
export DTPIPE=/bin/sh
PIDS=()
cleanup() {
    for p in "${PIDS[@]}"; do kill "$p" 2>/dev/null || true; done
    rm -rf "$STATE"
}
trap cleanup EXIT

command -v python3 >/dev/null 2>&1 || { echo "python3 is needed to stand in for the lab's processes"; exit 1; }

# A stand-in for a process of the lab: its command line names the state directory. Sets LAST to its pid.
LAST=""
standin() {
    python3 -c 'import time; time.sleep(120)' "$STATE/$1" &
    LAST=$!
    disown "$LAST"
    PIDS+=("$LAST")
}

alive() {
    local state
    state="$(ps -p "$1" -o stat= 2>/dev/null | tr -d ' ')"
    [[ -n "$state" && "$state" != Z* ]]
}

# Runs the lab script, keeping its output and exit status: OUT and RC.
OUT="" RC=0
lab() {
    if OUT="$("$LAB" "$@" 2>&1)"; then RC=0; else RC=$?; fi
}

# 1. A process of the lab no pid file records is stopped, and only then does down say so.
standin orphan
orphan=$LAST
sleep 1
alive "$orphan" || fail "the stand-in did not start"
lab down
[[ $RC -eq 0 ]] || fail "down failed with a process of the lab no pid file names: $OUT"
alive "$orphan" && fail "down said the lab stopped while a process of it, which no pid file names, was still running"
[[ "$OUT" == *"Lab stopped"* ]] || fail "down did not say the lab stopped: $OUT"
pass "down stops a process of the lab no pid file names, then says so"

# 2. A process a pid file names is stopped, and its pid file goes.
mkdir -p "$STATE/run"
standin tracked
tracked=$LAST
echo "$tracked" >"$STATE/run/coordinator.pid"
sleep 1
lab down
[[ $RC -eq 0 ]] || fail "down failed with a process a pid file names: $OUT"
alive "$tracked" && fail "down left a process its pid file names running"
[[ ! -e "$STATE/run/coordinator.pid" ]] || fail "down left the pid file of a stopped process"
pass "down stops a process a pid file names and removes the pid file"

# 3. A process down cannot stop is listed, and down neither says the lab stopped nor succeeds. kill is made to do nothing,
#    the only way to stand in for a process that survives both signals.
standin stubborn
stubborn=$LAST
sleep 1
if OUT="$( (kill() { :; }; export -f kill; "$LAB" down) 2>&1)"; then RC=0; else RC=$?; fi
[[ $RC -ne 0 ]] || fail "down succeeded while a process of the lab could not be stopped"
[[ "$OUT" != *"Lab stopped"* ]] || fail "down said the lab stopped while a process of it was still running"
[[ "$OUT" == *"$stubborn"* ]] || fail "down did not name the process it could not stop: $OUT"
kill "$stubborn" 2>/dev/null || true
sleep 1
pass "down fails, naming the process, when one of the lab's cannot be stopped"

# 4. up refuses to start over a process of the lab no pid file names, before it builds anything.
standin leftover
leftover=$LAST
sleep 1
lab up
[[ $RC -ne 0 ]] || fail "up started over a process of the lab no pid file names"
[[ "$OUT" == *"no pid file names"* ]] || fail "up did not say why it refused: $OUT"
[[ ! -e "$STATE/logs" ]] || fail "up began to build before it refused"
pass "up refuses to start over a process of the lab no pid file names"
lab down
[[ $RC -eq 0 ]] || fail "down failed after up refused: $OUT"

# 5. status shows a process of the lab no pid file names.
standin untracked
untracked=$LAST
sleep 1
lab status
[[ "$OUT" == *"untracked  pid $untracked"* ]] || fail "status did not show the untracked process: $OUT"
pass "status shows a process of the lab no pid file names"
lab down

# 6. down never stops its own caller, whose command line may name a path under the state directory, nor the processes
#    above it.
bash -c '"$0" down >"$1.out" 2>&1; : >"$1.lived"' "$LAB" "$STATE/caller" || true
[[ -e "$STATE/caller.lived" ]] || fail "down stopped the process that called it: $(cat "$STATE/caller.out")"
[[ "$(cat "$STATE/caller.out")" == *"Lab stopped"* ]] || fail "down did not say the lab stopped when its caller named the state directory: $(cat "$STATE/caller.out")"
pass "down leaves its caller alone"

# 7. A child of a process a pid file names is the lab's, not a process no pid file accounts for.
python3 -c 'import subprocess, sys, time; subprocess.Popen([sys.executable, "-c", "import time; time.sleep(120)", sys.argv[1] + "-child"]); time.sleep(120)' "$STATE/parent" &
parent=$!
disown "$parent"
PIDS+=("$parent")
mkdir -p "$STATE/run"
echo "$parent" >"$STATE/run/coordinator.pid"
sleep 1
lab status
[[ "$OUT" != *"untracked"* ]] || fail "status called untracked a child of a process a pid file names: $OUT"
pass "a child of a tracked process is not reported as untracked"
lab down
[[ $RC -eq 0 ]] || fail "down failed with a tracked process and its child: $OUT"
[[ -z "$(ps -ww -eo args= | grep -F "$STATE/parent-child" | grep -v grep || true)" ]] || fail "down left the child of a tracked process running"

# 8. up refuses to start while something that is not the lab holds a port it needs.
if command -v lsof >/dev/null 2>&1; then
    free_port() { python3 -c 'import socket; s = socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1])'; }
    held="$(free_port)"
    other="$(free_port)"
    # The holder's command line does not name the state directory: it is not the lab's.
    python3 -c 'import socket, sys, time; s = socket.socket(); s.bind(("127.0.0.1", int(sys.argv[1]))); s.listen(); time.sleep(120)' "$held" &
    PIDS+=("$!")
    disown
    sleep 1
    LAB_PORT="$held" LAB_IDP_PORT="$other" lab up
    [[ $RC -ne 0 ]] || fail "up started while another process held the coordinator's port"
    [[ "$OUT" == *"Port $held is held by pid"* ]] || fail "up did not name the port and its holder: $OUT"
    pass "up refuses to start while another process holds its port"
else
    echo "  (lsof is not installed: the port check is not played)"
fi

echo
echo -e "${GREEN}Lab stop and start checks passed.${NC}"
