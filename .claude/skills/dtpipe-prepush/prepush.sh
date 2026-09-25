#!/usr/bin/env bash
# Pre-push gate for the validators CI never runs. Full output goes to log files; stdout carries
# only verdicts and the tail of whatever failed.
#
# Usage: prepush.sh [--skip-build] [--skip-xml] [--all]
#   --skip-build  reuse dist/release/dtpipe (only when it was built from the current tree)
#   --skip-xml    leave out validate_xml (2.7 GB of scratch)
#   --all         run the whole validate_vitals battery instead of the CI-skipped subset
set -uo pipefail

# The failure extraction below matches English runner output; the host locale would translate it.
export DOTNET_CLI_UI_LANGUAGE=en VSLANG=1033
ROOT="$(git -C "$(dirname "$0")" rev-parse --show-toplevel)"
cd "$ROOT" || exit 1
LOG_DIR="tests/scripts/artifacts"
mkdir -p "$LOG_DIR"

SKIP_BUILD=false; SKIP_XML=false; ALL=false
for arg in "$@"; do
    case "$arg" in
        --skip-build) SKIP_BUILD=true ;;
        --skip-xml)   SKIP_XML=true ;;
        --all)        ALL=true ;;
        *) echo "Unknown option: $arg" >&2; exit 2 ;;
    esac
done

strip() { perl -pe 's/\e\[[0-9;]*[A-Za-z]//g'; }
failed=0

step() {
    local label="$1" log="$2"; shift 2
    printf '%-28s ' "$label"
    if "$@" > "$log" 2>&1; then
        echo "OK"
    else
        echo "FAIL (log: $log)"
        grep -E "error [A-Z]+[0-9]+|Failed |FAIL|Error:" "$log" | strip | sort -u | head -20
        echo "--- tail ---"; tail -n 15 "$log" | strip
        failed=$((failed + 1))
        return 1
    fi
}

step "commit trailers" "$LOG_DIR/prepush_trailers.log" bash tests/scripts/validate_commit_trailers.sh

if ! $SKIP_BUILD; then
    step "build.sh" "$LOG_DIR/prepush_build.log" ./build.sh || { echo "Build failed; validators not run."; exit 1; }
fi

step "infra" "$LOG_DIR/prepush_infra.log" tests/infra/start_infra.sh || { echo "Infra not up; validators not run."; exit 1; }

# Same rule build.yml uses to skip a script: sourcing lib/test_connections.sh declares a database.
if $ALL; then
    ONLY=""
else
    ONLY="$(grep -l 'lib/test_connections.sh' tests/scripts/validate_*.sh | xargs -n1 basename | sed 's/\.sh$//' | tr '\n' ' ')"
    $SKIP_XML || ONLY="$ONLY validate_xml"
fi

echo "--- validators ---"
DTPIPE_VITALS_ONLY="$ONLY" bash tests/scripts/validate_vitals.sh > "$LOG_DIR/prepush_vitals.log" 2>&1
vitals_status=$?
grep -E '^\s+validate_[a-z_]+ +\.\.\. ' "$LOG_DIR/prepush_vitals.log" | strip
for name in $(strip < "$LOG_DIR/prepush_vitals.log" | grep -E '\.\.\. +FAIL' | awk '{print $1}'); do
    echo "=== $name (log: $LOG_DIR/vitals_${name}.log) ==="
    tail -n 25 "$LOG_DIR/vitals_${name}.log" | strip
done
[ $vitals_status -ne 0 ] && failed=$((failed + 1))

echo "---"
if [ $failed -eq 0 ]; then echo "PREPUSH: PASS"; else echo "PREPUSH: FAIL ($failed step(s))"; exit 1; fi
