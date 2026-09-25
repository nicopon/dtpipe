#!/usr/bin/env bash
# Targeted dotnet test with the output reduced to build errors, failed tests and the summary.
#
# Usage: run_tests.sh [--integration] [--project PATH] (--changed | FILTER)
#   FILTER         a dotnet test filter, e.g. "FullyQualifiedName~PipelineLexerTests"
#   --changed      derive the filter from the .cs files changed against HEAD (tracked + untracked):
#                  each Foo.cs whose FooTests.cs exists under tests/ contributes FooTests
#   --integration  run through ./test_local.sh (starts the persistent containers first)
#   --project      test project (default tests/DtPipe.Tests/DtPipe.Tests.csproj)
set -uo pipefail

ROOT="$(git -C "$(dirname "$0")" rev-parse --show-toplevel)"
cd "$ROOT" || exit 1
LOG_DIR="tests/scripts/artifacts"
mkdir -p "$LOG_DIR"
LOG="$LOG_DIR/dtpipe_test.log"

PROJECT="tests/DtPipe.Tests/DtPipe.Tests.csproj"
INTEGRATION=false; CHANGED=false; FILTER=""
while [ $# -gt 0 ]; do
    case "$1" in
        --integration) INTEGRATION=true; shift ;;
        --changed)     CHANGED=true; shift ;;
        --project)     PROJECT="$2"; shift 2 ;;
        -*) echo "Unknown option: $1" >&2; exit 2 ;;
        *)  FILTER="$1"; shift ;;
    esac
done

if $CHANGED; then
    classes=""
    for f in $( { git diff --name-only HEAD; git ls-files --others --exclude-standard; } | grep -E '\.cs$' | sort -u); do
        name="$(basename "$f" .cs)"
        case "$name" in *Tests) classes="$classes $name"; continue ;; esac
        if git ls-files --cached --others --exclude-standard -- "tests/*${name}Tests.cs" | grep -q .; then
            classes="$classes ${name}Tests"
        fi
    done
    classes="$(echo "$classes" | tr ' ' '\n' | grep . | sort -u)"
    if [ -z "$classes" ]; then
        echo "No test class matches the changed files. Pass a FILTER, e.g. \"FullyQualifiedName~.Unit.\"."
        exit 2
    fi
    FILTER="$(echo "$classes" | sed 's/^/FullyQualifiedName~/' | paste -sd'|' -)"
fi

[ -z "$FILTER" ] && { echo "A FILTER or --changed is required." >&2; exit 2; }
echo "Filter: $FILTER"

# The parsing below matches English runner output; the host locale would translate it.
export DOTNET_CLI_UI_LANGUAGE=en VSLANG=1033
ARGS=("$PROJECT" --filter "$FILTER" --nologo -v q --logger "console;verbosity=normal")
if $INTEGRATION; then
    ./test_local.sh "${ARGS[@]}" > "$LOG" 2>&1
else
    dotnet test "${ARGS[@]}" > "$LOG" 2>&1
fi
status=$?

strip() { perl -pe 's/\e\[[0-9;]*[A-Za-z]//g'; }

grep -E ': error [A-Z]+[0-9]+' "$LOG" | strip | sed -E 's/ \[[^]]*\.csproj[^]]*\]$//' | sort -u | head -20

# A failed test prints "Failed <name> [time]", then its message and stack. Keep the message and the
# first frames that point into this repository.
awk '
    /^[[:space:]]+Failed [^!]/ { show = 1; frames = 0; print; next }
    show && /^[[:space:]]+(Passed|Skipped|Failed) |^Test Run / { show = 0 }
    show && /^[[:space:]]+at / { if (frames < 3 && $0 ~ /DtPipe|Apache\.Arrow\.(Ado|Serialization)/) { print; frames++ } ; next }
    show && /^[[:space:]]*$/ { next }
    show { print }
' "$LOG" | strip | head -120

grep -E '^(Passed|Failed)!|Total tests|^[[:space:]]+(Passed|Failed|Skipped): |Test Run (Successful|Failed)|No test matches' "$LOG" | strip | tail -6
echo "Exit: $status (full log: $LOG)"
exit $status
