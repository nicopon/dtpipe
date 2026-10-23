#!/bin/bash
set -euo pipefail

# format_csharp.sh [--verify]
# Formats every tracked C# file as .editorconfig says (`dotnet format whitespace`). With --verify
# it changes nothing, names each line that is not formatted, and fails; build.sh and build.yml run
# that form.
#
# `dotnet format --folder` walks the whole folder it is given and aborts on a directory it cannot
# read, so it never gets the repository root: it gets the first two levels of the directory of each
# tracked C# file. tests/scripts holds artifacts/restricted, unreadable on purpose (T77 of
# run_catalog_tests.sh), so its C# is reached through tests/scripts/tools.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR/../.."

mode=""
case "${1:-}" in
    "") ;;
    --verify) mode="--verify-no-changes" ;;
    *) echo "usage: $0 [--verify]" >&2; exit 2 ;;
esac

roots="$(git ls-files -- '*.cs' \
    | sed -E 's#/[^/]*$##' | cut -d/ -f1-2 | sort -u \
    | sed 's#^tests/scripts$#tests/scripts/tools#')"

# The first dotnet command on a fresh machine sets the CLI up under a named mutex, and first
# commands started together crash on it (Linux runners): one root runs alone, the rest in parallel.
status=0
dotnet format whitespace "$(head -n 1 <<< "$roots")" --folder $mode -v q || status=1
tail -n +2 <<< "$roots" | tr '\n' '\0' \
    | xargs -0 -P "$(getconf _NPROCESSORS_ONLN)" -I{} \
        dotnet format whitespace {} --folder $mode -v q \
    || status=1

if [ "$status" -ne 0 ]; then
    echo "dotnet format failed; tests/scripts/format_csharp.sh formats what it names above" >&2
    exit 1
fi
