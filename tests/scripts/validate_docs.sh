#!/bin/bash

# validate_docs.sh
# Verifies that all CLI flags (starting with --) mentioned in README.md and COOKBOOK.md
# are actually registered in the dtpipe binary.

set -e

REPO_ROOT=$(pwd)
DOC_FILES=("$REPO_ROOT/README.md" "$REPO_ROOT/COOKBOOK.md")
DTPIPE_BIN="$REPO_ROOT/dist/release/dtpipe"

# 1. Build project if bin is missing
if [ ! -f "$DTPIPE_BIN" ]; then
    echo "Building dtpipe..."
    ./build.sh
fi

echo "Extracting help from binary..."
$DTPIPE_BIN --help > help_output.tmp 2>&1

FAILED=0

for doc in "${DOC_FILES[@]}"; do
    echo "Checking $doc..."
    
    # Extract flags starting with -- from the document
    # 1. Ignore anchor links like [#--anchor] or (#--anchor)
    # 2. Filter out Markdown separators (---)
    FLAGS=$(grep -oE '\-\-[a-z0-9\-]+' "$doc" | grep -vE "^(--|---)$" | sort -u)
    
    # Flags allowed even if not in --help (e.g. subcommands, dotnet tool flags)
    ALLOW_LIST="--project --version --help --fake-list --secrets --columnar-fast-path --compute-types --linux-pipes --migration"

    for flag in $FLAGS; do
        if [[ $ALLOW_LIST =~ "$flag" ]]; then
            continue
        fi
        
        if grep -qF -- "$flag" help_output.tmp; then
            # Found
            true 
        else
            echo "  [FAIL] Flag '$flag' found in $doc but not in 'dtpipe --help'"
            FAILED=1
        fi
    done
done

rm -f help_output.tmp

if [ $FAILED -eq 0 ]; then
    echo "All documented flags found in CLI help!"
    exit 0
else
    echo "Documentation contains invalid/undocumented flags."
    exit 1
fi
