#!/bin/bash
# clean_test_data.sh - Remove all generated test artifacts.
# Run this before init_test_data.sh to force a full regeneration.

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ARTIFACTS_DIR="$SCRIPT_DIR/artifacts"

YELLOW='\033[1;33m'
GREEN='\033[0;32m'
NC='\033[0m'

if [ ! -d "$ARTIFACTS_DIR" ]; then
    echo "Nothing to clean: $ARTIFACTS_DIR does not exist."
    exit 0
fi

echo -e "${YELLOW}Cleaning test artifacts in $ARTIFACTS_DIR...${NC}"

# Restore permissions on restricted/ before deletion so rm -rf can remove it
if [ -d "$ARTIFACTS_DIR/restricted" ]; then
    chmod 755 "$ARTIFACTS_DIR/restricted"
fi

rm -rf "${ARTIFACTS_DIR:?}"/*

echo -e "${GREEN}Artifacts directory cleaned.${NC}"
