#!/bin/bash

# DtPipe Master Test Runner
# Automatically discovers and runs all .sh files in this directory except itself and common.sh

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ME="$(basename "${BASH_SOURCE[0]}")"

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

echo "===================================================="
echo "          DtPipe Full Integration Suite"
echo "===================================================="

PASSED=0
FAILED=0
FAILED_SCRIPTS=()

# Gather all scripts
# We use find to be more robust, and sort alphabetically
SCRIPTS=$(find "$SCRIPT_DIR" -maxdepth 1 -name "*.sh" ! -name "$ME" ! -name "common.sh" | sort)

for script in $SCRIPTS; do
    script_name=$(basename "$script")
    echo -e "\n${CYAN}▶ Running: $script_name${NC}"
    echo "----------------------------------------------------"
    
    # Run from the project root (most scripts expect this or handle it)
    # But to be safe, we'll run from the script directory's parent (project root)
    (cd "$SCRIPT_DIR/../.." && bash "tests/scripts/$script_name")
    
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✓ $script_name PASSED${NC}"
        PASSED=$((PASSED + 1))
    else
        echo -e "${RED}✗ $script_name FAILED${NC}"
        FAILED=$((FAILED + 1))
        FAILED_SCRIPTS+=("$script_name")
    fi
    echo "----------------------------------------------------"
done

echo -e "\n===================================================="
echo "                  TEST SUMMARY"
echo "===================================================="
echo -e "Total Scripts: $((PASSED + FAILED))"
echo -e "Passed:        ${GREEN}$PASSED${NC}"
echo -e "Failed:        ${RED}$FAILED${NC}"

if [ $FAILED -gt 0 ]; then
    echo -e "\n${RED}Failed Scripts:${NC}"
    for s in "${FAILED_SCRIPTS[@]}"; do
        echo -e " - $s"
    done
    exit 1
else
    echo -e "\n${GREEN}All integration tests passed successfully!${NC}"
    exit 0
fi
