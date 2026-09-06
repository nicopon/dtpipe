#!/bin/bash
set -e

# validate_vitals.sh
# Pre-flight baseline aggregator — runs every tests/scripts/validate_*.sh
# (glob, not a hard-coded list), prints a pass/fail summary table, and exits
# non-zero if any script fails.
#
# Usage:
#   ./tests/scripts/validate_vitals.sh                  # run everything
#   DTPIPE_VITALS_ONLY="validate_dag validate_sql" ./tests/scripts/validate_vitals.sh
#
# Scripts requiring live database containers (PG/SqlServer/Oracle/S3) need the
# persistent infra: ./tests/infra/start_infra.sh

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

DTPIPE="$PROJECT_ROOT/dist/release/dtpipe"

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo "========================================"
echo "    DtPipe Vitals — full E2E battery"
echo "========================================"

if [ ! -f "$DTPIPE" ]; then
    echo "Building release..."
    "$PROJECT_ROOT/build.sh" > /dev/null
fi

# Collect every validate_*.sh except this aggregator itself.
scripts=()
for f in "$SCRIPT_DIR"/validate_*.sh; do
    name="$(basename "$f" .sh)"
    [ "$name" = "validate_vitals" ] && continue
    scripts+=("$name")
done
IFS=$'\n' scripts=($(sort <<<"${scripts[*]}")); unset IFS

# Optional allowlist filter (space-separated base names).
if [ -n "${DTPIPE_VITALS_ONLY:-}" ]; then
    filtered=()
    for wanted in $DTPIPE_VITALS_ONLY; do
        for s in "${scripts[@]}"; do
            [ "$s" = "$wanted" ] && filtered+=("$s")
        done
    done
    scripts=("${filtered[@]}")
fi

declare -a results=()
failed=0

for name in "${scripts[@]}"; do
    printf '  %-34s ... ' "$name"
    log_file="$SCRIPT_DIR/artifacts/vitals_${name}.log"
    mkdir -p "$SCRIPT_DIR/artifacts"
    if bash "$SCRIPT_DIR/$name.sh" > "$log_file" 2>&1; then
        results+=("OK|$name")
        printf '%bOK%b\n' "$GREEN" "$NC"
    else
        results+=("FAIL|$name")
        failed=$((failed + 1))
        printf '%bFAIL%b (see %s)\n' "$RED" "$NC" "$log_file"
    fi
done

# Summary table
echo ""
echo "----------------------------------------"
printf '  %b%-10s%b  Script\n' "$NC" "" ""
for r in "${results[@]}"; do
    status="${r%%|*}"
    name="${r#*|}"
    if [ "$status" = "OK" ]; then
        printf '  %b%-10s%b  %s\n' "$GREEN" "PASS" "$NC" "$name"
    else
        printf '  %b%-10s%b  %s\n' "$RED" "FAIL" "$NC" "$name"
    fi
done
echo "----------------------------------------"
total=${#results[@]}
echo "  Total: $total · Failed: $failed"
echo ""

if [ "$failed" -gt 0 ]; then
    echo -e "${RED}Vitals FAILED ($failed/$total).${NC}"
    exit 1
fi
echo -e "${GREEN}Vitals passed ($total/$total).${NC}"
