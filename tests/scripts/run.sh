#!/bin/bash
set -e

# run.sh — DtPipe test orchestrator
# Usage:
#   ./run.sh --smoke         Quick smoke test (golden scenarios, no Docker required)
#   ./run.sh --test          All unit-style validation scripts (no Docker)
#   ./run.sh --test-docker   All validation scripts including Docker-based drivers
#   ./run.sh --catalog       135-command catalog test suite (requires init_test_data.sh + Docker)
#   ./run.sh --bench         Performance benchmarks
#   ./run.sh --bench --sql   Benchmarks including SQL JOIN
#   ./run.sh --sql-features  SQL window functions + time-bucketing on both engines
#   ./run.sh --full          Everything: smoke + test-docker + catalog + bench + sql
#   ./run.sh --dag           DAG topology validation only
#   ./run.sh --help          Show this help

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

FAILED=()
PASSED=()
SKIPPED=()

show_help() {
    echo "Usage: $(basename "$0") [MODE] [OPTIONS]"
    echo ""
    echo "Modes:"
    echo "  --smoke          Golden smoke test (vicious edge cases, all DB drivers, Docker required)"
    echo "  --test           Transformers, schema, options, docs, DAG (no Docker)"
    echo "  --test-docker    All --test scripts + drivers (Docker required)"
    echo "  --catalog        135-command catalog suite (requires init_test_data.sh + Docker)"
    echo "  --bench          Performance benchmarks (linear pipeline, DuckDB)"
    echo "  --bench --sql    Benchmarks + DataFusion SQL JOIN"
    echo "  --sql-features   SQL window functions + time-bucketing on both engines (no Docker)"
    echo "  --full           smoke + test-docker + catalog + bench + sql + sql-features"
    echo "  --dag            DAG topology validation only"
    echo "  --xml            Streaming XML validation (2GB massive volume)"
    echo ""
    echo "Individual scripts can be run directly:"
    echo "  $SCRIPT_DIR/validate_transformers.sh"
    echo "  $SCRIPT_DIR/validate_schema.sh"
    echo "  $SCRIPT_DIR/validate_drivers.sh"
    echo "  $SCRIPT_DIR/validate_docs.sh"
    echo "  $SCRIPT_DIR/validate_options.sh"
    echo "  $SCRIPT_DIR/validate_dag.sh"
    echo "  $SCRIPT_DIR/validate_sql.sh"
    echo "  $SCRIPT_DIR/validate_hooks.sh"
    echo "  $SCRIPT_DIR/validate_xml.sh"
    echo "  $SCRIPT_DIR/smoke.sh"
    echo "  $SCRIPT_DIR/bench.sh [--sql] [--direct]"
    echo "  $SCRIPT_DIR/run_catalog_tests.sh  (after init_test_data.sh)"
    exit 0
}

run_script() {
    local name="$1"
    local script="$2"
    shift 2
    echo ""
    echo -e "${CYAN}${BOLD}==> $name${NC}"
    if bash "$script" "$@"; then
        PASSED+=("$name")
    else
        FAILED+=("$name")
        echo -e "${RED}FAILED: $name${NC}"
    fi
}

MODE_SMOKE=0
MODE_TEST=0
MODE_TEST_DOCKER=0
MODE_CATALOG=0
MODE_BENCH=0
MODE_BENCH_SQL=0
MODE_DAG=0
MODE_SQL_FEATURES=0
MODE_XML=0
MODE_FULL=0

if [ $# -eq 0 ]; then
    show_help
fi

for arg in "$@"; do
    case "$arg" in
        --smoke)        MODE_SMOKE=1 ;;
        --test)         MODE_TEST=1 ;;
        --test-docker)  MODE_TEST_DOCKER=1 ;;
        --catalog)      MODE_CATALOG=1 ;;
        --bench)        MODE_BENCH=1 ;;
        --sql)          MODE_BENCH_SQL=1 ;;
        --sql-features) MODE_SQL_FEATURES=1 ;;
        --dag)          MODE_DAG=1 ;;
        --xml)          MODE_XML=1 ;;
        --full)         MODE_FULL=1 ;;
        --help|-h)      show_help ;;
        *)              echo "Unknown option: $arg"; show_help ;;
    esac
done

if [ $MODE_FULL -eq 1 ]; then
    MODE_SMOKE=1; MODE_TEST=1; MODE_TEST_DOCKER=1; MODE_CATALOG=1; MODE_BENCH=1; MODE_BENCH_SQL=1; MODE_DAG=1; MODE_SQL_FEATURES=1; MODE_XML=1
fi

echo -e "${BOLD}DtPipe Test Runner${NC}"
echo "Project root: $PROJECT_ROOT"
echo "Script dir:   $SCRIPT_DIR"
echo ""

if [ $MODE_SMOKE -eq 1 ]; then
    run_script "Golden smoke test" "$SCRIPT_DIR/smoke.sh"
fi

if [ $MODE_SQL_FEATURES -eq 1 ] || [ $MODE_TEST -eq 1 ] || [ $MODE_TEST_DOCKER -eq 1 ]; then
    run_script "SQL features"  "$SCRIPT_DIR/validate_sql.sh"
fi

if [ $MODE_TEST -eq 1 ] || [ $MODE_TEST_DOCKER -eq 1 ]; then
    run_script "Transformers"   "$SCRIPT_DIR/validate_transformers.sh"
    run_script "Schema"         "$SCRIPT_DIR/validate_schema.sh"
    run_script "Options"        "$SCRIPT_DIR/validate_options.sh"
    run_script "Docs"           "$SCRIPT_DIR/validate_docs.sh"
    run_script "Hooks"          "$SCRIPT_DIR/validate_hooks.sh"
    run_script "XML Massive"    "$SCRIPT_DIR/validate_xml.sh"
fi

if [ $MODE_DAG -eq 1 ] || [ $MODE_TEST -eq 1 ] || [ $MODE_TEST_DOCKER -eq 1 ]; then
    run_script "DAG topologies" "$SCRIPT_DIR/validate_dag.sh"
fi

if [ $MODE_XML -eq 1 ]; then
    run_script "XML Massive"    "$SCRIPT_DIR/validate_xml.sh"
fi

if [ $MODE_TEST_DOCKER -eq 1 ]; then
    run_script "Drivers"        "$SCRIPT_DIR/validate_drivers.sh"
fi

if [ $MODE_CATALOG -eq 1 ]; then
    echo ""
    echo -e "${YELLOW}Note: --catalog requires init_test_data.sh to have been run first.${NC}"
    run_script "Catalog (135 commands)" "$SCRIPT_DIR/run_catalog_tests.sh"
fi

if [ $MODE_BENCH -eq 1 ]; then
    BENCH_ARGS=()
    [ $MODE_BENCH_SQL -eq 1 ] && BENCH_ARGS+=(--sql --direct)
    run_script "Benchmarks"     "$SCRIPT_DIR/bench.sh" "${BENCH_ARGS[@]}"
fi

# ----------------------------------------
# Summary
# ----------------------------------------
echo ""
echo "========================================"
echo -e "${BOLD}  Summary${NC}"
echo "========================================"

if [ ${#PASSED[@]} -gt 0 ]; then
    for s in "${PASSED[@]}"; do
        echo -e "${GREEN}  PASS  $s${NC}"
    done
fi

if [ ${#FAILED[@]} -gt 0 ]; then
    for s in "${FAILED[@]}"; do
        echo -e "${RED}  FAIL  $s${NC}"
    done
    echo ""
    echo -e "${RED}${BOLD}${#FAILED[@]} suite(s) failed.${NC}"
    exit 1
else
    echo ""
    echo -e "${GREEN}${BOLD}All ${#PASSED[@]} suite(s) passed.${NC}"
fi
