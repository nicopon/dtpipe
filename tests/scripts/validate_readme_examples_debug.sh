#!/bin/bash
set -e

echo "QueryDump README Examples Validation (DEBUG)"
echo "=========================================="

APP="./dist/release/querydump"

function run_test() {
    local title=$1
    local cmd=$2
    echo "Testing: $title..."
    # Replace generic placeholders with working ones for DuckDB memory
    local final_cmd=$(echo "$cmd" | sed 's/duckdb:source.db/duckdb::memory:/g' \
                                 | sed 's/duckdb:customers.db/duckdb::memory:/g' \
                                 | sed 's/customers_anon.parquet/\/tmp\/out.parquet/g' \
                                 | sed 's/customers_anon.csv/\/tmp\/out.csv/g' \
                                 | sed 's/users.parquet/\/tmp\/users.parquet/g')
    
    if [[ "$final_cmd" == *"input"* ]]; then
        # It's a full command
        eval "$final_cmd"
    else
        # It's a fragment, add necessary base args
        eval "$APP --input \"duckdb::memory:\" --query \"SELECT 'Alice' as FIRSTNAME, 'Smith' as LASTNAME, 30 as AGE, '0612345678' as PHONE\" --output \"/tmp/test.csv\" $final_cmd --dry-run"
    fi

    if [ $? -eq 0 ]; then
        echo "✅ PASS"
    else
        echo "❌ FAIL"
        echo "Command: $final_cmd"
        exit 1
    fi
}

# 1. Quick Start
run_test "Quick Start" "$APP --input \"duckdb::memory:\" --query \"SELECT 1\" --output \"/tmp/users.parquet\""
