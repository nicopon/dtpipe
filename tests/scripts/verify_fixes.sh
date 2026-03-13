#!/bin/bash
# Comprehensive verification of fixes for T4, T7, T21, T26, T30

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DTPIPE="$SCRIPT_DIR/../../dist/release/dtpipe"

mkdir -p artifacts
cd "$SCRIPT_DIR"

run_test() {
    local id=$1
    local cmd=$2
    echo "----------------------------------------------------"
    echo "TEST $id: $cmd"
    echo "----------------------------------------------------"
    eval "DEBUG=1 $cmd"
    echo "RESULT: PASSED"
}

# T4: Columnar Masking (Reproduction of NRE if sharing columns)
run_test "T4" "$DTPIPE -i artifacts/test_data.csv --mask 'Email:####@####.com' -o artifacts/output_t4_verify.parquet"

# T7: Overwrite & Null (Reproduction of NRE)
run_test "T7" "$DTPIPE -i artifacts/test_data.parquet --null 'Category' --overwrite 'Price:0.0' -o artifacts/output_t7_verify.csv"

# T21 (DAG-ified): Using multiple global flags (-i, -x, -q, --main) to verify arity fix
run_test "T21" "$DTPIPE -i artifacts/test_data.csv --alias c -x fusion-engine --main c -q 'SELECT id FROM c' --alias stream1 -x fusion-engine --main c --ref stream1 -q 'SELECT * FROM c JOIN stream1 ON c.id = stream1.id' -o artifacts/output_t21_verify.csv"

# T26: CROSS JOIN DAG
run_test "T26" "$DTPIPE --main 'generate:100' --alias g1 --ref 'generate:50' --alias g2 -x fusion-engine -q 'SELECT g1.* FROM g1 CROSS JOIN g2' -o artifacts/output_t26_verify.parquet"

# T30: Fan-out DAG
run_test "T30" "$DTPIPE -i artifacts/test_data.csv --alias src --from src -o artifacts/output_t30_a_verify.parquet --from src -o artifacts/output_t30_b_verify.csv"

echo "===================================================="
echo "ALL VERIFICATION TESTS COMPLETED"
echo "===================================================="
