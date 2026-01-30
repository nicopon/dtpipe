#!/bin/bash

echo "DtPipe README Examples Validation"
echo "===================================="

APP="./dist/release/dtpipe"

# Ensure app is built
if [ ! -f "$APP" ]; then
    echo "Error: $APP not found. Running build.sh..."
    ./build.sh
fi

function run_test() {
    local title=$1
    local cmd=$2
    echo -n "Testing: $title... "
    
    local TMP_DIR="/tmp/dtpipe_test"
    mkdir -p "$TMP_DIR"
    
    local final_cmd=$(echo "$cmd" | sed "s|users.parquet|$TMP_DIR/users.parquet|g" \
                                 | sed "s|source.db|:memory:|g" \
                                 | sed "s|customers.db|:memory:|g" \
                                 | sed "s|customers_anon.parquet|$TMP_DIR/out.parquet|g" \
                                 | sed "s|customers_anon.csv|$TMP_DIR/out.csv|g")
    
    local output
    if [[ "$final_cmd" == *"input"* ]]; then
        output=$(eval "$final_cmd" 2>&1)
    else
        output=$(eval "$APP --input \"duck::memory:\" --query \"SELECT 'Alice' as FIRSTNAME, 'Smith' as LASTNAME, 30 as AGE, '0612345678' as PHONE, 'TEMP' as STATUS, 'JS' as FULL_NAME, 'EMAIL' as EMAIL, 'INTERNAL_ID' as INTERNAL_ID\" --output \"$TMP_DIR/test.csv\" $final_cmd --dry-run" 2>&1)
    fi

    if [ $? -eq 0 ]; then
        echo "✅ PASS"
    else
        echo "❌ FAIL"
        echo "--- Command ---"
        echo "$final_cmd"
        echo "--- Output ---"
        echo "$output"
        exit 1
    fi
}

# 1. Quick Start
run_test "Quick Start" "$APP --input \"duck:source.db\" --query \"SELECT 1\" --output \"users.parquet\""

# 2. Iterate Workflow Examples
run_test "Iterative Workflow (Dry Run)" "$APP --input \"duck::memory:\" --query \"SELECT 1\" --output \"users.csv\" --sample-rate 0.1 --dry-run"
run_test "Iterative Workflow (Export)" "$APP --input \"duck::memory:\" --query \"SELECT 1\" --output \"users.parquet\" --fake \"NAME:name.fullName\" --export-job /tmp/job.yaml"

# 3. Transformer Basics
run_test "Nullify" "--null \"INTERNAL_ID\""
run_test "Overwrite" "--overwrite \"STATUS:anonymized\""
run_test "Format" "--format \"DISPLAY_NAME:{FIRSTNAME} {LASTNAME}\""

# 4. Advanced Transformers
run_test "Masking" "--mask \"EMAIL:###****\""
run_test "Scripting (Row based)" "--script \"FULL_NAME:return row.FIRSTNAME + ' ' + row.LASTNAME;\""
run_test "Scripting (Auto-return)" "--script \"NAME:row.FIRSTNAME.toUpperCase()\""

# 5. Anonymization
run_test "Anonymization (Basic)" "--fake \"NAME:name.fullName\" --fake \"EMAIL:internet.email\" --fake-locale fr"
run_test "Anonymization (Seeding)" "--fake \"NAME:name.fullName\" --fake-seed-column FIRSTNAME"

# 6. Filtering
run_test "Project" "--project \"FIRSTNAME,LASTNAME,EMAIL\""
run_test "Drop" "--drop \"INTERNAL_ID\""

# 7. YAML Validation
cat <<EOF > /tmp/readme_test_job.yaml
input: "duck::memory:"
query: "SELECT 'Alice' as name, 'test@example.com' as email, '0612345678' as phone, 1 as id"
output: "/tmp/customers_anon.parquet"
transformers:
  - null:
      mappings:
        phone: ~
  - fake: 
      mappings:
        name: name.fullName
        email: internet.email
      options:
        locale: fr
        seed-column: id
EOF

echo -n "Testing: YAML Job File... "
$APP --job /tmp/readme_test_job.yaml --dry-run > /dev/null 2>&1
if [ $? -eq 0 ]; then
    echo "✅ PASS"
else
    echo "❌ FAIL"
    exit 1
fi

echo "===================================="
echo "All README examples validated successfully!"
rm -rf /tmp/dtpipe_test
