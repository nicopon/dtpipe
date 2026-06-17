#!/bin/bash

# validate_keyring.sh
# Validates the fake keyring and secret resolution functionality.

SCRIPT_DIR=$(dirname "$0")
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Detect binary (checks dist release first, otherwise falls back to dotnet DLL run)
if [ -f "$PROJECT_ROOT/dist/release/dtpipe" ]; then
    DTPIPE="$PROJECT_ROOT/dist/release/dtpipe"
elif [ -f "$PROJECT_ROOT/dist/release/dtpipe.exe" ]; then
    DTPIPE="$PROJECT_ROOT/dist/release/dtpipe.exe"
else
    DTPIPE="dotnet $PROJECT_ROOT/src/DtPipe/bin/Debug/net10.0/DtPipe.dll"
fi

# Determine fake keyring file location depending on OS
if [[ "$OSTYPE" == "darwin"* ]]; then
    FAKE_KEYRING_FILE="$HOME/Library/Application Support/dtpipe/fake_keyring.json"
elif [ -n "$APPDATA" ]; then
    # Windows
    FAKE_KEYRING_FILE="$APPDATA/dtpipe/fake_keyring.json"
else
    # Linux / other Unix
    FAKE_KEYRING_FILE="$HOME/.config/dtpipe/fake_keyring.json"
fi

echo "=== Keyring & Secret Resolution Validation ==="

# Ensure clean slate
rm -f "$FAKE_KEYRING_FILE"

# Activate unsafe fake keyring mode
export DTPIPE_UNSAFE_INSECURE_FAKE_KEYRING=true

# 1. Test Secret Storage
echo "Storing test secret..."
$DTPIPE secret set test-secret-alias "generate:count=5"

if [ ! -f "$FAKE_KEYRING_FILE" ]; then
    echo "❌ Error: Fake keyring file was not created at expected path: $FAKE_KEYRING_FILE"
    exit 1
fi
echo "✅ Secret file successfully created."

# Verify Unix file permissions if on macOS/Linux
if [[ "$OSTYPE" != "msys" && "$OSTYPE" != "cygwin" && "$OSTYPE" != "win32" ]]; then
    PERMISSIONS=$(stat -f "%A" "$FAKE_KEYRING_FILE" 2>/dev/null || stat -c "%a" "$FAKE_KEYRING_FILE")
    # We expect 600 (read/write owner only)
    if [ "$PERMISSIONS" != "600" ]; then
        echo "⚠️ Warning: File permissions are $PERMISSIONS, expected 600."
    else
        echo "✅ Secure file permissions verified (600)."
    fi
fi

# 2. Test Secret Retrieval
echo "Retrieving test secret..."
SECRET_VAL=$($DTPIPE secret get test-secret-alias | tail -n 1)
if [ "$SECRET_VAL" != "generate:count=5" ]; then
    echo "❌ Error: Retrieved secret value '$SECRET_VAL' does not match expected."
    exit 1
fi
echo "✅ Secret successfully retrieved."

# 3. Test Pipeline Connection String Resolution
echo "Running E2E pipeline utilizing keyring connection string..."
$DTPIPE -i keyring://test-secret-alias -o null:-
if [ $? -ne 0 ]; then
    echo "❌ Error: Running pipeline with keyring:// resolution failed."
    exit 1
fi
echo "✅ Pipeline resolved connection string and ran successfully."

# 3.b Test Job YAML Secret Interpolation
echo "Running job pipeline utilizing keyring secret in YAML..."
TEMP_JOB_YAML=$(mktemp)
cat > "$TEMP_JOB_YAML" <<EOF
test-job:
  input: \${{keyring://test-secret-alias}}
  output: null:-
EOF

$DTPIPE --job "$TEMP_JOB_YAML"
if [ $? -ne 0 ]; then
    echo "❌ Error: Running --job with keyring resolution failed."
    rm -f "$TEMP_JOB_YAML"
    exit 1
fi
rm -f "$TEMP_JOB_YAML"
echo "✅ Job pipeline resolved secret and ran successfully."


# 4. Test Secrets Listing
echo "Listing secrets..."
LIST_OUTPUT=$($DTPIPE secret list 2>&1)
if ! echo "$LIST_OUTPUT" | grep -q "test-secret-alias"; then
    echo "❌ Error: test-secret-alias not listed."
    exit 1
fi
echo "✅ Secret successfully listed."

# 5. Test Secret Deletion
echo "Deleting secret..."
$DTPIPE secret delete test-secret-alias
LIST_OUTPUT_AFTER_DELETE=$($DTPIPE secret list 2>&1)
if echo "$LIST_OUTPUT_AFTER_DELETE" | grep -q "test-secret-alias"; then
    echo "❌ Error: test-secret-alias was not deleted."
    exit 1
fi
echo "✅ Secret successfully deleted."

# Clean up
rm -f "$FAKE_KEYRING_FILE"
unset DTPIPE_UNSAFE_INSECURE_FAKE_KEYRING

echo "=== Keyring Validation Passed Successfully ==="
exit 0
