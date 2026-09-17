#!/bin/bash
set -e

# validate_secret_redaction.sh
# A connection string reaches a message only through ConnectionStringSanitizer.Redact.
#
# Six sites sanitised and two did not, and nothing distinguished them: a resolved
# keyring alias — password included — was reprinted on stderr by the two routing
# failures, which is the one thing using a keyring was meant to prevent.
#
# Redact parses the string and shows only keys known to carry no credential; Sanitize
# only scans text that has no grammar, so handing it a connection is a silent
# downgrade. That is why the two are not interchangeable here, and why the weaker call
# is refused by name rather than merely preferred in prose.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

pass() { echo -e "  ${GREEN}OK: $1${NC}"; }
fail() { echo -e "  ${RED}FAIL: $1${NC}"; exit 1; }

echo "========================================"
echo "    DtPipe Secret Redaction Validation"
echo "========================================"

# Expressions that hold a connection string, by name.
# The \b on the property form matters: a bare "ConnectionString" also matches the
# sanitizer's own class name, and every call to it reports itself as an offender.
CONNECTION_EXPRESSIONS='job\.Input|job\.Output|branch\.Input|branch\.Output|b\.Input|b\.Output|j\.Input|w\.Output|connectionString|\.ConnectionString\b'

# Helpers allowed to stand in for a Redact call inside an interpolation. Each one is
# checked below to be defined over Redact, so renaming the body breaks the check
# rather than silently widening it.
ALLOWED_HELPERS='Safe'

# ----------------------------------------------------------------------------
# 1. No connection-valued expression interpolated outside a redaction.
#
#    Exempt, with reason:
#      Adapters/MemoryChannel/     the parameter is named connectionString by the router's
#                                  signature, but the value is a branch alias
#      Common/DuckSecretBuilder.cs builds the CREATE SECRET statement sent to DuckDB, which
#                                  must carry the real credential; its SecretSql.Redact
#                                  strips those values from anything quoted back to a user
# ----------------------------------------------------------------------------
OFFENDERS=""
while IFS= read -r line; do
    [ -n "$line" ] || continue
    file="${line%%:*}"
    case "$file" in
        */bin/*|*/obj/*) continue ;;
        */Adapters/MemoryChannel/*) continue ;;
        */Common/DuckSecretBuilder.cs) continue ;;
    esac
    # A named helper, and only a named one — any identifier would let a helper that
    # redacts nothing through, which is the loophole this check exists to close.
    if echo "$line" | grep -qE "\{($ALLOWED_HELPERS)\("; then
        continue
    fi
    OFFENDERS="${OFFENDERS}${line}"$'\n'
done <<< "$(grep -rnE "\\\$\"[^\"]*\{[^}]*(${CONNECTION_EXPRESSIONS})" \
    "$PROJECT_ROOT/src" --include="*.cs" 2>/dev/null \
    | grep -vE 'ConnectionStringSanitizer\.Redact\(' || true)"

if [ -n "$OFFENDERS" ]; then
    echo "$OFFENDERS"
    fail "a connection string is interpolated into a message without Redact"
fi
pass "every interpolated connection string goes through Redact"

# ----------------------------------------------------------------------------
# 2. Each allowed helper really is a Redact call.
# ----------------------------------------------------------------------------
IFS='|' read -ra HELPERS <<< "$ALLOWED_HELPERS"
for helper in "${HELPERS[@]}"; do
    DEFS=$(grep -rlE "(string|var) +${helper}\(" "$PROJECT_ROOT/src" --include="*.cs" 2>/dev/null \
        | grep -v "/bin/" | grep -v "/obj/" || true)
    [ -n "$DEFS" ] || fail "allowed helper '${helper}' is not defined anywhere"
    for def in $DEFS; do
        grep -A3 -E "(string|var) +${helper}\(" "$def" \
            | grep -q "ConnectionStringSanitizer\.Redact(" \
            || fail "allowed helper '${helper}' in ${def#$PROJECT_ROOT/} is not defined over Redact"
    done
done
pass "every helper the check lets through is defined over Redact"

# ----------------------------------------------------------------------------
# 3. Sanitize receives prose, and only prose it has been declared to receive.
#
#    Listing the connection-shaped names instead would only move the hole: the first
#    miss found here was `Sanitize(connection ?? "")` in CheckpointKey, whose parameter
#    is named `connection` and matched no pattern. So this is the fail-closed direction,
#    the same one Redact itself takes — an argument nobody declared is refused.
#
#    Adding a name here is the moment to ask whether the value really has no grammar.
#    If it is a connection string, the answer is Redact.
# ----------------------------------------------------------------------------
PROSE_ARGUMENTS='ex\.Message|reason|text|kv\.Value|report\.SchemaInspectionError|\$"'

UNDECLARED=""
while IFS= read -r line; do
    [ -n "$line" ] || continue
    file="${line%%:*}"
    case "$file" in
        */bin/*|*/obj/*|*/Security/ConnectionStringSanitizer.cs) continue ;;
    esac
    arg=$(echo "$line" | sed 's/.*ConnectionStringSanitizer\.Sanitize(//')
    echo "$arg" | grep -qE "^(${PROSE_ARGUMENTS})" || UNDECLARED="${UNDECLARED}${line}"$'\n'
done <<< "$(grep -rn 'ConnectionStringSanitizer\.Sanitize(' "$PROJECT_ROOT/src" --include="*.cs" 2>/dev/null || true)"

if [ -n "$UNDECLARED" ]; then
    echo "$UNDECLARED"
    fail "Sanitize receives an argument nobody declared as prose — if it is a connection, use Redact"
fi
pass "Sanitize receives only declared prose"

# ----------------------------------------------------------------------------
# 4. The word-boundary defect must not come back. \b breaks on neither '_' nor a
#    capital, so \b(secret)\b finds no boundary in s3_secret_access_key: four forms
#    the product builds itself passed through in the clear under that pattern.
# ----------------------------------------------------------------------------
if grep -qE '\\b\(password' "$PROJECT_ROOT/src/DtPipe.Core/Security/ConnectionStringSanitizer.cs"; then
    fail "the sensitive-key pattern is word-boundary anchored again (see ConnectionStringSanitizer)"
fi
pass "sensitive keys are matched without a word boundary"

echo ""
echo -e "${GREEN}Secret redaction validation passed${NC}"
