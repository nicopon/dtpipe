#!/bin/bash
set -e

# validate_core_boundary.sh
# F10 — source-level boundary guard: no concrete SQL/dialect/cursor-persistence classes
# in DtPipe.Core; standalone Arrow libraries stay DtPipe-free; the Arrow bridge inside Core
# sees only the three DtPipe types it needs.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

pass() { echo -e "  ${GREEN}OK: $1${NC}"; }
fail() { echo -e "  ${RED}FAIL: $1${NC}"; exit 1; }

echo "========================================"
echo "    DtPipe Core Boundary Validation"
echo "========================================"

# 1. Concrete SQL/dialect/cursor-store code in Core?
if grep -rlE "class (BaseSqlDataWriter|[A-Za-z]*SqlDialect|DatabaseRetryPolicy|CursorStateStore)\b" "$PROJECT_ROOT/src/DtPipe.Core/" --include="*.cs" 2>/dev/null | grep -v bin > /dev/null; then
    fail "concrete SQL/dialect/retry/cursor-store code found in DtPipe.Core"
fi
pass "no concrete SQL/dialect infrastructure in DtPipe.Core"

# 2. Standalone Arrow libraries untouched (no project refs into DtPipe.*, no using DtPipe.*).
if grep -rqE 'ProjectReference Include="[^"]*DtPipe' "$PROJECT_ROOT/src/Apache.Arrow.Serialization/" --include="*.csproj" 2>/dev/null \
   || grep -rq "using DtPipe\." "$PROJECT_ROOT/src/Apache.Arrow.Serialization/" --include="*.cs" 2>/dev/null; then
    fail "Apache.Arrow.Serialization references DtPipe"
fi
if grep -rqE 'ProjectReference Include="[^"]*DtPipe' "$PROJECT_ROOT/src/Apache.Arrow.Ado/" --include="*.csproj" 2>/dev/null \
   || grep -rq "using DtPipe\." "$PROJECT_ROOT/src/Apache.Arrow.Ado/" --include="*.cs" 2>/dev/null; then
    fail "Apache.Arrow.Ado references DtPipe"
fi
pass "standalone Arrow libraries remain DtPipe-free"

# 3. Temporal hygiene: nothing may invent a time zone for a zone-less DateTime.
#
# "new DateTimeOffset(dt)" resolves a Kind=Unspecified value against TimeZoneInfo.Local, putting
# the host's time zone into the data path and making the same input produce different output on
# different machines. The rule lives in exactly one place; this check keeps it there.
#
# Scope, stated honestly: this catches the constructor, not every way a bare DateTime can reach
# TimestampArray.Builder.Append — that hazard depends on the argument's static type and is not
# expressible as a grep (Date32/Date64 builders take DateTime legitimately, and share a file with
# the timestamp handler). tests/scripts/validate_temporal.sh is the net for that half: it runs the
# real binary under two TZ values and fails if the outputs differ.
TEMPORAL_RULE="Mapping/TemporalNormalization.cs"
DTO_HITS=$(grep -rn "new DateTimeOffset(" "$PROJECT_ROOT/src/" --include="*.cs" 2>/dev/null \
    | grep -v "/obj/" \
    | grep -v "$TEMPORAL_RULE" \
    | sed 's://.*::' \
    | grep "new DateTimeOffset(" || true)
if [ -n "$DTO_HITS" ]; then
    echo "$DTO_HITS"
    fail "new DateTimeOffset(...) outside $TEMPORAL_RULE — use TemporalNormalization.ToOffset"
fi
pass "time-zone resolution confined to TemporalNormalization"

# 4. The Arrow bridge keeps a narrow surface onto the rest of DtPipe.
#
# src/DtPipe.Core/Infrastructure/Arrow/ is generic Arrow code: the CLR<->Arrow map, the
# row<->columnar bridges, the ownership helpers. It is allowed to know exactly three DtPipe
# types — the schema record it converts, and the two bridge interfaces it implements. Anything
# else crossing into it means engine concerns are leaking into the conversion layer, and the
# conversion layer is the hottest path in the product.
#
# The allowlist is deliberately short and checked against every type DtPipe.Core declares, so a
# type added to Models or Abstractions is covered without editing this script.
#
# Scope, stated honestly: this matches type names, so a Core type whose name is also an ordinary
# word or an Apache.Arrow type name would false-positive. None of the 52 declared today collide;
# if one ever does, rename it or widen the allowlist deliberately rather than deleting the check.
ARROW_DIR="$PROJECT_ROOT/src/DtPipe.Core/Infrastructure/Arrow"
ALLOWED="PipeColumnInfo IRowToColumnarBridge IColumnarToRowBridge"

CORE_TYPES=$(grep -rhoE '^(public |internal )?(sealed |abstract |static )*(class|interface|record|struct|enum) +[A-Za-z_][A-Za-z0-9_]*' \
    "$PROJECT_ROOT/src/DtPipe.Core/Models/" "$PROJECT_ROOT/src/DtPipe.Core/Abstractions/" --include="*.cs" 2>/dev/null \
    | awk '{print $NF}' | sort -u)

if [ -z "$CORE_TYPES" ]; then
    fail "could not enumerate DtPipe.Core types — the boundary check would pass vacuously"
fi

LEAKS=""
for TYPE in $CORE_TYPES; do
    case " $ALLOWED " in *" $TYPE "*) continue ;; esac
    HIT=$(grep -rlw "$TYPE" "$ARROW_DIR" --include="*.cs" 2>/dev/null || true)
    if [ -n "$HIT" ]; then
        LEAKS="$LEAKS\n  $TYPE <- $(echo "$HIT" | tr '\n' ' ')"
    fi
done

if [ -n "$LEAKS" ]; then
    echo -e "$LEAKS"
    fail "DtPipe types beyond the allowlist reached Infrastructure/Arrow/ (allowed: $ALLOWED)"
fi
pass "Arrow bridge sees only $ALLOWED"

echo ""
echo -e "${GREEN}Core boundary checks passed.${NC}"
