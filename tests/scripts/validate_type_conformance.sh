#!/usr/bin/env bash
# validate_type_conformance.sh — the server half of the conformance table (pg, mysql, mssql, ora).
#
# The file readers and writers play their cells in the unit tests, and every writer's gate is answered
# there, without a server. What needs one lives in Integration/Conformance: a server's reader on both
# paths, and what a server gives back of a column it was handed. Same harness, same marks: a cell known
# false names the lot that corrects it, and turns red the day it reads right.
# Needs tests/infra running — sources lib/test_connections.sh.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib/test_connections.sh
source "$SCRIPT_DIR/lib/test_connections.sh"
ROOT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"

export DTPIPE_CONFORMANCE_PG="$PG"
export DTPIPE_CONFORMANCE_MYSQL="$MYSQL"
export DTPIPE_CONFORMANCE_MSSQL="$MSSQL"
export DTPIPE_CONFORMANCE_ORA="$ORA"

dotnet test "$ROOT_DIR/tests/DtPipe.Tests/DtPipe.Tests.csproj" \
    --filter "FullyQualifiedName~Integration.Conformance" \
    --verbosity quiet --nologo
