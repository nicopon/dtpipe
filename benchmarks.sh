#!/usr/bin/env bash
# ==============================================================================
# benchmarks.sh - Run all DtPipe benchmark suites.
# ==============================================================================
#
# Usage:
#   ./benchmarks.sh                     # run both suites
#   ./benchmarks.sh --filter "*Arrow*"  # BenchmarkDotNet filter (DtPipe.Benchmarks only)
#
# DtPipe.Benchmarks uses BenchmarkDotNet:
#   dotnet run -c Release -- --filter "*"
#
# Apache.Arrow.Serialization.Benchmarks is a standalone throughput comparison
# (Arrow vs JSON serialization) — no filter arguments needed.
# ==============================================================================

set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "=== DtPipe Benchmarks (BenchmarkDotNet) ==="
dotnet run --project "$SCRIPT_DIR/tests/DtPipe.Benchmarks/" -c Release -- "${@:---filter *}"

echo ""
echo "=== Apache.Arrow.Serialization Benchmarks ==="
dotnet run --project "$SCRIPT_DIR/tests/Apache.Arrow.Serialization.Benchmarks/" -c Release
