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
# When no args are provided, default to --filter '*'. The two tokens must stay separate
# so BenchmarkDotNet receives --filter and * as distinct arguments. A single quoted string
# "${@:---filter *}" collapses into one token, which the BDN parser rejects as unknown.
[ $# -eq 0 ] && set -- --filter '*'
dotnet run --project "$SCRIPT_DIR/tests/DtPipe.Benchmarks/" -c Release -- "$@"

echo ""
echo "=== Apache.Arrow.Serialization Benchmarks ==="
dotnet run --project "$SCRIPT_DIR/tests/Apache.Arrow.Serialization.Benchmarks/" -c Release
