param([string[]]$Filter = @("*"))

# ==============================================================================
# benchmarks.ps1 - Run all DtPipe benchmark suites.
# ==============================================================================
#
# Usage:
#   .\benchmarks.ps1                        # run both suites
#   .\benchmarks.ps1 -Filter "*Arrow*"      # BenchmarkDotNet filter (DtPipe.Benchmarks only)
#
# DtPipe.Benchmarks uses BenchmarkDotNet.
# Apache.Arrow.Serialization.Benchmarks is a standalone throughput comparison.
# ==============================================================================

$ErrorActionPreference = "Stop"
$ScriptDir = $PSScriptRoot

Write-Host "=== DtPipe Benchmarks (BenchmarkDotNet) ===" -ForegroundColor Cyan
dotnet run --project "$ScriptDir/tests/DtPipe.Benchmarks/" -c Release -- --filter $Filter

Write-Host ""
Write-Host "=== Apache.Arrow.Serialization Benchmarks ===" -ForegroundColor Cyan
dotnet run --project "$ScriptDir/tests/Apache.Arrow.Serialization.Benchmarks/" -c Release
