$ErrorActionPreference = "Stop"
# Build DtPipe with experimental features enabled (DataFusion native bridge).
# Equivalent to: $env:DTPIPE_EXPERIMENTAL=1; ./build.ps1
$env:DTPIPE_EXPERIMENTAL = "1"
& "$PSScriptRoot/build.ps1" @args
