$ErrorActionPreference = "Stop"

$ScriptDir = $PSScriptRoot
Set-Location $ScriptDir

Write-Host "Building DtPipe.XStreamers.DataFusion (Rust Native Bridge) in Release mode..." -ForegroundColor Green

$TargetArgs = @()
$TargetDir = "release"
if ($env:RUST_TARGET) {
    $TargetArgs += "--target", $env:RUST_TARGET
    $TargetDir = "$env:RUST_TARGET\release"
}

# Build the Rust crate
Set-Location "src\DtPipe.XStreamers.DataFusion"
& cargo build --release @TargetArgs

if ($LASTEXITCODE -ne 0) {
    Write-Error "Cargo build failed"
    exit $LASTEXITCODE
}

Write-Host "Copying compiled native libraries to DtPipe.XStreamers\DataFusion\..." -ForegroundColor Green
Set-Location $ScriptDir

# Ensure destination directory exists
$DestDir = "src\DtPipe.XStreamers\DataFusion"
if (!(Test-Path $DestDir)) {
    New-Item -ItemType Directory -Force -Path $DestDir | Out-Null
}

$SourceDir = "src\DtPipe.XStreamers.DataFusion\target\$TargetDir"

# Copy output libraries if they exist
$FilesToCopy = @(
    "dtpipe_xstreamers_datafusion.dll",
    "libdtpipe_xstreamers_datafusion.so",
    "libdtpipe_xstreamers_datafusion.dylib"
)

foreach ($File in $FilesToCopy) {
    $SourcePath = Join-Path $SourceDir $File
    if (Test-Path $SourcePath) {
        Copy-Item -Path $SourcePath -Destination $DestDir -Force
    }
}

Write-Host "DataFusion Bridge built and copied successfully." -ForegroundColor Green
