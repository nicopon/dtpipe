$ErrorActionPreference = "Stop"

$ScriptDir = $PSScriptRoot
Set-Location $ScriptDir

$ProfileName = $env:CARGO_BUILD_PROFILE
if (!$ProfileName) { $ProfileName = "release" }

Write-Host "Building DtPipe.Processors.DataFusion (Rust Native Bridge) with profile=$ProfileName..." -ForegroundColor Green

$TargetArgs = @()
$TargetDir = "$ProfileName"
if ($env:RUST_TARGET) {
    $TargetArgs += "--target", $env:RUST_TARGET
    $TargetDir = "$env:RUST_TARGET\$ProfileName"
}

# Build the Rust crate
Set-Location "src\DtPipe.Processors.DataFusion"
& cargo build --profile "$ProfileName" @TargetArgs

if ($LASTEXITCODE -ne 0) {
    Write-Error "Cargo build failed"
    exit $LASTEXITCODE
}

Write-Host "Copying compiled native libraries to DtPipe.Processors\DataFusion\..." -ForegroundColor Green
Set-Location $ScriptDir

# Ensure destination directory exists
$DestDir = "src\DtPipe.Processors\DataFusion"
if (!(Test-Path $DestDir)) {
    New-Item -ItemType Directory -Force -Path $DestDir | Out-Null
}

$SourceDir = "src\DtPipe.Processors.DataFusion\target\$TargetDir"

# Copy output libraries if they exist
$FilesToCopy = @(
    "dtpipe_datafusion.dll",
    "libdtpipe_datafusion.so",
    "libdtpipe_datafusion.dylib"
)

foreach ($File in $FilesToCopy) {
    $SourcePath = Join-Path $SourceDir $File
    if (Test-Path $SourcePath) {
        Copy-Item -Path $SourcePath -Destination $DestDir -Force
    }
}

Write-Host "DataFusion Bridge built and copied successfully." -ForegroundColor Green
