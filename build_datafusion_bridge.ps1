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

# Ensure cargo is available
$HasCargo = (Get-Command cargo -ErrorAction SilentlyContinue) -ne $null

if (!$HasCargo) {
    Write-Host "Error: cargo is not installed. Native bridge cannot be built." -ForegroundColor Red
    # Check if binaries already exist
    $ExistingDll = "src\DtPipe.Processors.DataFusion\target\$TargetDir\dtpipe_datafusion.dll"
    if (Test-Path $ExistingDll) {
        Write-Host "Found existing binary ($ExistingDll), proceeding with staging only..." -ForegroundColor Yellow
    } else {
        exit 1
    }
} else {
    # Build the Rust crate
    Set-Location "src\DtPipe.Processors.DataFusion"
    & cargo build --profile "$ProfileName" @TargetArgs
    if ($LASTEXITCODE -ne 0) {
        Write-Error "Cargo build failed"
        exit $LASTEXITCODE
    }
    Set-Location ..\..
}

# Detect Runtime Identifier (RID)
$Arch = $env:PROCESSOR_ARCHITECTURE
# Normalize Arch for non-Windows if needed (e.g. running pwsh on MacOS/Linux)
if ([string]::IsNullOrEmpty($Arch)) {
    $Arch = [System.Runtime.InteropServices.RuntimeInformation]::OSArchitecture.ToString()
}

$Rid = "win-x64"
if ($Arch -match "ARM64|Arm64") {
    if ($IsMacOS) { $Rid = "osx-arm64" }
    elseif ($IsLinux) { $Rid = "linux-arm64" }
    else { $Rid = "win-arm64" }
} else {
    if ($IsMacOS) { $Rid = "osx-x64" }
    elseif ($IsLinux) { $Rid = "linux-x64" }
    else { $Rid = "win-x64" }
}

Write-Host "Target RID: $Rid" -ForegroundColor Cyan

# Ensure destination directory exists and is clean
$DestDir = "src\DtPipe.Processors\runtimes\$Rid\native"
if (Test-Path $DestDir) {
    Write-Host "Cleaning destination directory $DestDir..." -ForegroundColor Yellow
    Remove-Item -Path "$DestDir\*" -Force -ErrorAction SilentlyContinue
} else {
    New-Item -ItemType Directory -Force -Path $DestDir | Out-Null
}

$SourceDir = "src\DtPipe.Processors.DataFusion\target\$TargetDir"

Write-Host "Copying compiled native libraries from $SourceDir to $DestDir..." -ForegroundColor Green

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
        Write-Host "  Copied: $File"
    }
}

Write-Host "DataFusion Bridge built and copied to $DestDir successfully." -ForegroundColor Green
