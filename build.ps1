$ErrorActionPreference = "Stop"

$ScriptDir = $PSScriptRoot
Write-Host "Script Directory: $ScriptDir"

# Configure local build environment to avoid permission issues or pollution
$env:DOTNET_CLI_HOME = Join-Path $ScriptDir ".dotnet_home"
$env:NUGET_PACKAGES = Join-Path $ScriptDir ".nuget_packages"
$env:NUGET_HTTP_CACHE_PATH = Join-Path $ScriptDir ".nuget_cache"
$env:NUGET_PLUGINS_CACHE_PATH = Join-Path $ScriptDir ".nuget_plugins"
$env:NUGET_SCRATCH = Join-Path $ScriptDir ".nuget_scratch"

# Create directories if they don't exist
New-Item -ItemType Directory -Force -Path $env:DOTNET_CLI_HOME | Out-Null
New-Item -ItemType Directory -Force -Path $env:NUGET_PACKAGES | Out-Null
New-Item -ItemType Directory -Force -Path $env:NUGET_HTTP_CACHE_PATH | Out-Null
New-Item -ItemType Directory -Force -Path $env:NUGET_PLUGINS_CACHE_PATH | Out-Null
New-Item -ItemType Directory -Force -Path $env:NUGET_SCRATCH | Out-Null

Write-Host "DtPipe Build Script (Windows)" -ForegroundColor Green
Write-Host "========================"

if ($PSVersionTable.PSVersion.Major -ge 6) {
    # PowerShell Core
    if ($IsMacOS) { $FullMac = $true }
    if ($IsLinux) { $FullLinux = $true }
}

$Arch = $env:PROCESSOR_ARCHITECTURE
# Normalize Arch for non-Windows if needed
if ([string]::IsNullOrEmpty($Arch)) {
    $Arch = [System.Runtime.InteropServices.RuntimeInformation]::OSArchitecture.ToString()
}

$Rid = "win-x64"
$Ext = ".exe"

if ($FullMac) {
    $Ext = ""
    if ($Arch -match "ARM64|Arm64") { $Rid = "osx-arm64" } else { $Rid = "osx-x64" }
}
elseif ($FullLinux) {
    $Ext = ""
    if ($Arch -match "ARM64|Arm64") { $Rid = "linux-arm64" } else { $Rid = "linux-x64" }
}
else {
    $Ext = ".exe"
    if ($Arch -match "ARM64|Arm64") { $Rid = "win-arm64" } else { $Rid = "win-x64" }
}

Write-Host "Platform: $Rid" -ForegroundColor Yellow

# ============================================================
# Build Release (single-file self-contained)
# ============================================================
$ReleaseDir = Join-Path $ScriptDir "dist\release"

if (Test-Path $ReleaseDir) {
    Remove-Item -Recurse -Force $ReleaseDir
}
New-Item -ItemType Directory -Force -Path $ReleaseDir | Out-Null

# ============================================================
# Run Tests
# ============================================================
Write-Host ""
Write-Host "Running Tests..." -ForegroundColor Yellow
dotnet test "tests\DtPipe.Tests\DtPipe.Tests.csproj" -c Release --filter "FullyQualifiedName~.Unit."

if ($LASTEXITCODE -ne 0) {
    Write-Error "Tests failed with exit code $LASTEXITCODE"
    exit $LASTEXITCODE
}

if ($env:DTPIPE_EXPERIMENTAL -eq "1") {
    Write-Host ""
    Write-Host "Building DataFusion native bridge (DTPIPE_EXPERIMENTAL=1)..." -ForegroundColor Yellow
    $env:CARGO_BUILD_PROFILE = "release"
    & "$ScriptDir\build_datafusion_bridge.ps1"
    if ($LASTEXITCODE -ne 0) {
        Write-Error "DataFusion bridge build failed."
        exit $LASTEXITCODE
    }
} else {
    Write-Host ""
    Write-Host "Skipping DataFusion native bridge (experimental feature)." -ForegroundColor Yellow
    Write-Host "  Set `$env:DTPIPE_EXPERIMENTAL = '1' then run build.ps1 to enable it."
}

Write-Host ""
Write-Host "Performing a clean full rebuild..." -ForegroundColor Yellow
dotnet clean "DtPipe.sln" -c Release
dotnet build "DtPipe.sln" -c Release

Write-Host ""
Write-Host "Building Release (single-file)..." -ForegroundColor Yellow

dotnet publish "src\DtPipe\DtPipe.csproj" -c Release `
    -r $Rid `
    --self-contained true `
    -p:PublishSingleFile=true `
    -p:IncludeNativeLibrariesForSelfExtract=true `
    -p:EnableCompressionInSingleFile=true `
    -p:DebugType=none `
    -p:DebugSymbols=false `
    -o $ReleaseDir

if ($LASTEXITCODE -ne 0) {
    Write-Error "Build failed with exit code $LASTEXITCODE"
    exit $LASTEXITCODE
}

# Ensure lowercase name (optional on Windows case-insensitive fs, but good for consistency)
# Creating a copy/rename explicitly if needed, but output name from dotnet is usually standard.
# Visual Studio / dotnet usually produces "DtPipe.exe" (or no ext on unix). To match "dtpipe" preference:

$ExePath = Join-Path $ReleaseDir ("DtPipe" + $Ext)

if (Test-Path $ExePath) {
    Rename-Item -Path $ExePath -NewName ("dtpipe" + $Ext) -Force
}

Write-Host "----------------------------------------" -ForegroundColor Cyan
Write-Host "  Verifying binary integrity..." -ForegroundColor Cyan
Write-Host "----------------------------------------" -ForegroundColor Cyan
& (Join-Path $ReleaseDir ("dtpipe" + $Ext)) --help | Out-Null
if ($LASTEXITCODE -eq 0) {
    Write-Host "  OK: Binary is healthy." -ForegroundColor Green
} else {
    Write-Error "  FAILED: Binary sanity check failed."
    exit 1
}

# ============================================================
# Summary
# ============================================================
Write-Host ""
Write-Host "Build complete!" -ForegroundColor Green
Write-Host ""
Write-Host "Release (single-file):"
Get-ChildItem "$ReleaseDir\dtpipe$Ext" | Format-Table Name, Length

# ── Write dev env file ────────────────────────────────────────────
$DtpipeBinPath = Join-Path $ReleaseDir "dtpipe$Ext"
Set-Content -Path (Join-Path $ReleaseDir ".env") -Value "`$env:DTPIPE_BIN = '$DtpipeBinPath'"

# ── Profile Detection ───────────────────────────────────────────
$ProfilePath = $PROFILE
if (-not $ProfilePath) {
    if ($IsWindows) {
        $ProfilePath = Join-Path $env:USERPROFILE "Documents\WindowsPowerShell\Microsoft.PowerShell_profile.ps1"
    } else {
        $ProfilePath = Join-Path $home ".config\powershell\Microsoft.PowerShell_profile.ps1"
    }
}

Write-Host ""
Write-Host "Dev tip (autocompletion):" -ForegroundColor Yellow
Write-Host "  1. " -ForegroundColor Green -NoNewline; Write-Host "Run: `$env:DTPIPE_BIN = '$DtpipeBinPath'"
Write-Host "  2. " -ForegroundColor Green -NoNewline; Write-Host "Run: & `$env:DTPIPE_BIN completion --install"
Write-Host "  3. " -ForegroundColor Green -NoNewline; Write-Host "Run: . $ProfilePath"
Write-Host ""
Write-Host "   (Restart your terminal for changes to take effect if not reloaded)"

Write-Host ""
Write-Host "Usage:" -ForegroundColor Yellow
Write-Host "  .\dist\release\dtpipe$Ext --help"
