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

Write-Host "QueryDump Build Script (Windows)" -ForegroundColor Green
Write-Host "========================"

# Detect Platform and Architecture
$FullMac = $false
$FullLinux = $false
$FullWindows = $false

if ($PSVersionTable.PSVersion.Major -ge 6) {
    # PowerShell Core
    if ($IsMacOS) { $FullMac = $true }
    if ($IsLinux) { $FullLinux = $true }
    if ($IsWindows) { $FullWindows = $true }
} else {
    # Windows PowerShell (Desktop)
    $FullWindows = $true
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
dotnet test "tests\QueryDump.Tests\QueryDump.Tests.csproj" -c Release --filter "FullyQualifiedName~.Unit."

if ($LASTEXITCODE -ne 0) {
    Write-Error "Tests failed with exit code $LASTEXITCODE"
    exit $LASTEXITCODE
}

Write-Host ""
Write-Host "Building Release (single-file)..." -ForegroundColor Yellow

dotnet publish "src\QueryDump\QueryDump.csproj" -c Release `
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
# Visual Studio / dotnet usually produces "QueryDump.exe" (or no ext on unix). To match "querydump" preference:

$ExePath = Join-Path $ReleaseDir ("QueryDump" + $Ext)
$TargetExePath = Join-Path $ReleaseDir ("querydump" + $Ext)

if (Test-Path $ExePath) {
    Rename-Item -Path $ExePath -NewName ("querydump" + $Ext) -Force
}

# ============================================================
# Summary
# ============================================================
Write-Host ""
Write-Host "Build complete!" -ForegroundColor Green
Write-Host ""
Write-Host "Release (single-file):"
Get-ChildItem "$ReleaseDir\querydump$Ext" | Format-Table Name, Length
Write-Host ""
Write-Host "Usage:" -ForegroundColor Yellow
Write-Host "  .\dist\release\querydump$Ext --help"
