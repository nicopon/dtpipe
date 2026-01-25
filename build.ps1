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

# Detect Architecture
$Arch = $env:PROCESSOR_ARCHITECTURE
$Rid = "win-x64"

if ($Arch -eq "ARM64") {
    $Rid = "win-arm64"
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
# Visual Studio / dotnet usually produces "QueryDump.exe". To match "querydump.exe" preference:

$ExePath = Join-Path $ReleaseDir "QueryDump.exe"
$TargetExePath = Join-Path $ReleaseDir "querydump.exe"

if (Test-Path $ExePath) {
    Rename-Item -Path $ExePath -NewName "querydump.exe" -Force
}

# ============================================================
# Summary
# ============================================================
Write-Host ""
Write-Host "Build complete!" -ForegroundColor Green
Write-Host ""
Write-Host "Release (single-file):"
Get-ChildItem "$ReleaseDir\querydump.exe" | Format-Table Name, Length
Write-Host ""
Write-Host "Usage:" -ForegroundColor Yellow
Write-Host "  .\dist\release\querydump.exe --help"
