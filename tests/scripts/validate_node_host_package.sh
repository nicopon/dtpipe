#!/bin/bash
set -e

# validate_node_host_package.sh
# Packs DtPipe.PipelineNode.Hosting, the one package of the distributed family, and checks what a consumer gets: the assemblies
# of the three projects it bundles in lib/, the targets file under build/ and buildTransitive/, and a nuspec that depends on the
# packages the bundled projects use and on none of the bundled projects themselves. Nothing is published.
#
#   tests/scripts/validate_node_host_package.sh [--version <x.y.z[-suffix]>] [--out <dir>]
#
# The version defaults to Directory.Build.props' with a -local.<time> suffix, so a local build never takes the number of a release
# in the NuGet cache; --out defaults to dist/node-host, which is outside every pack glob of the release.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$PROJECT_ROOT"

GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'
pass() { echo -e "  ${GREEN}OK: $1${NC}"; }
fail() { echo -e "  ${RED}FAIL: $1${NC}"; exit 1; }

VERSION=""
OUT="$PROJECT_ROOT/dist/node-host"
while [ $# -gt 0 ]; do
    case "$1" in
        --version) VERSION="$2"; shift 2 ;;
        --out) OUT="$(cd "$2" 2>/dev/null && pwd || echo "$2")"; shift 2 ;;
        *) fail "unknown argument: $1" ;;
    esac
done
if [ -z "$VERSION" ]; then
    BASE=$(sed -n 's|.*<Version>\(.*\)</Version>.*|\1|p' Directory.Build.props | head -1)
    [ -n "$BASE" ] || fail "no version: pass --version"
    VERSION="$BASE-local.$(date +%s)"
fi

PROJECT=src/DtPipe.PipelineNode.Hosting/DtPipe.PipelineNode.Hosting.csproj
PACKAGE="$OUT/DtPipe.PipelineNode.Hosting.$VERSION.nupkg"

echo "========================================"
echo "  DtPipe node host package ($VERSION)"
echo "========================================"

mkdir -p "$OUT"
rm -f "$PACKAGE"
dotnet pack "$PROJECT" -c Release -o "$OUT" -p:PackageVersion="$VERSION" -nologo -v q >/dev/null || fail "dotnet pack failed"
[ -f "$PACKAGE" ] || fail "no package at $PACKAGE"
pass "packed $PACKAGE"

LIST=$(unzip -Z1 "$PACKAGE")
NUSPEC=$(unzip -p "$PACKAGE" DtPipe.PipelineNode.Hosting.nuspec)

for dll in DtPipe.PipelineNode.Hosting DtPipe.PipelineNode DtPipe.Coordinator.Contracts DtPipe.Jobs; do
    echo "$LIST" | grep -qx "lib/net10.0/$dll.dll" || fail "lib/net10.0/$dll.dll is not in the package"
done
pass "lib/net10.0 holds the host library and the three projects it bundles"

if echo "$LIST" | grep -E '^lib/' | grep -qiE 'TransportR|YamlDotNet|Apache\.Arrow'; then
    fail "a third-party assembly was bundled into lib/: it must be a dependency"
fi
pass "no third-party assembly in lib/"

for path in build/DtPipe.PipelineNode.Hosting.targets buildTransitive/DtPipe.PipelineNode.Hosting.targets; do
    echo "$LIST" | grep -qx "$path" || fail "$path is not in the package"
done
pass "the targets file ships under build/ and buildTransitive/"

for dep in TransportR TransportR.Client YamlDotNet Apache.Arrow Microsoft.AspNetCore.SignalR.Client; do
    echo "$NUSPEC" | grep -q "<dependency id=\"$dep\"" || fail "the nuspec does not depend on $dep"
done
pass "the nuspec depends on the packages the bundled projects use"

if echo "$NUSPEC" | grep -qE '<dependency id="DtPipe\.'; then
    fail "the nuspec depends on a DtPipe.* package: the three projects are bundled, never dependencies"
fi
pass "the nuspec depends on no DtPipe.* package"

echo ""
echo -e "${GREEN}Node host package checks passed.${NC} $PACKAGE"
