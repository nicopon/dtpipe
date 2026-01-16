#!/bin/bash
# QueryDump Build Script
# Builds both Release and AOT binaries

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${GREEN}QueryDump Build Script${NC}"
echo "========================"

# Detect platform
case "$(uname -s)" in
    Darwin)
        if [[ "$(uname -m)" == "arm64" ]]; then
            RID="osx-arm64"
        else
            RID="osx-x64"
        fi
        ;;
    Linux)
        if [[ "$(uname -m)" == "aarch64" ]]; then
            RID="linux-arm64"
        else
            RID="linux-x64"
        fi
        ;;
    *)
        echo "Unsupported platform"
        exit 1
        ;;
esac

echo -e "Platform: ${YELLOW}$RID${NC}"

# ============================================================
# Build Release (single-file self-contained)
# ============================================================
RELEASE_DIR="./dist/release"
rm -rf "$RELEASE_DIR"
mkdir -p "$RELEASE_DIR"

echo ""
echo -e "${YELLOW}Building Release (single-file)...${NC}"
dotnet publish src/QueryDump/QueryDump.csproj -c Release \
    -r "$RID" \
    --self-contained true \
    -p:PublishSingleFile=true \
    -p:IncludeNativeLibrariesForSelfExtract=true \
    -p:EnableCompressionInSingleFile=true \
    -p:DebugType=none \
    -p:DebugSymbols=false \
    -o "$RELEASE_DIR"

# Rename to lowercase (standard unix convention)
mv "$RELEASE_DIR/QueryDump" "$RELEASE_DIR/querydump"

# ============================================================
# Summary
# ============================================================
echo ""
echo -e "${GREEN}Build complete!${NC}"
echo ""
echo "Release (single-file):"
ls -lh "$RELEASE_DIR/querydump"
echo ""
echo -e "${YELLOW}Usage:${NC}"
echo "  ./dist/release/querydump --help"
