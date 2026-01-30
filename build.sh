#!/bin/bash
# DtPipe Build Script
# Builds both Release and AOT binaries

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# Configure local build environment to avoid sandbox permission issues
export DOTNET_CLI_HOME="$SCRIPT_DIR/.dotnet_home"
export NUGET_PACKAGES="$SCRIPT_DIR/.nuget_packages"
export NUGET_HTTP_CACHE_PATH="$SCRIPT_DIR/.nuget_cache"
export NUGET_PLUGINS_CACHE_PATH="$SCRIPT_DIR/.nuget_plugins"
export NUGET_SCRATCH="$SCRIPT_DIR/.nuget_scratch"

# Create directories if they don't exist
mkdir -p "$DOTNET_CLI_HOME" "$NUGET_PACKAGES" "$NUGET_HTTP_CACHE_PATH" "$NUGET_PLUGINS_CACHE_PATH" "$NUGET_SCRATCH"

cd "$SCRIPT_DIR"

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${GREEN}DtPipe Build Script${NC}"
echo "========================"

# Detect platform
OS="$(uname -s)"
ARCH="$(uname -m)"
RID=""
EXT=""

case "$OS" in
    Darwin)
        if [[ "$ARCH" == "arm64" ]]; then
            RID="osx-arm64"
        else
            RID="osx-x64"
        fi
        ;;
    Linux)
        if [[ "$ARCH" == "aarch64" ]]; then
            RID="linux-arm64"
        else
            RID="linux-x64"
        fi
        ;;
    MINGW*|CYGWIN*|MSYS*)
        # Default to x64 for Windows bash unless explicitly arm64
        RID="win-x64" 
        EXT=".exe"
        ;;
    *)
        echo "Unsupported platform: $OS"
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

# ============================================================
# Run Tests
# ============================================================
echo ""
echo -e "${YELLOW}Running Tests...${NC}"
dotnet test tests/DtPipe.Tests/DtPipe.Tests.csproj -c Release --filter "FullyQualifiedName~.Unit."

echo ""
echo -e "${YELLOW}Building Release (single-file)...${NC}"
dotnet publish src/DtPipe/DtPipe.csproj -c Release \
    -r "$RID" \
    --self-contained true \
    -p:PublishSingleFile=true \
    -p:IncludeNativeLibrariesForSelfExtract=true \
    -p:EnableCompressionInSingleFile=true \
    -p:DebugType=none \
    -p:DebugSymbols=false \
    -o "$RELEASE_DIR"

# Rename to lowercase (standard unix convention)
if [ -f "$RELEASE_DIR/DtPipe$EXT" ]; then
    mv "$RELEASE_DIR/DtPipe$EXT" "$RELEASE_DIR/dtpipe$EXT"
fi

# ============================================================
# Summary
# ============================================================
echo ""
echo -e "${GREEN}Build complete!${NC}"
echo ""
echo "Release (single-file):"
ls -lh "$RELEASE_DIR/dtpipe$EXT"
echo ""
echo -e "${YELLOW}Usage:${NC}"
echo "  ./dist/release/dtpipe$EXT --help"
