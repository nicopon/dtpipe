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
        if [[ "$ARCH" == "arm64" || "$ARCH" == "aarch64" ]]; then
            RID="win-arm64"
        else
            RID="win-x64"
        fi
        EXT=".exe"
        ;;
    *)
        echo "Unsupported platform: $OS"
        exit 1
        ;;
esac

echo -e "Platform: ${YELLOW}$RID${NC}"

echo ""
echo -e "${YELLOW}Checking C# formatting...${NC}"
bash tests/scripts/format_csharp.sh --verify

# ============================================================
# Build Release (single-file self-contained)
# ============================================================
RELEASE_DIR="./dist/release"
rm -rf "$RELEASE_DIR"
mkdir -p "$RELEASE_DIR"

# Published before the tests below: tests/DtPipe.PipelineNode.Tests spawns this binary, and every
# discovered test project runs on a tree where dist/release/ was just removed above.
echo ""
echo -e "${YELLOW}Building Release (single-file)...${NC}"
dotnet restore src/DtPipe/DtPipe.csproj -r "$RID"
dotnet publish src/DtPipe/DtPipe.csproj -c Release \
    -r "$RID" \
    --self-contained true \
    -p:PublishSingleFile=true \
    -p:DebugType=none \
    -p:DebugSymbols=false \
    -o "$RELEASE_DIR"

# Rename to lowercase (standard unix convention)
if [ -f "$RELEASE_DIR/DtPipe$EXT" ]; then
    mv "$RELEASE_DIR/DtPipe$EXT" "$RELEASE_DIR/dtpipe$EXT"
fi
chmod +x "$RELEASE_DIR/dtpipe$EXT"

echo "----------------------------------------"
echo "  Verifying binary integrity..."
echo "----------------------------------------"
"$RELEASE_DIR/dtpipe$EXT" --help > /dev/null
[ $? -eq 0 ] && echo "  OK: Binary is healthy." || { echo "  FAILED: Binary sanity check failed."; exit 1; }

# ============================================================
# Build the coordinator (self-contained folder)
# ============================================================
# The coordinator is a folder, not a single file: its pages (wwwroot) and the native SQLite library
# sit beside the executable. Its library is a git repository, so the machine it runs on needs git.
COORDINATOR_DIR="$RELEASE_DIR/coordinator"
COORDINATOR_EXE="$COORDINATOR_DIR/dtpipe-coordinator$EXT"

# Starts the published coordinator on a port Kestrel picks (port 0: no port is chosen here, so none
# can be taken meanwhile), with a throwaway state, and waits for the anonymous /livez. Runs in a
# subshell whose EXIT trap always stops the process and removes the state.
smoke_coordinator() (
    dir="$1"
    exe="$2"
    log="$(mktemp)"
    state="$(mktemp -d)"
    pid=""
    cleanup() {
        set +e
        if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
            kill "$pid" 2>/dev/null
            for _ in 1 2 3 4 5 6 7 8 9 10; do kill -0 "$pid" 2>/dev/null || break; sleep 1; done
            kill -9 "$pid" 2>/dev/null
        fi
        rm -rf "$state" "$log"
    }
    trap cleanup EXIT
    fail() {
        echo "  FAILED: $1"
        if [ -s "$log" ]; then
            echo "  --- the coordinator's log ---"
            if [ "$(wc -l < "$log")" -gt 24 ]; then head -n 12 "$log"; echo "  ..."; tail -n 12 "$log"; else cat "$log"; fi
        fi
        exit 1
    }

    [ -f "$exe" ] || fail "$exe was not published."
    [ -f "$dir/wwwroot/index.html" ] || fail "$dir/wwwroot is missing: the coordinator would answer without its pages."
    { compgen -G "$dir/libe_sqlite3.*" || compgen -G "$dir/e_sqlite3.dll"; } > /dev/null || fail "the native SQLite library (e_sqlite3) is missing from $dir: the coordinator cannot open its state."
    command -v git > /dev/null || fail "git is not on the PATH: the coordinator keeps its library as a git repository and needs it."

    state_arg="$state"
    if command -v cygpath > /dev/null; then state_arg="$(cygpath -w "$state")"; fi
    Coordinator__StateDirectory="$state_arg" \
    Coordinator__Initialize=true \
    Coordinator__PublicUrl=http://127.0.0.1:1 \
    Coordinator__Auth__AllowHttpKeys=true \
    Coordinator__Bootstrap__AdminKey=dtp_smoke_0123456789abcdefghijklmnopqrstuvwxyzABCDEFG \
    ASPNETCORE_URLS=http://127.0.0.1:0 \
        "$exe" > "$log" 2>&1 &
    pid=$!

    for _ in $(seq 1 30); do
        kill -0 "$pid" 2>/dev/null || fail "the coordinator stopped before it answered."
        port="$(sed -n 's|.*Now listening on: http://127\.0\.0\.1:\([0-9][0-9]*\).*|\1|p' "$log" | head -n 1)"
        if [ -n "$port" ] && curl -fs -o /dev/null "http://127.0.0.1:$port/livez"; then
            echo "  OK: the coordinator answers /livez."
            exit 0
        fi
        sleep 1
    done
    fail "the coordinator did not answer /livez within 30 seconds."
)

echo ""
echo -e "${YELLOW}Building the coordinator (self-contained folder)...${NC}"
dotnet publish src/DtPipe.Coordinator.Host/DtPipe.Coordinator.Host.csproj -c Release \
    -r "$RID" \
    --self-contained true \
    -o "$COORDINATOR_DIR"
chmod +x "$COORDINATOR_EXE"

echo "----------------------------------------"
echo "  Verifying the coordinator..."
echo "----------------------------------------"
smoke_coordinator "$COORDINATOR_DIR" "$COORDINATOR_EXE"

# ============================================================
# Run Tests
# ============================================================
tests/scripts/run_unit_tests.sh

echo ""
echo -e "${YELLOW}Performing a clean full rebuild...${NC}"
dotnet restore DtPipe.sln
dotnet clean DtPipe.sln -c Release
dotnet build DtPipe.sln -c Release


# ============================================================
# Summary
# ============================================================
echo ""
echo -e "${GREEN}Build complete!${NC}"
echo ""
echo "Release (single-file):"
ls -lh "$RELEASE_DIR/dtpipe$EXT"
echo ""
echo "Coordinator (needs git on PATH):"
echo "  $COORDINATOR_EXE ($(du -sh "$COORDINATOR_DIR" | cut -f1))"

# ── Shell Detection ───────────────────────────────────────────
CURRENT_SHELL=$(basename "$SHELL")
RC_FILE=".${CURRENT_SHELL}rc"
if [[ "$CURRENT_SHELL" == "zsh" ]]; then
    RC_FILE="~/.zshrc"
elif [[ "$CURRENT_SHELL" == "bash" ]]; then
    RC_FILE="~/.bashrc"
else
    RC_FILE="your shell profile"
fi

echo ""
echo -e "${YELLOW}Dev tip (autocompletion):${NC}"
echo -e "  ${GREEN}1.${NC} Run: export DTPIPE_BIN=\"$SCRIPT_DIR/dist/release/dtpipe$EXT\""
echo -e "  ${GREEN}2.${NC} Run: \$DTPIPE_BIN completion --install"
echo -e "  ${GREEN}3.${NC} Run: source $RC_FILE"
echo ""
echo -e "   (Restart your terminal for changes to take effect if not sourced)"
echo ""
echo -e "   (Settings saved in dist/release/.env for reference)"

echo ""
echo -e "${YELLOW}Usage:${NC}"
echo "  ./dist/release/dtpipe$EXT --help"
