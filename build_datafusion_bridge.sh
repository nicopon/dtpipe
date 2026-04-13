#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

PROFILE="${CARGO_BUILD_PROFILE:-release}"

echo "Building DtPipe.Processors.DataFusion (Rust Native Bridge) with profile=${PROFILE}..."

RUST_TARGET="${RUST_TARGET:-}"
TARGET_ARGS=""
TARGET_DIR="${RUST_TARGET:+${RUST_TARGET}/}${PROFILE}"
if [ -n "$RUST_TARGET" ]; then
    TARGET_ARGS="--target $RUST_TARGET"
fi

# Ensure cargo is available if we need to build
if ! command -v cargo &> /dev/null; then
    echo "Error: cargo is not installed. Native bridge cannot be built."
    # Check if binaries already exist
    if [ -f "src/DtPipe.Processors.DataFusion/target/$TARGET_DIR/libdtpipe_datafusion.dylib" ] || \
       [ -f "src/DtPipe.Processors.DataFusion/target/$TARGET_DIR/libdtpipe_datafusion.so" ] || \
       [ -f "src/DtPipe.Processors.DataFusion/target/$TARGET_DIR/dtpipe_datafusion.dll" ]; then
        echo "Found existing binaries, proceeding with staging only..."
    else
        exit 1
    fi
else
    # Build the Rust crate
    cd src/DtPipe.Processors.DataFusion
    cargo build --profile "$PROFILE" $TARGET_ARGS
    cd ../..
fi

echo "Detecting Runtime Identifier (RID)..."
OS="$(uname -s)"
ARCH="$(uname -m)"
RID=""

case "$OS" in
    Darwin)
        if [[ "$ARCH" == "arm64" ]]; then RID="osx-arm64"; else RID="osx-x64"; fi
        ;;
    Linux)
        if [[ "$ARCH" == "aarch64" ]]; then RID="linux-arm64"; else RID="linux-x64"; fi
        ;;
    MINGW*|CYGWIN*|MSYS*)
        if [[ "$ARCH" == "arm64" || "$ARCH" == "aarch64" ]]; then RID="win-arm64"; else RID="win-x64"; fi
        ;;
    *)
        echo "Unsupported platform: $OS"
        exit 1
        ;;
esac

echo "Target RID: $RID"
DEST_DIR="src/DtPipe.Processors/runtimes/$RID/native"

# Clean destination directory to ensure no stale binaries
if [ -d "$DEST_DIR" ]; then
    echo "Cleaning destination directory $DEST_DIR..."
    rm -f "$DEST_DIR"/*
fi
mkdir -p "$DEST_DIR"

# Copy libraries
echo "Copying compiled native libraries to $DEST_DIR..."

SOURCE_DIR="src/DtPipe.Processors.DataFusion/target/$TARGET_DIR"

if [ -f "$SOURCE_DIR/libdtpipe_datafusion.dylib" ]; then
    cp "$SOURCE_DIR/libdtpipe_datafusion.dylib" "$DEST_DIR/"
fi

if [ -f "$SOURCE_DIR/libdtpipe_datafusion.so" ]; then
    cp "$SOURCE_DIR/libdtpipe_datafusion.so" "$DEST_DIR/"
fi

if [ -f "$SOURCE_DIR/dtpipe_datafusion.dll" ]; then
    cp "$SOURCE_DIR/dtpipe_datafusion.dll" "$DEST_DIR/"
fi

echo "DataFusion Bridge built and copied to $DEST_DIR successfully."
