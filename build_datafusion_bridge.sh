#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

echo "Building DtPipe.XStreamers.DataFusion (Rust Native Bridge) in Release mode..."

RUST_TARGET="${RUST_TARGET:-}"
TARGET_ARGS=""
TARGET_DIR="release"
if [ -n "$RUST_TARGET" ]; then
    TARGET_ARGS="--target $RUST_TARGET"
    TARGET_DIR="$RUST_TARGET/release"
fi

# Build the Rust crate
cd src/DtPipe.XStreamers.DataFusion
cargo build --release $TARGET_ARGS

echo "Copying compiled native libraries to DtPipe.XStreamers/DataFusion/..."
cd ../..

# Ensure destination directory exists
mkdir -p src/DtPipe.XStreamers/DataFusion

# Copy Unix shared libraries if they exist
if [ -f "src/DtPipe.XStreamers.DataFusion/target/$TARGET_DIR/libdtpipe_xstreamers_datafusion.dylib" ]; then
    cp "src/DtPipe.XStreamers.DataFusion/target/$TARGET_DIR/libdtpipe_xstreamers_datafusion.dylib" src/DtPipe.XStreamers/DataFusion/
fi

if [ -f "src/DtPipe.XStreamers.DataFusion/target/$TARGET_DIR/libdtpipe_xstreamers_datafusion.so" ]; then
    cp "src/DtPipe.XStreamers.DataFusion/target/$TARGET_DIR/libdtpipe_xstreamers_datafusion.so" src/DtPipe.XStreamers/DataFusion/
fi

# Copy Windows DLL if it exists
if [ -f "src/DtPipe.XStreamers.DataFusion/target/$TARGET_DIR/dtpipe_xstreamers_datafusion.dll" ]; then
    cp "src/DtPipe.XStreamers.DataFusion/target/$TARGET_DIR/dtpipe_xstreamers_datafusion.dll" src/DtPipe.XStreamers/DataFusion/
fi

echo "DataFusion Bridge built and copied successfully."
