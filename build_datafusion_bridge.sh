#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

echo "Building DtPipe.Processors.DataFusion (Rust Native Bridge) in Release mode..."

RUST_TARGET="${RUST_TARGET:-}"
TARGET_ARGS=""
TARGET_DIR="release"
if [ -n "$RUST_TARGET" ]; then
    TARGET_ARGS="--target $RUST_TARGET"
    TARGET_DIR="$RUST_TARGET/release"
fi

# Build the Rust crate
cd src/DtPipe.Processors.DataFusion
cargo build --release $TARGET_ARGS

echo "Copying compiled native libraries to DtPipe.Processors/DataFusion/..."
cd ../..

# Ensure destination directory exists
mkdir -p src/DtPipe.Processors/DataFusion

# Copy Unix shared libraries if they exist
if [ -f "src/DtPipe.Processors.DataFusion/target/$TARGET_DIR/libdtpipe_datafusion.dylib" ]; then
    cp "src/DtPipe.Processors.DataFusion/target/$TARGET_DIR/libdtpipe_datafusion.dylib" src/DtPipe.Processors/DataFusion/
fi

if [ -f "src/DtPipe.Processors.DataFusion/target/$TARGET_DIR/libdtpipe_datafusion.so" ]; then
    cp "src/DtPipe.Processors.DataFusion/target/$TARGET_DIR/libdtpipe_datafusion.so" src/DtPipe.Processors/DataFusion/
fi

# Copy Windows DLL if it exists
if [ -f "src/DtPipe.Processors.DataFusion/target/$TARGET_DIR/dtpipe_datafusion.dll" ]; then
    cp "src/DtPipe.Processors.DataFusion/target/$TARGET_DIR/dtpipe_datafusion.dll" src/DtPipe.Processors/DataFusion/
fi

echo "DataFusion Bridge built and copied successfully."
