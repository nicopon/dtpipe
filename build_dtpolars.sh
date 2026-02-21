#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

echo "Building DtPolars (Rust) in Release mode..."
cd src/DtPolars
cargo build --release

echo "Copying binary to dist/..."
cd ../..
mkdir -p dist
cp src/DtPolars/target/release/dtpolars dist/dtpolars

echo "DtPolars built successfully -> dist/dtpolars"
