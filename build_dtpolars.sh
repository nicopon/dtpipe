#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

echo "Building DtPolars (Rust) in Release mode..."
cd experiments/DtPolars
cargo build --release

echo "Copying binary to dist/..."
cd ../..
mkdir -p dist
cp experiments/DtPolars/target/release/dtpolars dist/dtpolars

echo "DtPolars built successfully -> dist/dtpolars"
