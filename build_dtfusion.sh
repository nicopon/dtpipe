#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

echo "Building DtFusion (Rust/DataFusion) in Release mode..."
cd experiments/DtFusion
cargo build --release

echo "Copying binary to dist/..."
cd ../..
mkdir -p dist
cp experiments/DtFusion/target/release/dtfusion dist/dtfusion

echo "DtFusion built successfully -> dist/dtfusion"
