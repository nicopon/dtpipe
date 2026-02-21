#!/usr/bin/env bash
set -eo pipefail

echo "Building DtDuck (Rust/DuckDB) in Release mode..."

# Navigate to the project directory
cd "$(dirname "$0")/src/DtDuck"

# Build the project
cargo build --release

# Copy the binary to dist/
mkdir -p ../../dist
cp target/release/dtduck ../../dist/dtduck

echo "DtDuck built successfully -> dist/dtduck"
