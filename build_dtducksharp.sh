#!/usr/bin/env bash
set -eo pipefail

echo "Building DtDuckSharp (.NET/DuckDB) in Release mode..."

# Navigate to the project directory
cd "$(dirname "$0")/src/DtDuckSharp"

# Build the project
dotnet publish -c Release -r osx-arm64 --self-contained true /p:PublishSingleFile=false -o bin/publish

# Copy the binary (and native libs) to dist/
mkdir -p ../../dist/dtducksharp_bin
cp -r bin/publish/* ../../dist/dtducksharp_bin/

# Create a symlink or wrapper in dist/dtducksharp
cat <<EOF > ../../dist/dtducksharp
#!/usr/bin/env bash
DIR="\$( cd "\$( dirname "\${BASH_SOURCE[0]}" )" && pwd )"
"\$DIR/dtducksharp_bin/DtDuckSharp" "\$@"
EOF
chmod +x ../../dist/dtducksharp

echo "DtDuckSharp built successfully -> dist/dtducksharp"
