#!/usr/bin/env bash
set -eo pipefail

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source "$SCRIPT_DIR/agent_runner.sh"

setup_data() {
    echo "Ensuring test source data in CSV format..."
    mkdir -p "tests/agentic/artifacts"
    
    cat <<EOF > "tests/agentic/artifacts/users_raw.csv"
id,full_name,contact_email
1,Jean Dupont,jean.dupont@gmail.com
2,Alice Martin,alice.martin@yahoo.fr
3,Bob Vance,bob.vance@vancerefrigeration.com
EOF
}

validate_data() {
    rm -f "tests/agentic/artifacts/users_raw.csv"
    return 0
}

run_mission \
    "MCP Enrichment Tools Verification" \
    "Your mission is to perform a pipeline check: 1. Use the 'inspect' tool to read the real schema of 'csv:tests/agentic/artifacts/users_raw.csv'. 2. Write a YAML job copying it to 'csv:tests/agentic/artifacts/users_clean.csv'. 3. Perform a 'dry-run' of that job to validate schema compatibility and preview transformations without writing to disk." \
    setup_data \
    validate_data
