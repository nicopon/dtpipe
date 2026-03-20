#!/bin/bash

# Shared helper functions for DtPipe integration tests

# Run dtpipe via YAML round-trip: export CLI args to a job file, then execute from it.
# Usage: run_via_yaml [args...]

run_via_yaml() {
    local yaml_file="$OUTPUT_DIR/temp_config_$$.yaml"
    
    # 1. Export configuration to YAML
    echo "  📄 Exporting config to YAML..."
    "$DTPIPE" "$@" "--export-job" "$yaml_file"
    
    if [ ! -f "$yaml_file" ]; then
        echo "❌ YAML Export failed: File $yaml_file not created."
        exit 1
    fi
    
    # Optional: Cat the yaml file for debugging if verbose
    echo "--- YAML CONTENT ---"
    cat "$yaml_file"
    echo "--------------------"


    # 2. Run using the generated YAML
    # We strip any arguments that are now in the YAML (like input, query, output, transformers)
    # But some args might be runtime overrides (like limit, dry-run). 
    # For simplicity in these tests, the YAML contains everything.
    echo "  🚀 Running via YAML..."
    "$DTPIPE" "--job" "$yaml_file"
    
    # Clean up
    rm -f "$yaml_file"
}
