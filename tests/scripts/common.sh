#!/bin/bash

# Shared helper functions for QueryDump integration tests

# Run querydump with automatic YAML export/import verification
# Usage: run_via_yaml [args...]
# This function will:
# 1. Run the command directly (to verify CLI args work)
# 2. Export the config to YAML
# 3. Run the command using the YAML config (to verify YAML support)
# 
# Note: For efficiency, we might want to ONLY run via YAML to save time if the goal is testing logic + yaml.
# But keeping the direct run ensures CLI parsing isn't broken.
# However, the user request "production des yaml via la ligne de commande puis deuxième run avec le yaml" implies doing both or at least the chain.
# To match the user request exactly: "export yaml via CLI args -> run with yaml". 
# So we can skip the direct run if we want to be strict about "using yaml for the run", allows testing that export works.
# But for now let's be safe: If we replace $QUERYDUMP with this, previous tests expected $QUERYDUMP to execute the action.
# So we should probably do:
# 1. Export to YAML
# 2. Run from YAML
# IF we skip direct run, we rely 100% on YAML path. This is good for "YAML integration tests".

run_via_yaml() {
    local yaml_file="$OUTPUT_DIR/temp_config_$$.yaml"
    
    # 1. Export configuration to YAML
    echo "  📄 Exporting config to YAML..."
    "$QUERYDUMP" "$@" "--export-job" "$yaml_file"
    
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
    "$QUERYDUMP" "--job" "$yaml_file"
    
    # Clean up
    rm -f "$yaml_file"
}
