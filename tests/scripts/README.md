# Integration Validation Scripts

This directory contains a suite of Bash scripts used to validate the DtPipe functionality from end to end. These tests go beyond unit testing by verifying the actual binary execution, file system interaction, and configuration persistence.

## General Usage

All scripts should be executed from the project root or the `tests/scripts` directory. They generally rebuild the project in `Release` mode before running the tests.

### Prerequisites
- Docker (for `validate_drivers_docker.sh` and `validate_chain.sh`)
- `dotnet` SDK 10.0+

## Scripts List and Purpose

| Script | Main Purpose | Dependencies |
|--------|--------------|--------------|
| **`common.sh`** | **Utility**. Contains shared functions, notably `run_via_yaml` which exports config to YAML before replaying it (CLI/YAML cross-validation). Not executed directly. | - |
| **`validate_drivers_docker.sh`** | **Real Database Tests**. Spins up Postgres, MSSQL, and Oracle containers, injects data, and verifies that DtPipe can read these sources correctly. | Docker |
| **`validate_chain.sh`** | **Complex Chaining Tests**. Verifies an end-to-end pipeline: CSV -> Postgres -> MSSQL -> Oracle -> Parquet, with input/output checksum (hash) verification to ensure data integrity. | Docker, `docker-compose` |
| **`validate_transformers.sh`** | **Transformer Tests**. Verifies correct behavior of `--overwrite`, `--null`, `--mask`, options on generated source CSV data. | - |
| **`validate_project.sh`** | **Projection Tests**. Verifies that `--project` (whitelist) and `--drop` (blacklist) options correctly filter output columns, including combined cases. | - |
| **`verify_sampling.sh`** | **Sampling Tests**. Verifies that the `--sample-rate` option effectively reduces the number of output rows (simple statistical test on 100 rows). | - |
| **`validate_readme_examples.sh`** | **Documentation Tests**. Literally executes the commands present in the main `README.md` to ensure documentation is up-to-date and functional (adapting file paths to `/tmp`). | - |
| **`validate_yaml_options.sh`** | **Provider Options Tests**. Verifies that provider-specific options (e.g., `strategy: Recreate` for SQLite) defined in YAML are correctly applied by the engine. | - |

## Technical Details

### `run_via_yaml` Methodology
Most scripts use the `run_via_yaml` function (defined in `common.sh`). This approach enforces a double check:
1.  **CLI Parsing**: The command is first run with `--export-job temp.yaml`. This verifies that the CLI correctly parses arguments.
2.  **YAML Parsing**: The command is then re-run with `--job temp.yaml`. This verifies that the generated YAML file is valid and produces the expected result.

### Outputs
Temporary files and test results are generated in the `tests/scripts/output/` folder. This folder is automatically cleaned up after each successful execution (via `trap cleanup EXIT`), except in case of failure to allow debugging.
