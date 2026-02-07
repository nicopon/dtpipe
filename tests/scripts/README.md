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
| **`common.sh`** | **Utility**. Shared functions like `run_via_yaml` (CLI/YAML cross-validation). Not executed directly. | - |
| **`run_all_tests.sh`** | **Master Runner**. Discovers and executes all `.sh` scripts in parallel (one by one) and provides a summary. | - |
| **`smoke_test.sh`** | **Quick Check**. Basic end-to-end flow: CSV -> Postgres -> CSV. | Docker |
| **`golden_smoke_test.sh`** | **Comprehensive Test**. "Vicious" suite covering 1M rows, edge cases (UTF-8, Quotes), and all DBs (Oracle, PG, MSSQL, etc.). | Docker |
| **`validate_chain.sh`** | **Chaining Test**. Multi-hop pipeline: CSV -> PG -> MSSQL -> Ora -> Parquet with checksum verification. | Docker, `docker-compose` |
| **`validate_incremental_loading.sh`** | **Incremental Test**. Deep dive into `Upsert` and `Ignore` strategies across all providers. | Docker |
| **`verify_hooks.sh`** | **Hooks Test**. Validates `PreExec`, `PostExec`, `OnErrorExec`, and `FinallyExec` lifecycle hooks. | Docker |
| **`validate_transformers.sh`** | **Transformer Test**. Behavior of `--overwrite`, `--null`, `--mask` on generated data. | - |
| **`validate_project.sh`** | **Projection Test**. Behavior of `--project` (whitelist) and `--drop` (blacklist). | - |
| **`verify_sampling.sh`** | **Sampling Test**. Statistical check of the `--sample-rate` logic. | - |
| **`validate_readme_examples.sh`** | **Doc Test**. Executes all code examples from the main `README.md`. | - |
| **`validate_yaml_options.sh`** | **Config Test**. Verifies YAML-defined provider options application. | - |
| **`validate_drivers_docker.sh`** | **Connectivity Test**. Spins up containers specifically to test provider read/write drivers. | Docker |
| **`validate_oracle_perf.sh`** | **Performance Test**. Benchmarks Oracle `Standard` vs `Bulk` vs `Append` modes. | Docker |
| **`verify_pg_only.sh`** | **Targeted Test**. Fast validation focused exclusively on PostgreSQL features. | Docker |

## Technical Details

### `run_via_yaml` Methodology
Most scripts use the `run_via_yaml` function (defined in `common.sh`). This approach enforces a double check:
1.  **CLI Parsing**: The command is first run with `--export-job temp.yaml`. This verifies that the CLI correctly parses arguments.
2.  **YAML Parsing**: The command is then re-run with `--job temp.yaml`. This verifies that the generated YAML file is valid and produces the expected result.

### Outputs
Temporary files and test results are generated in the `tests/scripts/artifacts/` folder. This folder is automatically cleaned up after each successful execution (via `trap cleanup EXIT`), except in case of failure to allow debugging.
