# Integration Validation Scripts

This directory contains a suite of Bash scripts used to validate the DtPipe functionality from end to end. These tests go beyond unit testing by verifying the actual binary execution, file system interaction, and multi-database connectivity.

## Infrastructure Management

DtPipe uses a centralized Docker infrastructure for all integration tests.

- **Shared Infrastructure**: All database containers (Postgres, MSSQL, Oracle) are defined in [tests/infra/docker-compose.yml](file:///Users/PonsartNi/Source/dtpipe/tests/infra/docker-compose.yml).
- **Startup & Health**: Scripts call [start_infra.sh](file:///Users/PonsartNi/Source/dtpipe/tests/infra/start_infra.sh) which ensures all services are not just running, but fully ready for SQL connections.
- **Persistence**: To accelerate development, containers **persist** after scripts finish. Use [stop_infra.sh](file:///Users/PonsartNi/Source/dtpipe/tests/infra/stop_infra.sh) if you need a full cleanup.

## General Usage

All scripts should be executed from the project root. They automatically build the project in `Release` mode before execution.

### Prerequisites
- Docker & Docker Compose
- `.net` SDK 10.0+
- `mssql-tools` (not required locally, handled via a sidecar container in Docker)

## Scripts List and Purpose

| Script | Main Purpose | Dependencies |
|--------|--------------|--------------|
| **`run_all_tests.sh`** | **Master Runner**. Executes all relevant scripts and provides a consolidated summary of results. | Shared Infra |
| **`smoke_test.sh`** | **Quick Check**. Basic end-to-end flow: CSV -> Postgres -> CSV. | Shared Infra |
| **`golden_smoke_test.sh`** | **Advanced Suite**. Covers 1M rows, edge cases (UTF-8, Quotes), and all DBs (Oracle, PG, MSSQL, etc.). | Shared Infra |
| **`validate_chain.sh`** | **Multi-Hop Pipeline**. CSV -> PG -> MSSQL -> Ora -> Parquet with checksum verification. | Shared Infra |
| **`validate_incremental_loading.sh`** | **Upsert/Ignore**. Deep dive into incremental strategies across all database providers. | Shared Infra |
| **`validate_drivers_docker.sh`** | **Connectivity**. Validates basic driver operations (Read/Write) for all database engines. | Shared Infra |
| **`validate_oracle_perf.sh`** | **Performance**. Benchmarks Oracle `Standard` vs `Bulk` vs `Append` modes. | Shared Infra |
| **`verify_hooks.sh`** | **Lifecycle Hooks**. Validates `PreExec`, `PostExec`, `OnErrorExec`, and `FinallyExec`. | Shared Infra |
| **`verify_pg_only.sh`** | **Postgres Focused**. Rapid validation targeting specific PostgreSQL features. | Shared Infra |
| **`validate_transformers.sh`** | **Transformation**. Exercises `--overwrite`, `--null`, `--mask` and JavaScript transforms. | - |
| **`validate_project.sh`** | **Column Ops**. Validates column whitelisting (`--project`) and blacklisting (`--drop`). | - |
| **`verify_sampling.sh`** | **Sampling**. Statistical verification of the `--sampling-rate` and deterministic seeds. | - |
| **`validate_readme_examples.sh`** | **Documentation**. Automatically executes and verifies code examples from the main README. | - |

## Technical Details

### `run_via_yaml` Methodology
Most scripts use the `run_via_yaml` function (defined in `common.sh`). This validates that CLI arguments correctly translate to YAML job definitions.

### Artifacts
Temporary files are generated in `tests/scripts/artifacts/`. Successful runs clean up their specific files, while failed runs preserve them for debugging.
