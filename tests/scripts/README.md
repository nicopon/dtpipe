# Integration Validation Scripts

This directory contains a suite of Bash scripts used to validate the DtPipe functionality from end to end. These tests go beyond unit testing by verifying the actual binary execution, file system interaction, and multi-database connectivity.

## Infrastructure Management

DtPipe uses a centralized Docker infrastructure for all integration tests.

- **Shared Infrastructure**: All database containers (Postgres, MSSQL, Oracle) are defined in [tests/infra/docker-compose.yml](../infra/docker-compose.yml).
- **Startup & Health**: Scripts call [start_infra.sh](../infra/start_infra.sh) which ensures all services are not just running, but fully ready for SQL connections.
- **Persistence**: To accelerate development, containers **persist** after scripts finish. Use [stop_infra.sh](../infra/stop_infra.sh) if you need a full cleanup.

## Scripts Index

All scripts should be executed from the project root. They automatically build the project in `Release` mode before execution.

### 🏆 Master Runners
| Script | Description |
|:---|:---|
| **`run_all_tests.sh`** | **Master Runner**. Executes the full validation suite and provides a consolidated summary. |
| **`smoke_test.sh`** | **Basic E2E**. Quick check: CSV -> Postgres -> CSV. |
| **`golden_smoke_test.sh`**| **Full Suite**. 1M rows, edge cases, and all database providers. |

### 🛠️ Feature Validation
| Script | Validates |
|:---|:---|
| **`validate_adapters.sh`** | Basic Read/Write for all registered adapters. |
| **`validate_chain.sh`** | Multi-hop pipelines (CSV -> PG -> MSSQL -> Ora -> Parquet). |
| **`validate_incremental_loading.sh`** | Upsert and Ignore strategies across all providers. |
| **`validate_auto_migration.sh`** | Automatic schema evolution (`--auto-migrate`). |
| **`validate_schema_validation.sh`** | Strict schema checks and validation rules. |
| **`validate_features_integration.sh`**| Combined features (Hooks, Sampling, Transformers). |
| **`validate_options_scoping.sh`** | Positional CLI scoping (Reader vs Writer options). |
| **`validate_yaml_options.sh`** | Job file parsing and option priority. |
| **`validate_project.sh`** | Column operations (`--project`, `--drop`). |
| **`validate_metrics.sh`** | Structured JSON metrics output. |

### ⚙️ Engine & XStreamer Tests
| Script | Focus Area |
|:---|:---|
| **`validate_dtfusion.sh`** | DataFusion XStreamer functionality and performance. |
| **`validate_dtpolars.sh`** | Polars-based transformations (if enabled). |
| **`validate_drivers_docker.sh`** | Connectivity and driver health for all DBs in Docker. |
| **`verify_pg_only.sh`** | PostgreSQL-specific features (Bulk Copy, etc.). |

### 🔄 Resilience & Reliability
| Script | Failure Scenario |
|:---|:---|
| **`validate_resilience.sh`** | General retry logic and transient error handling. |
| **`validate_resilience_network.sh`** | Network timeouts and connection drops. |
| **`validate_resilience_locking.sh`** | Database locks and concurrency issues. |
| **`validate_resilience_upsert.sh`** | Integrity errors during incremental loads. |

### 🧪 Transformations & Data
| Script | Description |
|:---|:---|
| **`validate_transformers.sh`** | Core transformers (Mask, Null, Overwrite, JS). |
| **`validate_new_transformers.sh`** | Newly added or experimental transformers. |
| **`verify_sampling.sh`** | Statistical accuracy of `--sampling-rate`. |
| **`verify_hooks.sh`** | Lifecycle hooks (`PreExec`, `PostExec`, etc.). |
| **`validate_readme_examples.sh`**| Verifies that all examples in the main README still work. |

### 📊 Benchmarks
| Script | Target |
|:---|:---|
| **`benchmark_engines_fair.sh`** | Comparison between DuckDB, DataFusion, and Core engines. |
| **`benchmark_dtpipe_columnar.sh`**| Performance of Zero-Copy paths. |
| **`benchmark_scale_100m.sh`** | Large-scale (100 million rows) stress test. |
| **`validate_oracle_perf.sh`** | Oracle-specific performance tuning. |

---

## Technical Details

### `run_via_yaml` Methodology
Most scripts use the `run_via_yaml` function (defined in `common.sh`). This validates that CLI arguments correctly translate to YAML job definitions.

### Artifacts
Temporary files are generated in `tests/artifacts/`. Successful runs clean up their specific files, while failed runs preserve them for debugging.
