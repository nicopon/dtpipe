# Integration Validation Scripts

This directory contains a suite of Bash scripts used to validate DtPipe functionality end to end. These tests go beyond unit testing by verifying the actual binary execution, file system interaction, and multi-database connectivity.

## Infrastructure Management

DtPipe uses a centralized Docker infrastructure for all integration tests.

- **Shared Infrastructure**: All database containers (Postgres, MSSQL, Oracle) are defined in [tests/infra/docker-compose.yml](../infra/docker-compose.yml).
- **Startup & Health**: Scripts call [start_infra.sh](../infra/start_infra.sh) which ensures all services are not just running, but fully ready for SQL connections.
- **Persistence**: Containers persist after scripts finish. Use [stop_infra.sh](../infra/stop_infra.sh) if you need a full cleanup.

## Quick Start

```bash
# Run everything (smoke + test-docker + catalog + bench)
./tests/scripts/run.sh --full

# No Docker: transformers, schema, options, hooks, docs, DAG topologies
./tests/scripts/run.sh --test

# Docker required: above + driver chain + upsert/ignore
./tests/scripts/run.sh --test-docker

# 135-command catalog suite (Docker + init_test_data.sh required)
./tests/scripts/init_test_data.sh
./tests/scripts/run.sh --catalog

# DAG topologies only (no Docker)
./tests/scripts/run.sh --dag
```

## Master Runner

**`run.sh`** — Orchestrates all suites. Modes:

| Flag | What runs | Docker? |
|:---|:---|:---|
| `--smoke` | Golden smoke test: edge cases, 1M rows, all DB drivers | Yes |
| `--test` | Transformers, schema, options, hooks, docs, resilience, DAG | No |
| `--test-docker` | All `--test` suites + driver chain (upsert/ignore/cross-DB) | Yes |
| `--catalog` | 135-command catalog (requires `init_test_data.sh` first) | Yes |
| `--dag` | DAG topology validation only | No |
| `--bench` | Performance benchmarks (linear, DuckDB, DataFusion SQL JOIN) | No |
| `--full` | All of the above | Yes |

## Scripts Index

### 🔥 Smoke & Drivers
| Script | Description | Docker? |
|:---|:---|:---|
| **`smoke.sh`** | Vicious edge cases (CSV escaping, SQL injection, NULL, UTF-8), 1M rows, composite-key upsert on all DB drivers. | Yes |
| **`validate_drivers.sh`** | Read/write for all drivers, Upsert/Ignore strategies, cross-driver chain (CSV→PG→MSSQL→Oracle→Parquet), Oracle insert modes. | Yes |

### 🧪 Feature Validation (no Docker)
| Script | Validates |
|:---|:---|
| **`validate_transformers.sh`** | All 13 row transformers: Overwrite, Null, Mask, Fake, Format, Compute, Drop, Project, Rename, Filter, Expand, Window, ordering. |
| **`validate_schema.sh`** | Strict-schema rejection, `--no-schema-validation` bypass, `--auto-migrate` for SQLite and Postgres. |
| **`validate_options.sh`** | Provider option scoping (global/writer/YAML), sampling rate + seed + determinism, YAML `provider-options`, `--metrics-path`. |
| **`validate_hooks.sh`** | `--pre-exec` (inline + file), `--post-exec`, `--finally-exec` lifecycle hooks via SQLite. |
| **`validate_docs.sh`** | All `--flags` in README/COOKBOOK are present in `--help`; representative README examples execute correctly. |
| **`validate_resilience.sh`** | YAML retry-options round-trip, SQLite lock retry, network interruption via Toxiproxy (Append + Upsert). |
| **`validate_dag.sh`** | All 8 canonical DAG topologies: Linear, Two-source, SQL, SQL JOIN, Fan-out, Fan-out+SQL, Diamond, Join→fan-out. |

### 📋 Catalog Suite
| Script | Description | Docker? |
|:---|:---|:---|
| **`run_catalog_tests.sh`** | 135 numbered commands covering the full feature surface: adapters, DAG patterns, transformers, volumetrics, error cases, real-world scenarios. Requires `init_test_data.sh`. | Yes |
| **`init_test_data.sh`** | Provisions all data sources (CSV, Parquet, Arrow, DuckDB, PG, MSSQL, Oracle) used by the catalog suite. Idempotent. | Yes |
| **`clean_test_data.sh`** | Removes all provisioned artifacts for a full reset. | No |

### 📊 Benchmarks
| Script | Target |
|:---|:---|
| **`bench.sh`** | Linear pipeline throughput (100k→CSV, CSV→Parquet, Parquet+transforms), DuckDB 1M rows, DataFusion SQL JOIN (DAG mode + direct `--src-main`/`--src-ref`). |
| **`benchmark_dtpipe_columnar.sh`** | Zero-copy columnar path performance. |
| **`generate_benchmark_datasets.sh`** | Generates large Parquet/CSV datasets for JOIN benchmarks. |

### 🛠️ Utilities
| Script | Description |
|:---|:---|
| **`validate_sample.sh`** | Validates the `DtPipe.Sample` programmatic API project (standalone, not in `run.sh`). |
| **`monitor_mem.sh`** | Memory usage monitoring helper. |

---

## Artifacts

Temporary files land in `tests/scripts/artifacts/`. Scripts clean up on success; failures preserve artifacts for debugging.
