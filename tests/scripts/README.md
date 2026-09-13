# Integration Validation Scripts

This directory contains a suite of Bash scripts used to validate DtPipe functionality end to end. These tests go beyond unit testing by verifying the actual binary execution, file system interaction, and multi-database connectivity.

## Infrastructure Management

DtPipe uses a centralized Docker infrastructure for all integration tests.

- **Shared Infrastructure**: All database and storage containers (Postgres, MSSQL, Oracle, MySQL, MinIO S3) are defined in [tests/infra/docker-compose.yml](../infra/docker-compose.yml).
- **Startup & Health**: Scripts call [start_infra.sh](../infra/start_infra.sh) which ensures all services are not just running, but fully ready for connections.
- **Persistence**: Containers persist after scripts finish. Use [stop_infra.sh](../infra/stop_infra.sh) if you need a full cleanup.

## Quick Start

```bash
# Run everything (smoke + test-docker + catalog + bench)
./tests/scripts/run.sh --full

# No Docker: transformers, schema, options, hooks, docs, DAG topologies
./tests/scripts/run.sh --test

# Docker required: above + driver chain + upsert/ignore + DuckDB hub
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
| `--test` | Transformers, schema, options, hooks, docs, DAG | No |
| `--test-docker` | All `--test` suites + driver chain (upsert/ignore/cross-DB) + DuckDB Hub | Yes |
| `--catalog` | 135-command catalog (requires `init_test_data.sh` first) | Yes |
| `--dag` | DAG topology validation only | No |
| `--bench` | Performance benchmarks (linear pipeline, DuckDB) | No |
| `--full` | All of the above | Yes |

## Scripts Index

### 🔥 Smoke & Drivers
| Script | Description | Docker? |
|:---|:---|:---|
| **`smoke.sh`** | Vicious edge cases (CSV escaping, SQL injection, NULL, UTF-8), 1M rows, composite-key upsert on all DB drivers. | Yes |
| **`validate_drivers.sh`** | Read/write for all drivers, Upsert/Ignore strategies, cross-driver chain (CSV→PG→MSSQL→Oracle→Parquet), Oracle insert modes. | Yes |
| **`validate_duck_hub.sh`** | DuckDB Extender & Hub (`duck+{provider}:`): SQLite, Postgres, MySQL, and MinIO S3 Object Storage (`httpfs` / `duck+s3:`). | Yes |

### 🧪 Feature Validation (no Docker)
| Script | Validates |
|:---|:---|
| **`validate_transformers.sh`** | All 13 row transformers: Overwrite, Null, Mask, Fake, Format, Compute, Drop, Project, Rename, Filter, Expand, Window, ordering. |
| **`validate_schema.sh`** | Strict-schema rejection, `--no-schema-validation` bypass, `--auto-migrate` for SQLite and Postgres. |
| **`validate_options.sh`** | Provider option scoping (global/writer/YAML), sampling rate + seed + determinism, YAML `provider-options`, `--metrics-path`. |
| **`validate_hooks.sh`** | `--pre-exec` (inline + file), `--post-exec`, `--finally-exec` lifecycle hooks via SQLite. |
| **`validate_cursor.sh`** | Incremental loading: `--cursor`, `--state`, `${{cursor://...}}` value resolution, state file lifecycle. |
| **`validate_docs.sh`** | All `--flags` in README/COOKBOOK are present in `--help`; representative README examples execute correctly. |
| **`validate_dag.sh`** | All 9 canonical DAG topologies: Linear, Two-source, SQL, SQL JOIN, Fan-out, Fan-out+SQL, Diamond, Join→fan-out, Nested data. |
| **`validate_duck_streaming.sh`** | A `duck:` read stays flat as the result grows — 20× the rows at the same peak memory, linear and fan-out. Calibrates its own tolerance against a query that holds the result whole, and skips rather than fails where that calibration does not hold. Not the ownership net: a missed `Dispose` is caught by `CDataOwnershipTests`, not by memory. |

### 📋 Catalog Suite
| Script | Description | Docker? |
|:---|:---|:---|
| **`run_catalog_tests.sh`** | 142 numbered commands covering the full feature surface: adapters, DAG patterns, transformers, volumetrics, error cases, real-world scenarios. A test that must fail declares the message fragment it expects as a third argument to `run_test`, so it asserts *why* it failed rather than only that it did. Requires `init_test_data.sh`. | Yes |
| **`init_test_data.sh`** | Provisions all data sources (CSV, Parquet, Arrow, DuckDB, PG, MSSQL, Oracle) used by the catalog suite. Idempotent. | Yes |
| **`clean_test_data.sh`** | Removes all provisioned artifacts for a full reset. | No |

### 📊 Benchmarks
| Script | Target |
|:---|:---|
| **`bench.sh`** | Linear pipeline throughput (100k→CSV, CSV→Parquet, Parquet+transforms), DuckDB 1M rows. |
| **`benchmark_dtpipe_columnar.sh`** | Zero-copy columnar path performance. |
| **`generate_benchmark_datasets.sh`** | Generates large Parquet/CSV datasets for JOIN benchmarks. |
| **`micro_perf_gate.sh`** | Micro performance gate — BenchmarkDotNet in-process on the hot conversion paths, compared against a versioned baseline. Local only, reference machine — not run in CI. |

#### The three-tier performance gate

| Tier | What | Where | Threshold |
|:---|:---|:---|:---|
| **Micro** | `micro_perf_gate.sh` — BenchmarkDotNet, no infrastructure | Local only, reference machine | Wide (detects a ×2) |
| **Macro complete** | 15 scenarios incl. Oracle and SQL Server | Local only, `dtpipe-sandbox` repo, tag ritual | 15 % |
| **Macro light** | file↔file + PostgreSQL subset | Optional, nightly — only if micro proves insufficient | Wide |

All three tiers stay out of CI. Macro complete never ran there — free runners cannot
host Oracle and SQL Server containers. Micro did, once, on 2026-09-05: run against the
reference-machine baseline via `--allow-foreign-host` on a GitHub-hosted runner, 30 of
31 committed benchmarks came back flagged as regressions, +111 % to +201 %, from
machine identity alone. GitHub gives no stability guarantee on shared-runner
performance, so no fixed threshold survives that gap without also surviving a real
regression unnoticed. Removed from `build.yml` the same day.

**Baselines carry a machine fingerprint and the gate refuses to compare across two.**
Comparing durations measured on different hardware does not give a weaker verdict, it
gives a misleading one. `--allow-foreign-host` overrides the refusal and clamps the
threshold to no tighter than 50 % for a deliberate, informed check — it is not wired
into any CI job.

The baseline is `tests/DtPipe.Benchmarks/baselines/micro_perf.json` — it lives with the
benchmark project, not in `tests/scripts/baselines/`, which holds golden *data* fixtures.

```bash
# Record a baseline on this machine
./tests/scripts/micro_perf_gate.sh --update

# Compare (strict — refuses a foreign host)
./tests/scripts/micro_perf_gate.sh

# Just print the numbers, compare nothing
./tests/scripts/micro_perf_gate.sh --report-only
```

Exit codes: `0` pass · `1` regression · `2` refused to render a verdict · `3` setup error.

### 🛠️ Utilities
| Script | Description |
|:---|:---|
| **`monitor_mem.sh`** | Memory usage monitoring helper. |

---

## Artifacts

Temporary files land in `tests/scripts/artifacts/`. Scripts clean up on success; failures preserve artifacts for debugging.
