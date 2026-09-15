# Object storage: S3 and Azure Blob

[← Connections](README.md) · [Documentation](../README.md)

Object-storage locations are first-class inputs and outputs, not a separate mode. They go through
the DuckDB engine already in the process, so globs, range requests and multipart uploads work with
no extra dependency.

```bash
dtpipe -i s3://bucket/events/2026-08-*.parquet --s3-region eu-west-1 -o events.csv
dtpipe -i sales.csv -o azure://reports/sales.parquet \
       --azure-connection-string "${{keyring://azure-conn}}"
```

| Provider | Schemes | Options |
|:---|:---|:---|
| S3-compatible | `s3://`, `s3a://` | `--s3-endpoint`, `--s3-region`, `--s3-access-key`, `--s3-secret-key`, `--s3-session-token`, `--s3-url-style` |
| Azure Blob | `azure://`, `az://` | `--azure-connection-string`, `--azure-account-name`, `--azure-account-key`, `--azure-sas`, `--azure-endpoint` |

`--s3-endpoint` covers MinIO, Ceph, R2 and anything else that speaks the S3 API.

## How it behaves

- **Reads stream.** No object is downloaded to a temp file first.
- **Writes stage, then upload.** A Parquet footer is only known at the end, so rows are buffered
  (spilling to the temp directory beyond memory) and the upload is issued once the pipeline
  completes — **a failed run leaves the existing object untouched** rather than replacing it with
  a partial one.
- **Writes replace the key.** Object storage has no append or upsert, so `--strategy` is refused
  rather than accepted and ignored.
- **Reads glob across prefixes**: `s3://bucket/dt=*/part-*.parquet` reads every match.
- **Format comes from the extension**, through a closed map: `.parquet`, `.csv`, `.tsv`, `.json`,
  `.jsonl`, `.ndjson`. Anything else is an error naming the supported set — there is no content
  sniffing. For a format outside that map, use `--duck-init` + `--query` with the matching DuckDB
  function.

## Credentials

Leave the key pair unset to use the ambient credential chain — environment, shared config,
instance profile. Otherwise any value form works, so nothing has to sit in shell history:

```bash
dtpipe -i s3://bucket/in.parquet \
  --s3-access-key "${{keyring://s3-key}}" --s3-secret-key "${{keyring://s3-secret}}" \
  -o out.csv
```

Secrets are scoped to their bucket or container, so a read and a write in the same pipeline can
use different credentials.

> [!NOTE]
> `https://` and `gs://` are not claimed by any provider. Reach them through the DuckDB engine:
> `--duck-init "INSTALL httpfs; LOAD httpfs"` plus a `--query` using `read_parquet(…)` or
> `COPY … TO …`. See [DuckDB](duckdb.md).

---

See also: [DuckDB](duckdb.md) · [Secrets](../guides/secrets.md) ·
[REFERENCE.md](../../REFERENCE.md#object-storage-s3-azure)
