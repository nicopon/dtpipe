# Object storage: S3 and Azure Blob

[← Connections](README.md) · [Documentation](../README.md)

```bash
dtpipe -i s3://bucket/events/2026-08-*.parquet --s3-region eu-west-1 -o events.csv
dtpipe -i sales.csv -o azure://reports/sales.parquet \
       --azure-connection-string "${{keyring://azure-conn}}"
```

| Provider | Schemes | Options |
|:---|:---|:---|
| S3-compatible (AWS, MinIO, R2…) | `s3://`, `s3a://` | `--s3-endpoint`, `--s3-region`, `--s3-access-key`, `--s3-secret-key`, `--s3-session-token`, `--s3-url-style` |
| Azure Blob | `azure://`, `az://` | `--azure-connection-string`, `--azure-account-name`, `--azure-account-key`, `--azure-sas`, `--azure-endpoint` |

- **Reads stream**, with no temporary download, and **glob** across folders:
  `s3://bucket/dt=*/part-*.parquet`.
- **Writes replace the object**, uploaded when the pipeline completes: a failed run leaves the
  existing object as it was. `--strategy` is refused.
- **The format comes from the extension**: `.parquet`, `.csv`, `.tsv`, `.json`, `.jsonl`,
  `.ndjson`. Anything else is refused.

## Credentials

Leave the keys unset to use the environment, the shared config or the instance profile. Otherwise
take them from the credential store or the environment:

```bash
dtpipe -i s3://bucket/in.parquet \
  --s3-access-key "${{keyring://s3-key}}" --s3-secret-key "${{keyring://s3-secret}}" \
  -o out.csv
```

A read and a write in the same pipeline can use different credentials.

`https://` and `gs://` have no provider: read them through DuckDB, with
`--duck-init "INSTALL httpfs; LOAD httpfs"` and a `read_parquet(…)` query. See
[DuckDB](duckdb.md).

---

See also: [Secrets](../guides/secrets.md) ·
[REFERENCE.md](../../REFERENCE.md#object-storage-s3-azure)
