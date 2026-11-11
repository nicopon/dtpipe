# Secrets

[← Documentation](../README.md)

Keep credentials out of the command line, the shell history and committed job files. The same
forms work on the command line and in a YAML job.

## The OS credential store

```bash
dtpipe secret set prod-db "pg:Host=prod;Database=app;Username=app;Password=…"
dtpipe -i keyring://prod-db --query "SELECT * FROM users" -o users.parquet
```

`dtpipe secret list`, `get` and `delete` manage the entries. They live in the macOS Keychain, the
Windows Credential Manager or the Linux Secret Service.

## Four forms

| Form | Replaces | Works in |
|:---|:---|:---|
| `keyring://alias` | The whole value | Connection strings, queries, hooks, scripts |
| `${{keyring://alias}}` | Part of a value, from the credential store | Everywhere, including any YAML value |
| `${{ENV_VAR}}` | Part of a value, from the environment | Everywhere, including any YAML value |
| `@path/to/file` | The whole value, read from a file | Queries, hooks, scripts |

```bash
# Half from the environment, half from the credential store
dtpipe -i "pg:Host=${{PGHOST}};Database=app;Username=app;Password=${{keyring://pg-pass}}" \
  --query "@queries/active_users.sql" -o users.parquet
```

In a YAML job, `${{…}}` works in any text value, but not in a number: `batch-size: ${{SIZE}}` fails.
The full matrix is in [REFERENCE.md](../../REFERENCE.md#value-resolution).

## In CI

A build agent has environment variables rather than a credential store:

```yaml
main:
  input: "pg:Host=${{PGHOST}};Database=${{PGDATABASE}};Username=${{PGUSER}};Password=${{PGPASSWORD}}"
  output: "s3://bucket/users.parquet"
```

Object-storage credentials left unset come from the environment, the shared config or the instance
profile: see [Object storage](../connections/object-storage.md).

## What an exported job holds

`--export-job` writes the reference, not the value behind it:

```bash
PGPASSWORD=hunter2 dtpipe \
  -i "pg:Host=db.internal;Database=app;Username=etl;Password=${{PGPASSWORD}}" \
  -o out.csv --export-job load.yaml
```

```yaml
main:
  input: pg:Host=db.internal;Database=app;Username=etl;Password=${{PGPASSWORD}}
  output: out.csv
  batch-size: 32768
  sampling-rate: 1
```

The file is safe to commit. A password you typed as a literal is exported as a literal.

## What dtpipe prints

Every connection string dtpipe prints is masked, except for keys known to hold no secret:

```bash
dtpipe -i "Host=db.internal;Database=app;Username=etl;Password=hunter2" -o out.csv
```

```
No reader factory resolved for input 'Host=db.internal;Database=app;Username=etl;Password=***'
```

Pitfalls:

- A database driver's own error message can quote its connection string, and dtpipe does not
  rewrite it. Do not publish a log without reading it.
- A key dtpipe does not know is masked too, even when it is harmless.

---

See also: [Jobs and production](production.md) ·
[REFERENCE.md](../../REFERENCE.md#secret-management)
