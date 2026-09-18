# Secrets and values

[← Documentation](../README.md)

A connection string in a shell history, a CI log or a committed YAML file is a credential leak
waiting to be found. dtpipe resolves values at runtime from four places, and the same forms work
on the command line and in a job file.

## The OS credential store

```bash
dtpipe secret set prod-db "pg:Host=prod;Database=app;Username=app;Password=…"
dtpipe secret list
dtpipe secret get prod-db        # to verify
dtpipe secret delete prod-db
```

Stored in the macOS Keychain, Windows Credential Manager or the Linux Secret Service — the
platform's own store, not a file dtpipe invented.

```bash
dtpipe -i keyring://prod-db --query "SELECT * FROM users" -o users.parquet
```

## Four forms

| Form | Replaces | Works in |
|:---|:---|:---|
| `keyring://alias` | The **whole** value — a connection string, a `--duck-init` block | Connection strings, queries, hooks, scripts |
| `${{keyring://alias}}` | Part of a string | Everywhere, including every YAML value |
| `${{ENV_VAR}}` | Part of a string, from the environment | Everywhere, including every YAML value |
| `@path/to/file` | The whole value, read from a file | Queries (`--query "@q.sql"`), hooks, scripts |

They compose: a keyring entry can itself contain `${{ENV_VAR}}` placeholders, resolved afterwards.

```bash
# Half from the environment, half from the keychain
dtpipe -i "pg:Host=${{PGHOST}};Database=app;Username=app;Password=${{keyring://pg-pass}}" \
  --query "@queries/active_users.sql" -o users.parquet
```

> [!NOTE]
> In a YAML job, `${{…}}` interpolation is applied to each scalar as it is read, so it reaches every
> **text** value — including every `provider-options` entry. It does not reach a key the loader
> converts to another type: `batch-size: ${{SIZE}}` fails rather than resolves. Full-value
> replacement (`keyring://alias` and `@file` without braces) only applies to the fields that pass
> through the CLI resolver. The compatibility matrix is in
> [REFERENCE.md](../../REFERENCE.md#value-resolution).

## What a job file you export carries

`--export-job` writes the **reference**, not the value behind it — even when the value resolves:

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

The same holds for `${{keyring://alias}}`, and whether the pipeline came from a command line or
from another job file — which is what makes an exported job safe to commit, and the only reason it
is. A connection string you wrote as a **literal** is exported as a literal; nothing masks it for
you.

## What reaches the logs

A value taken from the keyring is resolved **before** dtpipe decides which adapter handles it, so a
message about that decision is holding the real credential. Every connection string dtpipe prints —
the pipeline panel, a routing failure, the MCP tools' plan, the agent's approval dialog — is passed
through a redaction that shows only keys known to carry no credential and masks the rest:

```bash
dtpipe -i "Host=db.internal;Database=app;Username=etl;Password=hunter2" -o out.csv
```

```
No reader factory resolved for input 'Host=db.internal;Database=app;Username=etl;Password=***'
```

Masking on the unknown rather than the known is deliberate: a key nobody anticipated is hidden, at
the cost of a less specific diagnostic. A driver's own option that dtpipe has never heard of is
therefore shown as `***`, and that is the intended answer.

> [!WARNING]
> This covers what **dtpipe** prints. A database driver that raises an error quoting its own
> connection string — Npgsql, MySqlConnector, the Oracle client — is not intercepted, and neither
> is anything your shell history keeps. Redaction narrows the exposure; it does not make a log safe
> to publish.

## CI without a keychain

A build agent has environment variables rather than a desktop keychain:

```yaml
main:
  input: "pg:Host=${{PGHOST}};Database=${{PGDATABASE}};Username=${{PGUSER}};Password=${{PGPASSWORD}}"
  output: "s3://bucket/users.parquet"
```

Object-storage credentials left unset fall back to the ambient chain — environment, shared config,
instance profile — which is usually the right answer on a managed runner. See
[Object storage](../connections/object-storage.md).

---

See also: [Running in production](production.md) · [YAML jobs](yaml-jobs.md) ·
[dtpipe and .NET](../dotnet.md) ·
[REFERENCE.md](../../REFERENCE.md#secret-management)
