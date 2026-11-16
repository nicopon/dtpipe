# DtPipe

A command-line tool that moves data between databases, files and object storage, and transforms or
anonymizes it on the way. One self-contained binary; nothing is staged in between.

## Install

```bash
dotnet tool install -g dtpipe        # or download a binary from the releases page
dtpipe --help
```

Linux, macOS and Windows, x64 and arm64. The .NET runtime, DuckDB and every database driver are
inside the binary. Details: [docs/install.md](./docs/install.md).

## Example

Anonymize a table on the way out, so the clear values never reach the disk:

```bash
dtpipe \
  -i "pg:Host=localhost;Database=prod;Username=postgres" \
  --query "SELECT * FROM users" \
  --fake "email:internet.email" \
  --fake "name:name.fullName" \
  --mask "phone:###-****" \
  --null "ssn" \
  -o anonymized_users.parquet
```

A first pipeline in six steps, with no database to set up:
[docs/quickstart.md](./docs/quickstart.md).

## What it connects to

| Family | Prefix |
|:---|:---|
| Databases | `pg:`, `mysql:`, `sqlite:`, `duck:`, `mssql:`, `ora:` |
| Files | `csv:`, `jsonl:`, `parquet:`, `arrow:`, `xml:`, or just the file extension |
| Object storage | `s3://bucket/key.parquet`, `azure://container/blob.csv` |
| Test endpoints | `generate:N` (a source), `null:` and `checksum:` (sinks) |

`dtpipe providers` lists what your binary carries.

## Documentation

| | |
|:---|:---|
| [**docs/**](./docs/README.md) | **Start here**: install, quickstart, concepts, a guide per task, a page per connection |
| [REFERENCE.md](./REFERENCE.md) | The flags in common use, the YAML job schema, provider options, the MCP tools. `dtpipe --help` lists every flag |
| [EXTENDING.md](./EXTENDING.md) | Writing a new adapter or transformer |

Beyond the command line: one pipeline across several hosts
([docs/distributed.md](./docs/distributed.md)), `dtpipe studio` to design and preview jobs in a
local page, with an AI assistant if you give it a model, and `dtpipe mcp` for an assistant you
already use.

## License

MIT
