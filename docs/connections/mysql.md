# MySQL / MariaDB

[← Connections](README.md) · [Documentation](../README.md)

Read and write. Prefix `mysql:` — **required**, and never inferred: `Server=host;Database=db` is
character for character what a SQL Server connection string looks like, so no content heuristic
could tell them apart. Driver: [MySqlConnector](https://mysqlconnector.net/connection-options/).

## Connection string

```
mysql:Server=localhost;Port=3306;Database=app;User ID=root;Password=…
```

## The one server-side prerequisite

`--insert-mode Bulk` (the default) uses `MySqlBulkCopy`, which is built on
`LOAD DATA LOCAL INFILE`. That needs **two** switches: the client flag, which dtpipe sets for you,
and the server's `local_infile` — **OFF by default since MySQL 8**, and `SUPER` is required to
change it.

```sql
SET GLOBAL local_infile = 1;   -- or local_infile=ON in my.cnf, to survive a restart
```

dtpipe probes `@@GLOBAL.local_infile` once per run. When it is off, it warns and falls back to
batched `INSERT` rather than failing mid-stream — correct, slower.

## Upsert needs a unique index

`--strategy Upsert` generates `INSERT … ON DUPLICATE KEY UPDATE`, which MySQL fires on the table's
own PRIMARY KEY or UNIQUE indexes. There is no `ON CONFLICT (…)` to name a target.

> [!WARNING]
> **No matching index, no upsert.** Without a unique index covering exactly the key columns the
> clause degenerates into a plain `INSERT` and duplicates accumulate. dtpipe checks for the index
> and, when it is missing, warns and falls back to an explicit `DELETE`+`INSERT` — correct, slower.
> A row conflicting on an *unrelated* `UNIQUE` column is also updated: that is MySQL's semantics,
> and it has no equivalent in the other providers.

## Type mapping worth knowing

| Source | MySQL | Note |
|:---|:---|:---|
| `Guid` | `CHAR(36)` | MySQL has no UUID type |
| `bool` | `TINYINT(1)` | a bare `TINYINT` would come back as a number |
| `decimal` | `DECIMAL(38,9)` | |
| `DateTimeOffset` | `DATETIME(6)` | **the offset is not preserved** — MySQL has no offset-carrying type |
| `string` | `LONGTEXT`, or `VARCHAR(255)` in a key | `LONGTEXT` cannot be indexed without a prefix length |

---

See also: [Write strategies](../guides/write-strategies.md) ·
[REFERENCE.md](../../REFERENCE.md#mysql)
