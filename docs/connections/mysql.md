# MySQL / MariaDB

[← Connections](README.md) · [Documentation](../README.md)

Read and write, prefix `mysql:`, always required. Driver:
[MySqlConnector](https://mysqlconnector.net/connection-options/).

```
mysql:Server=localhost;Port=3306;Database=app;User ID=root;Password=…
```

## Bulk loading needs `local_infile`

The default bulk load uses `LOAD DATA LOCAL INFILE`, which the server must allow. It is off by
default since MySQL 8:

```sql
SET GLOBAL local_infile = 1;   -- or local_infile=ON in my.cnf
```

When it is off, dtpipe warns and uses batched `INSERT` instead: correct, slower.

## Upsert needs a unique index

`--strategy Upsert` uses `INSERT … ON DUPLICATE KEY UPDATE`, which only works with a primary key or
unique index on exactly the key columns. Without one, dtpipe warns and uses `DELETE` then `INSERT`.
A row that conflicts on another unique column is also updated: that is how MySQL works.

## Types

| Source | Written as | Note |
|:---|:---|:---|
| UUID | `CHAR(36)` | MySQL has no UUID type |
| Boolean | `TINYINT(1)` | |
| Decimal | `DECIMAL(p,s)` as declared, else `DECIMAL(38,18)` | |
| Timestamp with offset | `DATETIME(6)` | The offset is lost |
| Text | `LONGTEXT`, or `VARCHAR(255)` in a key | |

A `TIME` beyond 24 hours or below zero is a duration, not a time of day: reading it stops the run.
Cast it to text in the query.

---

See also: [Writing to a database](../guides/write-strategies.md) ·
[REFERENCE.md](../../REFERENCE.md#mysql)
