# Anonymization

[← Documentation](../README.md)

Give a developer, a vendor or a test environment data that behaves like production without being
production. The data is transformed on its way through: the source is only read, and no file holds
the clear values.

## A run, end to end

`emp.csv`

```
id,name,email,phone,iban,salary
1,Alice Martin,alice@corp.com,0612345678,FR7630006000011234567890189,52000
2,Bob Durand,bob@corp.com,0798765432,FR7630006000011234567890190,61000
```

```bash
dtpipe -i emp.csv \
  --fake "name:name.fullName" \
  --fake "email:internet.email" \
  --mask "phone:######****" \
  --mask "iban:####********************###" \
  --null salary \
  -o emp_anon.csv
```

```
id,name,email,phone,iban,salary
1,Marge Quitzon,Yoshiko.Quigley@yahoo.com,061234****,FR76********************189,
2,Krista Simonis,Roxane_Konopelski@yahoo.com,079876****,FR76********************190,
```

| Flag | What it does | Example |
|:---|:---|:---|
| `--fake` | Replaces a value with a realistic generated one | `--fake "email:internet.email"` |
| `--mask` | Keeps some characters: `#` keeps, any other character replaces | `--mask "phone:######****"` |
| `--null` | Sets a column to NULL | `--null salary` |
| `--overwrite` | Sets the same value on every row | `--overwrite "status:redacted"` |
| `--format` | Rebuilds a value from other columns | `--format "display:{first} {last}"` |
| `--compute` | Anything else, in JavaScript | `--compute "age:Math.floor(row.age/10)*10"` |

`--fake` uses [Bogus](https://github.com/bchavez/Bogus): `name.fullName`, `internet.email`,
`address.city`, `company.companyName`, `random.uuid`… An unknown path is refused, and the message
lists the valid ones. `--fake-locale fr` generates French names and addresses.

## Keep joins working

A random email in two tables breaks the join between them. `--fake-seed-column` makes the fake
value depend on another column, so the same input always gives the same output. Seed on a stable
key, such as a customer id, not on the column you replace.

`customers.csv` and `orders.csv` share `customer_id`:

```
customer_id,email,name                order_id,customer_id,customer_email,amount
C1,alice@corp.com,Alice Martin        10,C1,alice@corp.com,120
C2,bob@corp.com,Bob Durand            11,C2,bob@corp.com,80
                                      12,C1,alice@corp.com,45
```

```bash
dtpipe -i customers.csv --fake "email:internet.email" \
       --fake-seed-column customer_id -o customers_anon.csv

dtpipe -i orders.csv    --fake "customer_email:internet.email" \
       --fake-seed-column customer_id -o orders_anon.csv
```

```
customer_id,email,name                       order_id,customer_id,customer_email,amount
C1,Laron.Rice85@gmail.com,Alice Martin        10,C1,Laron.Rice85@gmail.com,120
C2,Erica41@yahoo.com,Bob Durand               11,C2,Erica41@yahoo.com,80
                                              12,C1,Laron.Rice85@gmail.com,45
```

Two runs, two files, and the join still holds.

| Option | Same output for | Use it for |
|:---|:---|:---|
| `--fake-seed-column "customer_id"` | The same value in that column | Joins across tables |
| `--fake-seed-column "region,branch"` | The same combination | A key made of several columns |
| `--fake-seed-row` | The same row number | Reproducible files with no stable key |
| `--fake-seed 12345` | The same whole run | Reproducing a run exactly |
| `--skip-null` | — | Leaving a NULL as NULL |

With `--fake-seed` alone, adding or reordering a `--fake` changes the other columns' values. With
`--fake-seed-column` or `--fake-seed-row`, it does not. Those two cannot be combined.

## Generate data from nothing

On a column that does not exist, `--fake` creates it:

```bash
dtpipe -i generate:10000 \
  --fake "customer_id:random.uuid" \
  --fake "name:name.fullName" \
  --fake "city:address.city" \
  --fake "signed_up:date.past" \
  --drop GenerateIndex \
  -o customers.parquet
```

So a mapping on a misspelled column adds a column instead of failing: check the result.

## Check before you hand it over

`--dry-run N` shows each column after each step, and writes nothing:

```bash
dtpipe -i emp.csv --fake "email:internet.email" --mask "phone:######****" \
  -o emp_anon.csv --dry-run 2
```

```
                      Pipeline Trace Analysis — Record 1/2
╭──────────┬────────────────┬────────────────┬────────────────┬────────────────╮
│ Column   │ Input          │ Fake (Step 1)  │ Mask (Step 2)  │ Output         │
├──────────┼────────────────┼────────────────┼────────────────┼────────────────┤
│ id       │ 1 (String)     │ 1 (String)     │ 1 (String)     │ String         │
│ name     │ Alice Martin   │ Alice Martin   │ Alice Martin   │ String         │
│          │ (String)       │ (String)       │ (String)       │                │
│ email    │ alice@corp.com │ Desmond_West@y │ Desmond_West@y │ String         │
│          │ (String)       │ ahoo.com       │ ahoo.com       │                │
│          │                │ (String)       │ (String)       │                │
│ phone    │ 0612345678     │ 0612345678     │ 061234****     │ String         │
│          │ (String)       │ (String)       │ (String)       │                │
│ iban     │ FR763000600001 │ FR763000600001 │ FR763000600001 │ String         │
│          │ 1234567890189  │ 1234567890189  │ 1234567890189  │                │
│          │ (String)       │ (String)       │ (String)       │                │
│ salary   │ 52000 (String) │ 52000 (String) │ 52000 (String) │ String         │
╰──────────┴────────────────┴────────────────┴────────────────┴────────────────╯
```

`email` changed at step 1, `phone` at step 2, and `iban` was not touched. **Output** is the type
written to the target.

## Make it a reviewed policy

Save the rules as a job file, review it, and run it with `--job`:

```bash
dtpipe -i emp.csv --fake "name:name.fullName" --fake "email:internet.email" \
  --null salary -o emp_anon.csv --export-job anonymize-employees.yaml
```

See [Jobs and production](production.md).

## Limits

- You choose the columns. Nothing scans for personal data, and a free-text column that quotes a
  name stays as it is.
- Masking and faking do not prove anonymity: a rare combination of postcode, birth date and job
  title can still identify someone. Drop what you do not need (`--drop`, `--project`) and round
  what you can (`--compute`).

---

See also: [Preview and checkpoints](preview-and-checkpoints.md) ·
[REFERENCE.md](../../REFERENCE.md#data-transformations)
