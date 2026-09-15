# Anonymization

[← Documentation](../README.md)

Give a developer, a vendor or a test environment data that **behaves** like production without
**being** production. The transformation happens in transit: the source is only read, and no
intermediate file holds the clear values.

## The six operations

| Flag | What it does | Example |
|:---|:---|:---|
| `--fake` | Replaces a value with a generated one that looks like it | `--fake "email:internet.email"` |
| `--mask` | Keeps part of a value, replaces the rest — `#` keeps, anything else replaces | `--mask "phone:######****"` |
| `--null` | Forces a column to NULL | `--null salary` |
| `--overwrite` | Sets one static value for every row | `--overwrite "status:redacted"` |
| `--format` | Rebuilds a value from other columns | `--format "display:{first} {last}"` |
| `--compute` | Anything the five above cannot express, in JavaScript | `--compute "age:Math.floor(row.age/10)*10"` |

`--fake` draws from [Bogus](https://github.com/bchavez/Bogus): `name.fullName`, `internet.email`,
`address.city`, `company.companyName`, `random.uuid`, `commerce.productName`… A value that is not
a known faker path is refused, naming what the dataset does have — `email:internet` is an error,
`email:internet.email` is the path.

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

The mask pattern is positional: `######****` keeps the first six characters and replaces the last
four, which leaves an operator enough to recognise a number without being able to dial it.

## Joins have to survive — seed the generator by value

Replacing `alice@corp.com` with a random address in two tables destroys the join between them.
`--fake-seed-column` makes the generated value a function of an input value, so the **same input
always produces the same output**.

Seed on the **stable key**, not on the column being replaced. A `customer_id` does not change; an
email address does, and seeding on it means a customer who changes address becomes a different
person in the anonymized extract.

`customers.csv` and `orders.csv` — the key is `customer_id`, present in both:

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

Two separate runs, two separate files, and the foreign key still points where it pointed. The
column names differ — `email` here, `customer_email` there — and that is fine: the name plays no
part, only the seed value does.

> [!WARNING]
> **The generated value depends on the whole `--fake` set of the run, not just on the seed.**
> Adding a second `--fake` mapping, or reordering two of them, changes what the first one produces
> — so two pipelines that must agree have to declare **the same mappings in the same order**. In
> practice today that means one `--fake` per pipeline, as above. Verify a cross-table run before
> relying on it: anonymize both sides, then join the two outputs and check the row count.

| Mode | What it is a function of | Use it for |
|:---|:---|:---|
| `--fake-seed-column "customer_id"` | The value of one column | Referential integrity across tables |
| `--fake-seed-column "region,branch"` | Several columns, composite | A key that is only unique in combination |
| `--fake-seed-row` | The row index | Reproducible files when no stable key exists |
| `--fake-seed 12345` | A global offset | Reproducing a whole run exactly |
| `--skip-null` | — | Leave NULL as NULL instead of inventing a value |

`--fake-seed-row` and `--fake-seed-column` are mutually exclusive, and using both is refused.

## Locale

```bash
dtpipe -i emp.csv --fake "name:name.fullName" --fake-locale fr -o emp_anon.csv
# Éloïse Martinez, Ascelin Morel
```

## Generating a column that does not exist

`--fake` maps a column that is not in the incoming rows by **creating** it. The same flag therefore
anonymizes what is there and synthesizes what is not, which is how a realistic dataset is built
from nothing:

```bash
dtpipe -i generate:10000 \
  --fake "customer_id:random.uuid" \
  --fake "name:name.fullName" \
  --fake "city:address.city" \
  --fake "signed_up:date.past" \
  --drop GenerateIndex \
  -o customers.parquet
```

That is also the trap behind the warning above: a mapping whose column is absent does not fail, it
adds a column — so "declare the same mappings on both sides" cannot be done by copying a flag list
between two tables that do not have the same columns.

## Prove it before you hand it over

`--dry-run N` runs the real pipeline over N rows with the writer switched off, and shows each
column at each step:

```bash
dtpipe -i emp.csv --fake "email:internet.email" --mask "phone:######****" \
  -o emp_anon.csv --dry-run 2
```

```
                      Pipeline Trace Analysis — Record 1/2
╭──────────┬────────────────┬────────────────┬────────────────┬────────────────╮
│ Column   │ Input          │ Fake (Step 1)  │ Mask (Step 2)  │ Output         │
├──────────┼────────────────┼────────────────┼────────────────┼────────────────┤
│ name     │ Alice Martin   │ Alice Martin   │ Alice Martin   │ String ->      │
│          │ (String)       │ (String)       │ (String)       │ STRING         │
│ email    │ alice@corp.com │ Kurt_Gerhold26 │ Kurt_Gerhold26 │ String ->      │
│          │ (String)       │ @hotmail.com   │ @hotmail.com   │ STRING         │
│          │                │ (String)       │ (String)       │                │
│ phone    │ 0612345678     │ 0612345678     │ 061234****     │ String ->      │
│          │ (String)       │ (String)       │ (String)       │ STRING         │
╰──────────┴────────────────┴────────────────┴────────────────┴────────────────╯
```

One column per step, so you can see *where* a value changed — `email` at step 1, `phone` at step 2
— and confirm that a column you expected to be touched actually was. **Output** is the type
mapping to the target, not a value; a dash or `Missing` there means the target column does not
exist yet.

Nothing is written, so the check costs nothing. See
[Preview and checkpoints](preview-and-checkpoints.md).

## Make it repeatable

Anonymization rules are policy: they belong in a file that is reviewed, not in a shell history.

```bash
dtpipe -i emp.csv --fake "name:name.fullName" --fake "email:internet.email" \
  --null salary -o emp_anon.csv --export-job anonymize-employees.yaml
```

```yaml
main:
  input: emp.csv
  transformers:
  - type: fake
    mappings:
      name: name.fullName
  - type: fake
    mappings:
      email: internet.email
  - type: null
    mappings:
      salary: ''
  output: emp_anon.csv
  batch-size: 32768
  sampling-rate: 1
```

Edit it, commit it, run it with `dtpipe --job anonymize-employees.yaml`.

> [!TIP]
> One `fake` transformer can carry several mappings and one set of options, which the YAML form
> expresses directly — `mappings:` with several columns under a single `options: {locale: fr}`.
> That is the shape to reach for when one policy covers several columns.

## What this is, and what it is not

- The **source is never modified**: a pipeline reads it and writes elsewhere.
- You choose the columns. Nothing scans for personal data on your behalf, and a free-text comment
  column that quotes a customer's name stays as it is unless you handle it.
- Masking and faking are not anonymity proofs. A dataset with a rare combination of quasi-
  identifiers — postcode, birth date, job title — can still be re-identified even with names
  replaced. Drop what you do not need (`--project`, `--drop`), and reduce precision where you can
  (`--compute` to round a date or a salary into a band).

---

See also: [SQL and JavaScript](sql-and-javascript.md) ·
[Preview and checkpoints](preview-and-checkpoints.md) ·
[Troubleshooting](../troubleshooting.md) ·
[REFERENCE.md](../../REFERENCE.md#data-transformations)
