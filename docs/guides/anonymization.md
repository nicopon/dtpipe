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
1,Alice Martin,alice@corp.com,0612345678,FR7630006000011234567890189,52000
2,Bob Durand,bob@corp.com,0798765432,FR7630006000011234567890190,61000
```

becomes

```
id,name,email,phone,iban,salary
1,Kelly Pacocha,Nyasia_Kertzmann@hotmail.com,061234****,FR76********************189,
2,Katheryn Rodriguez,Dawson_Murphy32@yahoo.com,079876****,FR76********************190,
```

The mask pattern is positional: `######****` keeps the first six characters and replaces the last
four, which leaves an operator enough to recognise a number without being able to dial it.

## Joins have to survive — seed the generator by value

Replacing `alice@corp.com` with a random address in two tables destroys the join between them.
`--fake-seed-column` makes the generated value a function of the input value, so the **same input
always produces the same output**, in this run and in next month's:

```bash
dtpipe -i customers.csv --fake "email:internet.email" --fake-seed-column email -o c_anon.csv
dtpipe -i orders.csv    --fake "customer_email:internet.email" --fake-seed-column customer_email -o o_anon.csv
```

```
id,email,name                          order_id,customer_email,amount
1,Ernest38@yahoo.com,Alice             10,Ernest38@yahoo.com,120
2,Freda77@yahoo.com,Bob                11,Freda77@yahoo.com,80
                                       12,Ernest38@yahoo.com,45
```

Two separate runs, two separate files, and the foreign key still points where it pointed. This is
the property that makes an anonymized extract usable for anything beyond a screenshot.

| Mode | What it is a function of | Use it for |
|:---|:---|:---|
| `--fake-seed-column "email"` | The value of one column | Referential integrity across tables |
| `--fake-seed-column "region,branch"` | Several columns, composite | A key that is only unique in combination |
| `--fake-seed-row` | The row index | Reproducible files when no stable key exists |
| `--fake-seed 12345` | A global offset | Reproducing a whole run exactly |
| `--skip-null` | — | Leave NULL as NULL instead of inventing a value |

## Locale

```bash
dtpipe -i emp.csv --fake "name:name.fullName" --fake-locale fr -o emp_anon.csv
# Xavier Marchand, Épiphane Le gall
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

## Prove it before you hand it over

`--dry-run N` runs the real pipeline over N rows with the writer switched off and shows each column
before and after:

```bash
dtpipe -i emp.csv --fake "email:internet.email" --mask "phone:######****" \
  -o emp_anon.csv --dry-run 5
```

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
[COOKBOOK.md](../../COOKBOOK.md#anonymization-before-export) ·
[REFERENCE.md](../../REFERENCE.md#data-transformations)
