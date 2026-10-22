# Your first distributed pipeline

> **Experimental.** The coordinator, the pipeline nodes, the runner and `dtpipe studio` may change
> or disappear without notice, and nothing here is meant for production. Read
> [Experimentation](../experimentation.md) for the limits first.

This page takes you from nothing to a first run that crosses two hosts. You need Docker with
Compose, a checkout of this repository (for `deploy/compose.yaml`) and the `dtpipe` binary; you
write no .NET project and no rights file.

You will start a coordinator and a runner in Docker, seed a small database on your machine, declare
that database as a **node** with `dtpipe node`, say in the coordinator who may send rows to whom,
and run a pipeline that reads the database, counts on the runner and writes the answer back through
the node. [Try a distributed pipeline](distributed-trial.md) explains what each piece is for.

## Start the coordinator and the runner

```bash
docker compose -f deploy/compose.yaml up -d --wait
```

Everything listens on the loopback of your machine, in plain HTTP. The addresses use
`localtest.me`, a public name that resolves to `127.0.0.1`; without a network that resolves it, add
`coordinator.localtest.me` to `/etc/hosts`.

Open <http://coordinator.localtest.me:8080> and sign in as `admin` with the password
`dtpipe-trial-password`. The coordinator is its own trial identity provider: it accepts any user
name with that one password, which is also every service's secret, and a red banner says it is not
for production. Only `admin` may change anything.

The coordinator starts with **no rights**: **Access** lists no identity and no group, and nobody may
send anything to anybody. Its library holds one pipeline, `shop-orders-by-country`, which reads a
brick named `orders` on a node named `shop` and writes a brick named `report` on the same node. No
such node exists yet.

## Seed a database

`generate` makes rows and `--fake` fills columns with believable values. A seed makes them the same
on every run. This writes a SQLite database of a thousand orders into a folder of its own:

```bash
mkdir shop && cd shop
dtpipe -i generate:1000 --fake "Country:address.countryCode" --fake "Product:commerce.product" \
  --fake-seed 7 --rename "GenerateIndex:OrderId" -o sqlite:shop.db --table orders
dtpipe -i sqlite:shop.db --query "SELECT * FROM orders ORDER BY OrderId LIMIT 3" -o first.csv
cat first.csv
```

```text
OrderId,Country,Product
0,HM,Pizza
1,NI,Car
2,GU,Chicken
```

Stay in this folder for the rest of the page.

## Say who may do what

The trial identity provider knows only the services the rights name, so you declare a host in
**Access** before you start it. With an external identity provider an unknown host would wait in
**Waiting for approval** and you would accept it there; the rights are the same.

In **Access**, press **Add an identity** twice:

| Client id | Groups |
|:---|:---|
| `runner-1` | `runner` |
| `shop` | `data` |

A **group** is a label; the **flow matrix** says which group may send rows to which. In the matrix
below the identities, tick `runner` → `data` and `data` → `runner`, and press **Apply**. The
pipeline needs both: the node sends orders to the runner, and the runner sends the counts back. A
change applies to the next run with no restart.

The runner started with the coordinator and was refused until now. It retries, and **runner-1**
shows as connected within seconds.

## Declare a node on the database

A node's **bricks** are the reads and writes you offer; the coordinator never holds a connection
string. `dtpipe node` takes them from a job file: a branch that only reads is a source, one that
only writes is a sink, and the branch's name is the brick's id. Write the file with absolute paths,
since a fragment runs in a working directory of the node's own, not in yours:

```bash
cat > bricks.yaml <<EOF
orders:
  input: sqlite:$PWD/shop.db
  provider-options:
    sqlite:
      query: SELECT * FROM orders
report:
  output: csv:$PWD/report.csv
EOF
```

Start the node. It names itself `shop`, finds the coordinator at its public address and reads its
secret from the environment, since a secret is never an argument:

```bash
export DTPIPE_TRIAL_PASSWORD=dtpipe-trial-password
dtpipe node --name shop --coordinator http://coordinator.localtest.me:8080 \
  --client-secret '${{DTPIPE_TRIAL_PASSWORD}}' --job bricks.yaml
```

The node stays in the foreground and logs that it connected, with two bricks. **Access** shows
`shop` as connected, and **Nodes** lists `orders` and `report` under it.

## Open the studio

In a second terminal, in the same folder:

```bash
dtpipe studio ./jobs --coordinator http://coordinator.localtest.me:8080
```

**Sign in** opens the identity provider in a new tab: sign in as `admin`. The studio now offers the
bricks of the nodes. **Open from library** writes `shop-orders-by-country` into `./jobs`, where you
can edit it; **Publish** saves it back to the library. The full behaviour is in
[REFERENCE.md](../../REFERENCE.md#studio-dtpipe-studio).

## Arm it, run it, read the result

On the coordinator's pages open `shop-orders-by-country` in the library and press **Deploy**, which
arms it: the coordinator checks that `shop` offers both bricks, samples `orders` and keeps the
version it saw, checks that the matrix allows every edge, and keeps that version ready to launch.
Press **Run**. The run's report in **Runs** names the two fragments, the node each ran on and the
rows that crossed each edge: a thousand orders from `shop` to the runner, five counts back. Then
read what the sink wrote:

```bash
cat report.csv
```

```text
Country,Orders
CC,11
CK,10
GF,10
IQ,10
BO,9
```

## What to know before you build on it

- **A brick's title is its id.** A command line has no place for a title or a description, so the
  studio shows `orders` and `report` as they are.
- **A command line carries sources, or one sink.** A job file with `--job` carries the mix, as here.
  A branch that reads another, transforms, or reads and writes at once is refused: everything
  between a read and a write belongs on the runner.
- **The step is on the runner.** The count ran there, not on the node. The node only reads and
  writes, so a join across two nodes needs no more of it.
- **A brick's version is the hash of its text.** Change `bricks.yaml`, restart the node, and the
  next run of the pipeline you armed is refused with `brick-changed` until you save it again, which
  pins the new version.
- **The check for a secret in a brick is best effort.** It reads the keys it knows as credentials
  and nothing more; keep a password out of the file with a `${{NAME}}` or `keyring://` reference.
- **A cursor is the coordinator's too.** An incremental load keeps its position there, not in a
  file on the node; [Incremental loading](incremental.md#in-a-distributed-pipeline) says what is
  served and what is not.
- **The rights you declared are the coordinator's.** They are kept in its volume and survive a
  restart; `down -v` removes them.

## Stop it

Press Ctrl+C in the node's terminal, then:

```bash
docker compose -f deploy/compose.yaml down -v
```

## Where to go next

| I want to… | Read |
|:---|:---|
| Know what each piece is for, and run the trial in Docker alone | [Try a distributed pipeline](distributed-trial.md) |
| Declare bricks on a real database | [`dtpipe node`](../../REFERENCE.md#a-node-from-the-cli-dtpipe-node) |
| Run the coordinator and the runner for real: volumes, Redis, TLS, secrets, backup | [deploy/README.md](../../deploy/README.md) |
| Cut a job in two at the point a pipeline could be split | [`dtpipe split`](../../REFERENCE.md#offering-the-points-a-job-could-be-cut-at) |

---
See also: [Experimentation](../experimentation.md) · [Anonymization](anonymization.md) ·
[REFERENCE.md](../../REFERENCE.md#distributed-pipeline-experimental)
