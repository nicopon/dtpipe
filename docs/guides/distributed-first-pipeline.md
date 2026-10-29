# Your first distributed pipeline

> **Experimental.** The coordinator, the pipeline nodes, the runner and `dtpipe studio` may change
> or disappear without notice, and nothing here is meant for production. Read
> [Experimentation](../experimentation.md) for the limits first.

This page takes you from nothing to a first run that crosses two hosts. You need Docker with
Compose, a checkout of this repository (for `deploy/compose.yaml`) and the `dtpipe` binary,
[installed](../install.md) or built by `./build.sh` in the checkout, which writes
`dist/release/dtpipe`; you write no .NET project and no rights file.

You will start a coordinator and a runner in Docker, seed a small database on your machine, declare
that database as a **node** with `dtpipe node`, say in the coordinator who may send rows to whom,
and run a pipeline that reads the database, counts on the runner and writes the answer back through
the node. [Try a distributed pipeline](distributed-trial.md) explains what each piece is for.

## Start the coordinator and the runner

The trial of [Try a distributed pipeline](distributed-trial.md) uses the same port: stop it with
`down -v` first.

### Build the images

Until a release publishes the authentication by key, the images on GHCR lack it: build the
coordinator and the runner from the checkout, with the two `docker build` commands of
[deploy/README.md](../../deploy/README.md#the-images), tagged as they are there, then point Compose
at them:

```bash
export DTPIPE_COORDINATOR_IMAGE=dtpipe-coordinator DTPIPE_RUNNER_IMAGE=dtpipe-runner
```

The variables live in this terminal, so run the Compose commands of this page from it.

### Start them

```bash
docker compose -f deploy/compose.yaml up -d --wait
```

Everything listens on the loopback of your machine, in plain HTTP, and port 8080 of the host must
be free; do not expose it. The addresses use `localtest.me`, a public name that resolves to
`127.0.0.1`; without a network that resolves it, add `coordinator.localtest.me` to `/etc/hosts`.
The runner starts as root only long enough to install network rules (`DTPIPE_EGRESS_ALLOW`), then
runs with no capability; [The runner's network](../../deploy/README.md#the-runners-network) says
what they allow.

Every identity here is verified by a **key** the coordinator issues tokens for. On the first `up`, a
one-off service wrote two random keys into the coordinator's volume: the first administrator's and
the runner's. The coordinator added both identities from them, and the runner read its own. Print
yours:

```bash
docker compose -f deploy/compose.yaml exec coordinator cat /secrets/admin.key
```

Open <http://coordinator.localtest.me:8080> and paste the key into the sign-in form. A red banner
says that keys cross the network in clear: the recipe has no TLS, so it lets the coordinator take a
key over HTTP, which a real deployment does not. The key is the only way in as `admin`, and
`down -v` drops it with the volumes.

**Access** lists `admin` and `runner-1`, in the group `runner`, and the flow matrix is empty: nobody
may send anything to anybody. Its library holds one pipeline, `shop-orders-by-country`, which reads
a brick named `orders` on a node named `shop` and writes a brick named `report` on the same node. No
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

A host verified by a key is declared before it starts: its key is what opens it. In **Access**,
press **Add an identity**, type `shop` as the client id, leave the kind `node` and the key, type
`data` as its group, keep **exchange with the runners both ways** ticked, and press **Create**.

A **group** is a label; the **flow matrix** says which group may send rows to which. The case you
kept ticked added `data` → `runner` and `runner` → `data` to the matrix, in the same change as the
identity: the node sends orders to the runner, and the runner sends the counts back. A change
applies to the next run with no restart.

A window shows the node's key, once, with **Copy** and the way to use it; closing it forgets the
key. Read it into the variable the window names, in the terminal where the node will run (paste,
then Enter):

```bash
read -rs DTPIPE_NODE_KEY && export DTPIPE_NODE_KEY
```

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

Start the node. It names itself `shop`, finds the coordinator at its public address, and reads its
key from the environment, since a secret is never an argument:

```bash
dtpipe node --name shop --coordinator http://coordinator.localtest.me:8080 \
  --client-secret '${{DTPIPE_NODE_KEY}}' --job bricks.yaml
```

The node stays in the foreground and logs that it connected, with two bricks. **Access** shows
`shop` as connected, and **Nodes** lists `orders` and `report` under it; **preview** on a brick
reads a few rows through the node.

## Open the studio

In a second terminal, in the same folder, read the administrator's key the same way, and start
the studio with a reference to it:

```bash
read -rs DTPIPE_KEY && export DTPIPE_KEY
mkdir jobs
dtpipe studio ./jobs --coordinator http://coordinator.localtest.me:8080 --key '${{DTPIPE_KEY}}'
```

The studio signs in as `admin` with the key as it starts, and says so in the terminal. It now offers
the bricks of the nodes. **Open from library** writes `shop-orders-by-country` into `./jobs`, where
you can edit it; **Publish** saves it back to the library. The full behaviour is in
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

The same count on one machine, with no coordinator, gives the same file:

```bash
dtpipe -i sqlite:shop.db \
  --query "SELECT Country, COUNT(*) AS Orders FROM orders GROUP BY Country ORDER BY Orders DESC, Country LIMIT 5" \
  -o csv:witness.csv
diff witness.csv report.csv && echo identical
```

## What to know before you build on it

- **A sink is judged the same before and after it has served.** A file the sink overwrites is not
  read by the designer's check, so publishing the same pipeline again after the first run is
  accepted, with `report.csv` in place.
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
  restart; `down -v` removes them, and the keys with them.
- **A key is shown once.** Lost, it is not read back: in **Access**, **Keys**, then **Rotate: make
  a second key** gives the identity another, and revoking the first ends what it opened at its
  next request.

## If something sticks

| Symptom | What to do |
|:---|:---|
| `docker compose` starts the published images, or finds nothing, in a new terminal | The two image variables live in the terminal that exported them: export them again. |
| The node says its client secret names a variable that is not set | The variable is empty in the node's terminal: run `read -rs DTPIPE_NODE_KEY && export DTPIPE_NODE_KEY` there. |
| The node does not connect | Its identity must exist in **Access** first, with the name given to `--name` as client id, and the key must be that identity's. A lost key is replaced in **Keys**, **Rotate**. |
| `--client-secret` is refused | A plain value is: give a reference, `'${{NAME}}'` in quotes, `keyring://alias` or `@file`. |
| **Deploy** or **Run** is refused | Read the message: node offline, an edge missing from the matrix, a brick changed. `docker compose -f deploy/compose.yaml logs -f coordinator` says the rest. |

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
| Run the coordinator and the runner for real: volumes, databases, TLS, secrets, backup | [deploy/README.md](../../deploy/README.md) |
| Cut a job in two at the point a pipeline could be split | [`dtpipe split`](../../REFERENCE.md#offering-the-points-a-job-could-be-cut-at) |

---
See also: [Experimentation](../experimentation.md) · [Anonymization](anonymization.md) ·
[REFERENCE.md](../../REFERENCE.md#distributed-pipeline-experimental)
