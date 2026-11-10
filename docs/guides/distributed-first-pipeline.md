# Your first distributed pipeline

> **Experimental.** The coordinator, the pipeline nodes, the runner and `dtpipe studio` may change
> or disappear without notice, and nothing here is meant for production. Read
> [Experimentation](../experimentation.md) for the limits first.

This page takes you from nothing to a first run that crosses two hosts. You need the `dtpipe`
binary, [installed](../install.md) or built by `./build.sh` in a checkout, and the coordinator,
which `./build.sh` and the releases publish as a folder; or Docker with Compose and a checkout of
this repository, for `deploy/compose.yaml`. You write no .NET project and no rights file.

You will start a coordinator and a runner, seed a small database on your machine, declare
that database as a **node** with `dtpipe node`, say in the coordinator who may send rows to whom,
and run a pipeline that reads the database, counts on the runner and writes the answer back through
the node. It ends with a scheduler that starts the same run from a script, and the studio, which
designs the job as cards.

## What the pieces are for

| Piece | What it does |
|:---|:---|
| **Coordinator** | The control plane. It knows the connected nodes, keeps the pipelines in a git-backed **library**, decides which fragment runs where, starts a run and gives it one verdict. Its pages show the library, the runs and the rights. |
| **Node** | A host that carries data. It offers **bricks**: reads and writes its owner declared on its own database, so the coordinator never holds a connection string. |
| **Runner** | A node with no data. It runs every step between bricks: a join, a SQL query, an anonymization. |
| **Studio** | `dtpipe studio`, a local page where you design a job as cards and sample it on the real engine. Given a coordinator, it also offers the nodes' bricks and publishes the job to the library. |

Who may do what is the coordinator's own configuration: its **rights** list the nodes, the group
each belongs to and which group may send rows to which. There is no identity provider here: every
identity is verified by a key the coordinator issues tokens for.

## Start the coordinator and the runner

Everything here listens on the loopback of your machine, in plain HTTP, so a key crosses the
network in clear and a red banner says so on every page: do not expose it. Start the two hosts
as programs, or as containers; the rest of the page is the same for both.

### With the programs

You need `git` on the `PATH` (the coordinator keeps its library as a git repository), `dtpipe`, and
the coordinator: a folder whose `dtpipe-coordinator` runs from where it is unpacked, with `wwwroot`
and the SQLite library beside it. Both are on the
[Releases](https://github.com/nicopon/dtpipe/releases) page, one archive per platform; in a
checkout, `./build.sh` writes them to `dist/release`. The commands below are for a POSIX shell; in
PowerShell, set a variable with `$env:NAME = 'value'`.

Work in a folder of its own. Every identity is verified by a **key**, and `dtpipe keygen` makes
one. The coordinator keeps only the hash of the keys it is given, so each file is yours to keep:

```bash
mkdir dtpipe-lab && cd dtpipe-lab
dtpipe keygen admin --out admin.key
dtpipe keygen runner-1 --out runner-1.key
```

The coordinator starts with a library of its own, which it takes from a folder on its first start.
Write the one pipeline this page runs; it reads a brick named `orders` on a node named `shop`, that
does not exist yet, and writes a brick named `report` on the same node:

```bash
mkdir library
cat > library/shop-orders-by-country.yaml <<'EOF'
# Orders by country
# Reads the orders brick of a node named shop, counts them per country on the runner, and writes the
# counts through the node's report brick. docs/guides/distributed-first-pipeline.md declares both.
orders:
  input: brick:shop/orders
by-country:
  from: orders
  provider-options:
    sql:
      query: SELECT Country, COUNT(*) AS Orders FROM orders GROUP BY Country ORDER BY Orders DESC, Country LIMIT 5
write:
  from: by-country
  output: brick:shop/report
EOF
cat > library/shop-orders-by-country.bindings.json <<'EOF'
{
  "orders": {
    "mode": "strict"
  },
  "write": {
    "mode": "strict"
  }
}
EOF
```

Start the coordinator in the background, giving the path of its folder. Its settings are
environment variables: it begins on an empty store, is reached at `http://127.0.0.1:8080`, accepts
a key over plain HTTP, lets `dtpipe studio` sign in with a key, and adds the administrator and the
runner from the files above, the runner in the group `runner`:

```bash
export COORDINATOR_URL=http://127.0.0.1:8080
Coordinator__Initialize=true \
Coordinator__PublicUrl=$COORDINATOR_URL \
Coordinator__Auth__AllowHttpKeys=true \
Coordinator__Studio__ClientId=dtpipe-studio \
Coordinator__StateDirectory=$PWD/coordinator-state \
Coordinator__LibrarySeed=$PWD/library \
Coordinator__Bootstrap__AdminKey=@$PWD/admin.key \
Coordinator__Bootstrap__Identities__0__Id=runner-1 \
Coordinator__Bootstrap__Identities__0__Name=runner-1 \
Coordinator__Bootstrap__Identities__0__Groups__0=runner \
Coordinator__Bootstrap__Identities__0__Key=@$PWD/runner-1.key \
ASPNETCORE_URLS=$COORDINATOR_URL \
  path/to/coordinator/dtpipe-coordinator > coordinator.log 2>&1 &
echo $! > coordinator.pid
```

Then the runner, a node with no data. It reads its key from the file, never from the command line:

```bash
dtpipe node --runner --name runner-1 --coordinator "$COORDINATOR_URL" \
  --client-secret "@$PWD/runner-1.key" > runner.log 2>&1 &
echo $! > runner.pid
```

The runner runs whatever step a pipeline holds, SQL and JavaScript included, with your rights on
your machine and no limit on the network. That is fine for a trial where you are the only
administrator; to confine it, run it in a container, as below.

Print the administrator's key:

```bash
cat admin.key
```

### Or in Docker

You need Docker with Compose and a checkout of this repository, for `deploy/compose.yaml`. Port
8080 of the host must be free. The addresses use `localtest.me`, a public name that resolves to
`127.0.0.1`; without a network that resolves it, add `coordinator.localtest.me` to `/etc/hosts`.

Until a release publishes the authentication by key, the images on GHCR lack it: build the
coordinator and the runner from the checkout, with the two `docker build` commands of
[deploy/README.md](../../deploy/README.md#the-images), tagged as they are there, then point Compose
at them. The variables live in this terminal, so run the Compose commands of this page from it:

```bash
export DTPIPE_COORDINATOR_IMAGE=dtpipe-coordinator DTPIPE_RUNNER_IMAGE=dtpipe-runner
export COORDINATOR_URL=http://coordinator.localtest.me:8080
docker compose -f deploy/compose.yaml up -d --wait
```

On the first `up`, a one-off service writes a random key for the first administrator and one for
the runner into the coordinator's volume; the coordinator adds both identities from them, and the
runner reads its own. Its library already holds the pipeline of this page. The runner starts as
root only long enough to install network rules (`DTPIPE_EGRESS_ALLOW`) that limit it to Docker's
local address pools and let nothing in, then runs with no capability;
[The runner's network](../../deploy/README.md#the-runners-network) says what they allow. Print the
administrator's key:

```bash
docker compose -f deploy/compose.yaml exec coordinator cat /secrets/admin.key
```

### Sign in

Open the address in `COORDINATOR_URL` and paste the administrator's key into the sign-in form. The
key is the only way in as `admin`. **Access** lists `admin` and `runner-1`, in the group `runner`,
and the flow matrix is empty: nobody may send anything to anybody. The library holds one pipeline,
`shop-orders-by-country`, which reads a brick named `orders` on a node named `shop` and writes a
brick named `report` on the same node. No such node exists yet.

In every other terminal of this page, export `COORDINATOR_URL` again.

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
`data` as its group, and press **Create**.

A **group** is a label; the **flow matrix** says which group may send rows to which, and `*` stands
for any group. In the matrix below, tick `*` → `runner`, `runner` → `*`, and `data` → `data`, then
press **Apply**: any group may use the runner, and `data` may send rows that return to `data`. When
rows cross an intermediate runner, the matrix requires authorization from each source group all the
way to the target groups. A change applies to the next run with no restart.

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
dtpipe node --name shop --coordinator "$COORDINATOR_URL" \
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
dtpipe studio ./jobs --coordinator "$COORDINATOR_URL" --key '${{DTPIPE_KEY}}'
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

## Start the run from a scheduler

A **launcher** is a service identity that may start the runs of the pipelines it lists, and
nothing else. In **Access**, press **Add an identity**, type `launcher-1` as the client id, choose
the kind `launcher`, type `shop-orders-by-country` as the pipeline it may launch, and press
**Create**. Read the key the window shows, once, into a variable (paste, then Enter):

```bash
read -rs LAUNCHER_KEY
```

The launcher exchanges its key for a token at the coordinator and posts the run to
`/service/runs`; the pipeline must be armed first, as above, or the coordinator answers 409. This
needs `curl` and `jq`:

```bash
TOKEN=$(curl -s "$COORDINATOR_URL/connect/token" \
  -d grant_type=client_credentials -d client_id=launcher-1 \
  -d "client_secret=$LAUNCHER_KEY" | jq -r .access_token)
curl -s -X POST "$COORDINATOR_URL/service/runs" \
  -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d '{"pipelineId": "shop-orders-by-country"}'
```

The answer carries a `runId`. Read the run's `state` with `GET /service/runs/<runId>` and the
same header until it is `succeeded` or `failed`.

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
- **The rights you declared are the coordinator's.** They are kept in its state directory, or in
  its volume with Docker, and survive a restart. Delete `coordinator-state` (or `down -v`) to begin
  again, with new keys.
- **A key is shown once.** Lost, it is not read back: in **Access**, **Keys**, then **Rotate: make
  a second key** gives the identity another, and revoking the first ends what it opened at its
  next request.
- **WAN and network buffer tuning.** By default, node hosts exchange data over TransportR using
  256 KB batches with up to 16 batches in-flight (a 4 MB window), compressed with Arrow IPC LZ4.
  Over high-latency or cross-region WAN links (e.g. cloud runners with RTT > 30 ms), setting
  `DtPipeNode__MaxInFlightBatches=32` (an 8 MB in-flight window) and optionally
  `DtPipeNode__BatchBytes=524288` (512 KB) prevents network stalls and maximizes throughput.

## If something sticks

| Symptom | What to do |
|:---|:---|
| A command has no address, or the page does not open | `COORDINATOR_URL` lives in the terminal that exported it: export it again. |
| The coordinator stops at start with "is not a key of this coordinator" | A key file holds something `dtpipe keygen` did not make, or an extra line: make it again with `dtpipe keygen <id> --out <file>`, the id being the identity's. |
| `docker compose` starts the published images, or finds nothing, in a new terminal | The two image variables live in the terminal that exported them: export them again. |
| The node says its client secret names a variable that is not set | The variable is empty in the node's terminal: run `read -rs DTPIPE_NODE_KEY && export DTPIPE_NODE_KEY` there. |
| The node does not connect | Its identity must exist in **Access** first, with the name given to `--name` as client id, and the key must be that identity's. A lost key is replaced in **Keys**, **Rotate**. |
| `--client-secret` is refused | A plain value is: give a reference, `'${{NAME}}'` in quotes, `keyring://alias` or `@file`. |
| **Deploy** or **Run** is refused | Read the message: node offline, an edge missing from the matrix, a brick changed. `docker compose -f deploy/compose.yaml logs -f coordinator` says the rest. |

## Stop it

Press Ctrl+C in the node's terminal. With the programs, stop the two in the background, and delete
`coordinator-state` to begin again from an empty coordinator, with new keys if you make them again:

```bash
kill $(cat coordinator.pid runner.pid)
```

With Docker:

```bash
docker compose -f deploy/compose.yaml down -v
```

## Where to go next

| I want to… | Read |
|:---|:---|
| See every setting of the coordinator and of a node host | [REFERENCE.md](../../REFERENCE.md#distributed-pipeline-experimental) |
| Make my own .NET service a node that offers bricks | [`AddDtPipeNode`](../../src/DtPipe.PipelineNode.Hosting/CLAUDE.md) |
| Watch four data nodes, a join across them, rights and faults | [The distributed lab](../../samples/distributed-lab/README.md) |
| Declare bricks on a real database | [`dtpipe node`](../../REFERENCE.md#a-node-from-the-cli-dtpipe-node) |
| Run the coordinator and the runner for real: volumes, databases, TLS, secrets, backup | [deploy/README.md](../../deploy/README.md) |
| Cut a job in two at the point a pipeline could be split | [`dtpipe split`](../../REFERENCE.md#offering-the-points-a-job-could-be-cut-at) |

---
See also: [Experimentation](../experimentation.md) · [DAG pipelines](dag.md) ·
[Anonymization](anonymization.md) ·
[REFERENCE.md](../../REFERENCE.md#distributed-pipeline-experimental)
