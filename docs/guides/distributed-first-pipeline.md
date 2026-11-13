# Your first distributed pipeline

What the pieces are and their limits: [Distributed pipelines](../distributed.md).

You will start a coordinator and a runner, declare a small database as a **node**, allow the rows
to flow, and run a pipeline that reads the database, counts on the runner, and writes the result
back through the node. Then you will start the same run from a script.

You need the `dtpipe` binary and either the coordinator, or Docker with Compose and a checkout of
this repository. The coordinator is the folder `coordinator/` of the release archive
`dtpipe-coordinator-<platform>`, or `dist/release/coordinator/` after `./build.sh`; it needs `git`
on the `PATH` ([deploy/README.md](../../deploy/README.md)).

Everything here listens on your machine's loopback in plain HTTP, so keys cross it in clear: do not
expose it. The commands are for a POSIX shell.

## 1. Start the coordinator and the runner

### With the programs

Work in a folder of your own. Every identity is verified by a key, made with `dtpipe keygen`:

```bash
mkdir dtpipe-lab && cd dtpipe-lab
dtpipe keygen admin --out admin.key
dtpipe keygen runner-1 --out runner-1.key
```

The coordinator takes its first library from a folder. Write the pipeline this page runs: it
reads the brick `orders` of a node `shop`, and writes its brick `report`.

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

Start the coordinator in the background. Its settings are environment variables: an empty store,
the address `http://127.0.0.1:8080`, keys accepted over HTTP, `dtpipe studio` allowed to sign in,
and the two identities from the files above:

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

Then the runner, which reads its key from the file:

```bash
dtpipe node --runner --name runner-1 --coordinator "$COORDINATOR_URL" \
  --client-secret "@$PWD/runner-1.key" > runner.log 2>&1 &
echo $! > runner.pid
cat admin.key        # the administrator's key, to sign in
```

The runner runs any step of a pipeline, JavaScript included, with your rights. To confine it, run
it in a container, as below.

### Or with Docker

Port 8080 must be free. The addresses use `localtest.me`, a public name for `127.0.0.1`; without a
network that resolves it, add `coordinator.localtest.me` to `/etc/hosts`.

Compose pulls the images of the latest release. With a checkout newer than that release, build
both images with the two `docker build` commands of
[deploy/README.md](../../deploy/README.md#the-images), and point Compose at them with the first
line below. Run every Compose command of this page from this terminal:

```bash
# Only with images built from the checkout:
export DTPIPE_COORDINATOR_IMAGE=dtpipe-coordinator DTPIPE_RUNNER_IMAGE=dtpipe-runner
export COORDINATOR_URL=http://coordinator.localtest.me:8080
docker compose -f deploy/compose.yaml up -d --wait
docker compose -f deploy/compose.yaml exec coordinator cat /secrets/admin.key
```

The first `up` makes the keys of the administrator and of the runner, and seeds the library with
this page's pipeline. The runner can only reach Docker's local networks:
[The runner's network](../../deploy/README.md#the-runners-network).

### Sign in

Open `COORDINATOR_URL` and paste the administrator's key. **Access** lists `admin` and `runner-1`,
and the flow matrix is empty: nobody may send rows to anybody yet. In every other terminal of this
page, export `COORDINATOR_URL` again.

## 2. Seed a database

A thousand orders in SQLite, the same on every run thanks to the seed:

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

## 3. Allow the node and the flows

In **Access**, press **Add an identity**: client id `shop`, kind `node`, group `data`, then
**Create**. A window shows the node's key once. Read it into a variable in the terminal where the
node will run (paste, then Enter):

```bash
read -rs DTPIPE_NODE_KEY && export DTPIPE_NODE_KEY
```

In the **flow matrix**, tick `*` → `runner`, `runner` → `*` and `data` → `data`, then **Apply**.
Each group may then use the runner, and rows from `data` may return to `data`. A change applies to
the next run.

## 4. Declare the node

A node offers **bricks**: a branch that only reads is a source, one that only writes is a sink.
Use absolute paths, since a brick runs in a folder of the node's own:

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

dtpipe node --name shop --coordinator "$COORDINATOR_URL" \
  --client-secret '${{DTPIPE_NODE_KEY}}' --job bricks.yaml
```

The node stays in the foreground. **Nodes** lists `orders` and `report` under `shop`, and
**preview** on a brick reads a few rows through it.

## 5. Open the studio (optional)

In another terminal, in the same folder:

```bash
read -rs DTPIPE_KEY && export DTPIPE_KEY
mkdir jobs
dtpipe studio ./jobs --coordinator "$COORDINATOR_URL" --key '${{DTPIPE_KEY}}'
```

The studio offers the nodes' bricks. **Open from library** writes `shop-orders-by-country` into
`./jobs` to edit; **Publish** saves it back.

## 6. Arm it, run it

In the library, open `shop-orders-by-country` and press **Deploy**: the coordinator checks the
bricks and the flows, and arms that version. Press **Run**. **Runs** shows each part, the host it
ran on and the rows that crossed. Then:

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

The same query on one machine gives the same file:

```bash
dtpipe -i sqlite:shop.db \
  --query "SELECT Country, COUNT(*) AS Orders FROM orders GROUP BY Country ORDER BY Orders DESC, Country LIMIT 5" \
  -o csv:witness.csv
diff witness.csv report.csv && echo identical
```

## 7. Start the run from a script

A **launcher** may start the pipelines it lists, and nothing else. In **Access**, add an identity:
client id `launcher-1`, kind `launcher`, pipeline `shop-orders-by-country`. Read its key:

```bash
read -rs LAUNCHER_KEY
```

Exchange the key for a token, then post the run (needs `curl` and `jq`; the pipeline must be
armed):

```bash
TOKEN=$(curl -s "$COORDINATOR_URL/connect/token" \
  -d grant_type=client_credentials -d client_id=launcher-1 \
  -d "client_secret=$LAUNCHER_KEY" | jq -r .access_token)
curl -s -X POST "$COORDINATOR_URL/service/runs" \
  -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d '{"pipelineId": "shop-orders-by-country"}'
```

The answer holds a `runId`. `GET /service/runs/<runId>`, with the same header, returns its `state`
until it is `succeeded` or `failed`.

## Good to know

- **The step ran on the runner**, not on the node: a node only reads and writes its own data.
- **A brick's version is the hash of its text.** Change `bricks.yaml` and restart the node, and the
  armed pipeline is refused with `brick-changed` until you deploy it again.
- **A key is shown once.** If you lose it, **Access** → **Keys** → **Rotate** makes another.
- **The rights survive a restart.** Delete `coordinator-state` (or `docker compose … down -v`) to
  start again with new keys.
- **Keep passwords out of `bricks.yaml`** with `${{NAME}}` or `keyring://`.
- **An incremental load keeps its cursor on the coordinator:** see
  [REFERENCE.md](../../REFERENCE.md#incremental-loading-in-a-distributed-pipeline).

## If something sticks

| Symptom | What to do |
|:---|:---|
| A command has no address, or the page does not open | Export `COORDINATOR_URL` again in this terminal |
| The coordinator stops with "is not a key of this coordinator" | Make the key file again with `dtpipe keygen <id> --out <file>` |
| `docker compose` starts the wrong images in a new terminal | Export the two image variables again |
| The node says its client secret names a variable that is not set | Run `read -rs DTPIPE_NODE_KEY && export DTPIPE_NODE_KEY` in the node's terminal |
| The node does not connect | Its identity must exist in **Access**, with the `--name` as client id, and the key must be its own |
| `--client-secret` is refused | Give a reference, never the value: `'${{NAME}}'`, `keyring://alias` or `@file` |
| **Deploy** or **Run** is refused | Read the message: node offline, flow missing from the matrix, brick changed. The coordinator's log says more (`docker compose -f deploy/compose.yaml logs -f coordinator`, or `coordinator.log`) |

## Stop it

Ctrl+C in the node's terminal, then:

```bash
kill $(cat coordinator.pid runner.pid)              # with the programs
docker compose -f deploy/compose.yaml down -v       # with Docker
```

## Next

| | |
|:---|:---|
| Every setting of the coordinator and the nodes | [REFERENCE.md](../../REFERENCE.md#distributed-pipeline) |
| Declare bricks on a real database | [`dtpipe node`](../../REFERENCE.md#a-node-from-the-cli-dtpipe-node) |
| Make your own .NET service a node | [`AddDtPipeNode`](../../src/DtPipe.PipelineNode.Hosting/CLAUDE.md) |
| Four data nodes, joins across them, rights and faults | [The distributed lab](../../samples/distributed-lab/README.md) |
| Run it for real: volumes, databases, TLS, backup | [deploy/README.md](../../deploy/README.md) |
