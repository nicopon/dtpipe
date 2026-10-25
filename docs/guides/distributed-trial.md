# Try a distributed pipeline

> **Experimental.** The coordinator, the pipeline nodes, the runner and `dtpipe studio` may change
> or disappear without notice, and nothing here is meant for production. Read
> [Experimentation](../experimentation.md) for the limits first.

A pipeline normally runs in one process. A **distributed pipeline** is cut into fragments that run
on different hosts, each next to the data it reads or writes, with the rows streaming between them
as Arrow. This page starts a small one on your machine, from one command, and runs it, with
everything in Docker. To run one over a database of your own, with a node you declare yourself, read
[Your first distributed pipeline](distributed-first-pipeline.md).

## What the pieces are for

| Piece | What it does |
|:---|:---|
| **Coordinator** | The control plane. It knows which nodes are connected, keeps the pipelines in a git-backed **library**, decides which fragment runs where, starts the run and gives it one verdict. Its pages show the library, the runs and the rights. |
| **Node** | A host that carries data. It offers **bricks**: reads and writes its owner declared on its own database, so the coordinator never holds a connection string. |
| **Runner** | A node with no data. It runs every step between the bricks: a join, a SQL query, an anonymization. |
| **Studio** | `dtpipe studio`, a local page where you design a job as cards and sample it on the real engine. Given a coordinator, it also offers the bricks of the nodes and publishes the job to the library. |

Who may do what is the coordinator's own configuration: **rights** list the nodes, the groups
they belong to and which group may send rows to which. The identity provider only says who a
caller is. In this recipe there is no identity provider: every identity is verified by a key the
coordinator issues tokens for.

## Start the recipe

You need Docker with Compose and a checkout of this repository. A second Compose file adds a trial
data node, built from the checkout, so the first start takes a few minutes, and the rights and the
library that name it. The coordinator and the runner are the images each release publishes on GHCR.

```bash
docker compose -f deploy/compose.yaml -f deploy/compose.trial.yaml up -d --wait
```

The command returns once the coordinator is healthy and the one-off `keys` service, below, has
exited with its work done. Everything listens on the loopback of your machine, in
plain HTTP. The addresses use `localtest.me`, a public name that resolves to `127.0.0.1`; without
a network that resolves it, add `coordinator.localtest.me` to `/etc/hosts`.

On the first start, a one-off service wrote a random key for each identity of the recipe into the
coordinator's volume, and gave each node its own; the coordinator added the identities from them.
Print the first administrator's:

```bash
docker compose -f deploy/compose.yaml -f deploy/compose.trial.yaml exec coordinator \
  cat /secrets/admin.key
```

Open <http://coordinator.localtest.me:8080> and paste it into the sign-in form. A red banner on
every page says that keys cross the network in clear: the recipe has no TLS, so it sets
`Coordinator__Auth__AllowHttpKeys`, without which the coordinator takes no key over HTTP. Nobody
but `admin` may change anything on this coordinator.

- **Library** holds one pipeline, `trial-totals`: a source brick on the trial node
  (`brick:trial-node/numbers`), summed by the runner, written to a file by a sink brick on the trial
  node (`brick:trial-node/totals`). A job names a brick by reference; the node holds what it reads
  and writes.
- **Runs** shows the queue and the journal of every run.
- **Access** shows the identities the keys opened: `admin`, the two nodes `trial-node` and
  `runner-1`, the scheduler `launcher-1`, and the groups that may send rows to each other, which
  `deploy/compose.trial.yaml` seeds. Without that file the coordinator starts with `admin` and
  `runner-1` alone, as in [Your first distributed pipeline](distributed-first-pipeline.md).

## Run the pipeline

Open `trial-totals` in the library and press **Deploy**, which arms it: the coordinator checks that
the bricks it names are offered, samples each one and keeps the version it saw, checks that the
rights allow every edge, then keeps that version ready to launch. Press **Run**, then read what the
sink wrote:

```bash
docker compose -f deploy/compose.yaml -f deploy/compose.trial.yaml exec node cat /state/totals.csv
```

```text
n,total
1000,499500
```

The run's report in **Runs** names the fragments, the node each ran on and the rows that crossed
each edge.

### From a scheduler

A **launcher** is a service identity that may start the runs of the pipelines it lists, and
nothing else. The recipe declares one, `launcher-1`. It exchanges its key for a token at the
coordinator and posts the run to `/service/runs`; the pipeline must be armed first. This needs
`curl` and `jq`:

```bash
KEY=$(docker compose -f deploy/compose.yaml -f deploy/compose.trial.yaml exec coordinator \
  cat /secrets/launcher-1.key)
TOKEN=$(curl -s http://coordinator.localtest.me:8080/connect/token \
  -d grant_type=client_credentials -d client_id=launcher-1 \
  -d "client_secret=$KEY" | jq -r .access_token)
curl -s -X POST http://coordinator.localtest.me:8080/service/runs \
  -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d '{"pipelineId": "trial-totals"}'
```

The answer carries a `runId`. Read the run's `state` with
`GET /service/runs/<runId>` and the same header until it is `succeeded` or `failed`.

## Design a job in the studio

The studio is part of the CLI. Over a folder of YAML jobs it needs no coordinator:

```bash
dtpipe studio ./jobs
```

Pointed at the recipe's coordinator, with a reference to the administrator's key, it also offers
the bricks of the nodes. Read the key into a variable (paste, then Enter), never onto the command
line:

```bash
read -rs DTPIPE_KEY && export DTPIPE_KEY
dtpipe studio ./jobs --coordinator http://coordinator.localtest.me:8080 --key '${{DTPIPE_KEY}}'
```

The studio signs in as `admin` with the key as it starts. **Open from library**
writes `trial-totals` into the folder as a job file you can edit; **Publish** sends the saved job
back, and the pipeline keeps running the version it was armed with until it is armed again. The
coordinator's own pages design nothing: what a person writes reaches the library from the studio.
The full behaviour is in [REFERENCE.md](../../REFERENCE.md#studio-dtpipe-studio).

## Stop it

```bash
docker compose -f deploy/compose.yaml -f deploy/compose.trial.yaml down -v
```

`-v` removes the volumes: the next start begins from the recipe's rights and library again, with
new keys. After
you pull a newer checkout, run `docker compose -f deploy/compose.yaml -f deploy/compose.trial.yaml
build` before starting, or the trial node keeps the old build.

## Where to go next

| I want to… | Read |
|:---|:---|
| Run the coordinator and the runner for real: volumes, databases, TLS, secrets, backup | [deploy/README.md](../../deploy/README.md) |
| See every setting of the coordinator and of a node host | [REFERENCE.md](../../REFERENCE.md#distributed-pipeline-experimental) |
| Declare bricks on my own data and run a pipeline over them | [Your first distributed pipeline](distributed-first-pipeline.md) |
| See every option of `dtpipe node` | [`dtpipe node`](../../REFERENCE.md#a-node-from-the-cli-dtpipe-node) |
| Make my own service a node that offers bricks | [`AddDtPipeNode`](../../src/DtPipe.PipelineNode.Hosting/CLAUDE.md), and the trial node in [src/DtPipe.PipelineNode.TrialNode/](../../src/DtPipe.PipelineNode.TrialNode/Program.cs) |
| Watch four data nodes, a join across them, rights and faults | [The distributed lab](../../samples/distributed-lab/README.md) |
| Cut a job in two at the point a pipeline could be split | [`dtpipe split`](../../REFERENCE.md#offering-the-points-a-job-could-be-cut-at) |

---
See also: [Experimentation](../experimentation.md) · [DAG pipelines](dag.md) ·
[REFERENCE.md](../../REFERENCE.md#distributed-pipeline-experimental)
