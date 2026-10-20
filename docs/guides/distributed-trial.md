# Try a distributed pipeline

> **Experimental.** The coordinator, the pipeline nodes, the runner and `dtpipe studio` may change
> or disappear without notice, and nothing here is meant for production. Read
> [Experimentation](../experimentation.md) for the limits first.

A pipeline normally runs in one process. A **distributed pipeline** is cut into fragments that run
on different hosts, each next to the data it reads or writes, with the rows streaming between them
as Arrow. This page starts a small one on your machine, from one command, and runs it.

## What the pieces are for

| Piece | What it does |
|:---|:---|
| **Coordinator** | The control plane. It knows which nodes are connected, keeps the pipelines in a git-backed **library**, decides which fragment runs where, starts the run and gives it one verdict. Its pages show the library, the runs and the rights. |
| **Node** | A host that carries data. It offers **bricks**: reads and writes its owner declared on its own database, so the coordinator never holds a connection string. |
| **Runner** | A node with no data. It runs every step between the bricks: a join, a SQL query, an anonymization. |
| **Studio** | `dtpipe studio`, a local page where you design a job as cards and sample it on the real engine. Given a coordinator, it also offers the bricks of the nodes and publishes the job to the library. |

Who may do what is the coordinator's own configuration: **rights** list the nodes, the groups
they belong to and which group may send rows to which. The identity provider only says who a
caller is.

## Start the recipe

You need Docker with Compose and a checkout of this repository. The recipe builds a trial identity
provider and a trial data node from the checkout, so the first start takes a few minutes. The
coordinator and the runner are the images each release publishes on GHCR.

```bash
docker compose -f deploy/compose.yaml up -d
docker compose -f deploy/compose.yaml ps
```

Wait until every service is `healthy`. Everything listens on the loopback of your machine, in
plain HTTP. The addresses use `localtest.me`, a public name that resolves to `127.0.0.1`; without
a network that resolves it, add `idp.localtest.me` and `coordinator.localtest.me` to `/etc/hosts`.

Open <http://coordinator.localtest.me:8080> and sign in as `admin` with the password `adminpwd`:
the trial identity provider accepts any name made of letters, digits, `.`, `_` or `-`, and the
password is the name followed by `pwd`. Nobody but `admin` may do anything on this coordinator.

- **Library** holds one pipeline, `trial-totals`: a generated source on the trial node, summed by
  the runner, written to a file by a sink brick on the trial node.
- **Runs** shows the queue and the journal of every run.
- **Access** shows the rights: the two nodes, `trial-node` and `runner-1`, and the groups that may
  send rows to each other.

## Run the pipeline

Open `trial-totals` in the library and press **Deploy**, which arms it: the coordinator checks
that the bricks it names are offered and that the rights allow every edge, then keeps that version
ready to launch. Press **Run**, then read what the sink wrote:

```bash
docker compose -f deploy/compose.yaml exec node cat /state/totals.csv
```

```text
n,total
1000,499500
```

The run's report in **Runs** names the fragments, the node each ran on and the rows that crossed
each edge.

### From a scheduler

A **launcher** is a service identity that may start the runs of the pipelines it lists, and
nothing else. The recipe declares one, `launcher-1`. It asks the identity provider for a token and
posts the run to `/service/runs`; the pipeline must be armed first. This needs `curl` and `jq`:

```bash
TOKEN=$(curl -s http://idp.localtest.me:5190/connect/token \
  -d grant_type=client_credentials -d client_id=launcher-1 \
  -d client_secret=launcher-1-trial-secret | jq -r .access_token)
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

Pointed at the recipe's coordinator, it also offers the bricks of the nodes:

```bash
dtpipe studio ./jobs --coordinator http://coordinator.localtest.me:8080
```

**Sign in** opens the identity provider in a new tab. The recipe registers the studio's client and
lists `admin` as the coordinator's administrator, so sign in as `admin`. **Open from library**
writes `trial-totals` into the folder as a job file you can edit; **Publish** sends the saved job
back, and the pipeline keeps running the version it was armed with until it is armed again. The
coordinator's own pages design nothing: what a person writes reaches the library from the studio.
The full behaviour is in [REFERENCE.md](../../REFERENCE.md#studio-dtpipe-studio).

## Stop it

```bash
docker compose -f deploy/compose.yaml down -v
```

`-v` removes the volumes: the next start begins from the recipe's rights and library again. After
you pull a newer checkout, run `docker compose -f deploy/compose.yaml build` before starting, or
the trial hosts keep the old build.

## Where to go next

| I want to… | Read |
|:---|:---|
| Run the coordinator and the runner for real: volumes, Redis, TLS, secrets, backup | [deploy/README.md](../../deploy/README.md) |
| See every setting of the coordinator and of a node host | [REFERENCE.md](../../REFERENCE.md#distributed-pipeline-experimental) |
| Make my own service a node that offers bricks | [`AddDtPipeNode`](../../src/DtPipe.PipelineNode.Hosting/CLAUDE.md), and the trial node in [src/DtPipe.PipelineNode.TrialNode/](../../src/DtPipe.PipelineNode.TrialNode/Program.cs) |
| Watch four data nodes, a join across them, rights and faults | [The distributed lab](../../samples/distributed-lab/README.md) |
| Cut a job in two at the point a pipeline could be split | [`dtpipe split`](../../REFERENCE.md#offering-the-points-a-job-could-be-cut-at) |

---
See also: [Experimentation](../experimentation.md) · [DAG pipelines](dag.md) ·
[REFERENCE.md](../../REFERENCE.md#distributed-pipeline-experimental)
