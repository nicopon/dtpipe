# Distributed pipelines

[← Documentation](README.md)

A distributed pipeline runs one pipeline across several hosts, with the rows streaming between
them.

| Piece | What it does |
|:---|:---|
| **Coordinator** | Holds a library of pipelines, decides which part runs where, starts each run and gives it one verdict. It says which host may send rows to which |
| **Node** | A host next to its own data. It offers **bricks**: reads and writes its owner declared on that data, so the coordinator never holds a connection string |
| **Runner** | A node with no data. It runs the steps between the bricks: a join, a SQL query, an anonymization |

A node is `dtpipe node`, or any .NET host that calls `AddDtPipeNode`
([a service as a node](dotnet.md#a-service-as-a-node)). The coordinator and the runner are
published as container images.

To try it: [Your first distributed pipeline](guides/distributed-first-pipeline.md). To run it for
real: [deploy/README.md](../deploy/README.md). Every setting:
[REFERENCE.md](../REFERENCE.md#distributed-pipeline). A demonstrator with four data nodes, joins
across them and injected faults: [the lab](../distributed-lab/README.md).

## The studio

`dtpipe studio ./jobs` opens a local page over the YAML jobs of a folder; the folder's name in the
header (its full path shows on hover) opens another one. Each job shows as steps that you edit and
preview on the real engine, without writing to the target. It needs no coordinator; once one is
connected (`--coordinator`, or the coordinator zone of the page), it also offers the nodes' bricks
and publishes jobs to the coordinator's library. With `--llm-provider`, a panel lets a model design
the job with you: see [The agent](guides/agent.md). Details:
[REFERENCE.md](../REFERENCE.md#studio-dtpipe-studio).

## Sealing the data plane

Rows go from node to node through the coordinator's hub, in clear unless the nodes seal them. Make
one key and start every node with the same two flags:

```bash
(umask 077; openssl rand -base64 32 > e2e.key)
dtpipe node --name shop --coordinator "$COORDINATOR_URL" --client-secret '${{DTPIPE_NODE_KEY}}' \
  --e2e --e2e-key "@$PWD/e2e.key" --job bricks.yaml
```

The key is a file of each node and never the coordinator's. A node's level is one of three words,
which it prints at start; the **Nodes** page says it in the words of the third column:

| Level | Set with | Nodes page | What it protects against |
|:---|:---|:---|:---|
| `none` | the default | `in clear` | Nothing of its own: the hub and a proxy that ends TLS read the rows, and so does whoever reads a hop without TLS |
| `encrypted` | `--e2e` | `sealed` | The hub, a proxy that ends TLS and whoever reads the path see bytes they cannot read. A hub that rewrites a batch is not caught |
| `encrypted-psk` | `--e2e` and `--e2e-key` | `sealed, key` | As `encrypted`, and a hub that alters a transfer makes it fail |

Sealing hides the rows, not the plan: the coordinator opens every transfer, so it still knows which
nodes exchange rows, and its run page lists the rows of each edge.

### What the coordinator checks

A transfer whose two ends are set differently fails before its first row, so the coordinator checks
and shows it:

- **Each node.** A node announces its level and its key id, a fingerprint that is not the key. The
  Nodes page shows both, and warns of replicas of one node set differently: none is refused, since
  a rolling update passes through that.
- **Each edge.** Deploy, and a run when it is placed, refuse an edge whose ends are not at the same
  level and key, before anything launches, and name it. A pipeline's diagram draws a closed lock on
  a sealed edge, and a broken red one, with the reason on hover, on an edge that cannot join:

  ```text
  edge orders@crm -> orders@runner-1: orders@crm seals with key 3f9a0c11d2e4b7a8, orders@runner-1 sends in clear.
  ```

- **The floor.** With `Coordinator__DataPlane__RequiredProtection`
  ([deploy/README.md](../deploy/README.md#what-a-deployment-provides)), the Nodes page marks a
  replica under it `below the minimum`, and it runs no fragment.
- **Each run.** The run page gives, for each edge, what each end applied: `in clear`, `sealed`,
  `sealed, key`, or `unknown` when an end reported nothing. A run that fails on an edge names it as
  its cause:

  ```text
  The ends of orders@crm -> orders@runner-1 are set differently for end-to-end encryption, or its stream was altered on its way.
  ```

- **The studio.** With a coordinator connected, its Problems tab lists the edges the coordinator
  would refuse, and says so before **Publish**, which goes ahead.

What it cannot check:

- **The key.** It never holds one: it compares key ids.
- **A node that is offline.** It has no level to compare: its edges carry no lock, and are judged
  when a run is placed.
- **What changes after Deploy.** A run judges the edges again when it is placed, on the nodes
  connected then. A transfer that still opens between ends set differently fails before its first
  row, and the run names the edge as its cause.

The settings: [REFERENCE.md](../REFERENCE.md#node-host-settings-dtpipenode__).

## Limits

- **The coordinator is one process.** Its state survives a restart, the journal of runs included; a
  run in flight does not, and ends as an error. The events and log lines its pages show are kept in
  memory only, and a restart empties them. On PostgreSQL or SQL Server a second instance can wait as
  a standby and take over. Storage and backup: [deploy/README.md](../deploy/README.md).
- **Tried on one machine.** As local processes and as Linux containers, on macOS. It has not been
  run on Windows.

---

How a fragment's boundary is bound to a run is in the
[reference](../REFERENCE.md#binding-a-fragments-boundary).
