# Experimentation

[← Documentation](README.md)

> **Experimental.** Everything on this page may change or disappear without notice. It is not
> covered by versioning and is not meant for production. It has not been run on Windows.

## Distributed pipelines

A distributed pipeline runs one pipeline across several hosts, with the rows streaming between
them.

| Piece | What it does |
|:---|:---|
| **Coordinator** | Holds a library of pipelines, decides which part runs where, starts each run and gives it one verdict. It says which host may send rows to which |
| **Node** | A host next to its own data. It offers **bricks**: reads and writes its owner declared on that data, so the coordinator never holds a connection string |
| **Runner** | A node with no data. It runs the steps between the bricks: a join, a SQL query, an anonymization |

A node is `dtpipe node`, or any .NET host that calls `AddDtPipeNode`. The coordinator and the runner
are published as container images.

To try it: [Your first distributed pipeline](guides/distributed-first-pipeline.md). To run it for
real: [deploy/README.md](../deploy/README.md). Every setting:
[REFERENCE.md](../REFERENCE.md#distributed-pipeline-experimental). A demonstrator with four data
nodes, joins across them and injected faults: [the lab](../samples/distributed-lab/README.md).

## The studio

`dtpipe studio ./jobs` opens a local page over the YAML jobs of a folder. Each job shows as cards
that you edit and preview on the real engine, without writing to the target. It needs no
coordinator; with `--coordinator`, it also offers the nodes' bricks and publishes jobs to the
coordinator's library. Details: [REFERENCE.md](../REFERENCE.md#studio-dtpipe-studio).

## Limits

- **The coordinator is one process.** Its state survives a restart; a run in flight does not, and
  ends as an error. On PostgreSQL or SQL Server a second instance can wait as a standby and take
  over. Storage and backup: [deploy/README.md](../deploy/README.md).
- **Tried on one machine.** As local processes and as Linux containers, on macOS.
- **No contract.** The libraries, the messages between coordinator and nodes, and the lab may all
  change.

---

`dtpipe split` offers the points where a job can be cut in two:
[REFERENCE.md](../REFERENCE.md#offering-the-points-a-job-could-be-cut-at).
