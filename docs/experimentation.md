# Experimentation

> **Experimental.** Everything on this page may change or disappear without notice. It carries no
> stability promise, ships in no package, and is not meant for production.

DtPipe runs a pipeline in one process. The experimental part cuts one pipeline into **fragments**
and runs each on a different host, with the Arrow data streaming between them.

## What is there

| | What it is | Where |
|:---|:---|:---|
| **`DtPipe.Coordinator`** | The control plane of a distributed pipeline: it checks a plan, knows which nodes are present, starts every fragment and every transfer between them, and gives the run one verdict | `src/DtPipe.Coordinator/` |
| **`DtPipe.Coordinator.Server`** | The coordinator as a server: an API and pages, the placement of a pipeline's fragments on nodes, a git-backed library of pipelines, a queue of runs, and the rights that say which nodes exist and which group may send to which | `src/DtPipe.Coordinator.Server/` |
| **`DtPipe.PipelineNode`** | Runs one fragment on its host as a `dtpipe` child process and relays its Arrow streams to the peers | `src/DtPipe.PipelineNode/` |
| **`DtPipe.PipelineNode.Hosting`** | `AddDtPipeNode`: makes any .NET host a node, offering the **bricks** its owner declares, each a read or a write on the host's own data | `src/DtPipe.PipelineNode.Hosting/` |
| **`DtPipe.PipelineNode.Runner`** | The official runner: a node that hosts no data and runs any fragment | `src/DtPipe.PipelineNode.Runner/` |
| **The lab** | A demonstrator that hosts these libraries: a coordinator, four data nodes with their own databases and a runner, as local processes or a compose stack, with a page to assemble, cut, place and run a pipeline | `samples/distributed-lab/` |

The distributed projects move the data over [TransportR](https://github.com/nicopon/TransportR), a
separate transport library; nothing else in dtpipe depends on it, and the CLI hosts neither a node
nor a hub. These projects sit outside `DtPipe.sln`, so `build.yml` does not build them; the lab,
with its test nodes and their data, sits outside the solution and the build.

The lab exists to try the idea and to find where it breaks, not to serve a workload. None of this
has been run on Windows. To see it run, follow its
[README](../samples/distributed-lab/README.md). The rules the server holds to (rights, library,
placement, runs) are in its [CLAUDE.md](../src/DtPipe.Coordinator.Server/CLAUDE.md).

## Nodes and replicas

A node is an identity: a name, the groups it belongs to, and the token it connects with. One or
more hosts, its **replicas**, can carry that identity. A run is placed on one connected replica of
the node whose bricks match the plan, and a replica that is lost ends the runs placed on it. The
replicas of a node are expected to be interchangeable (the same database, the same variables, the
same bricks outside a rolling update); nothing checks it. The lab starts several replicas with
`LAB_REPLICAS`, described in its README.

## The studio

`dtpipe studio` belongs to the CLI, not to the distributed projects. It opens a local page over the
YAML jobs of a folder, to design, debug and run them, with no coordinator at all. Given
`--coordinator`, it also offers the bricks the nodes declare, opens pipelines from the coordinator's
library and publishes a job there. The coordinator's own pages design nothing: what a person writes
reaches its library from a studio. The command is in the
[reference](../REFERENCE.md#studio-dtpipe-studio).

## Limits to know first

- **The coordinator is one process, not a highly available service.** Its rights, its library and
  its armed deployments are files in its state directory, and the history of its runs is in the
  Redis it is configured with (`Coordinator:Redis:ConnectionString`). It does not start without
  one, and a first start on an empty Redis needs `Coordinator:Redis:Initialize` set to true. All
  of it survives a restart. A run in flight does not: it is ended as an error, keeps its id, and
  the nodes that carried it are asked to abandon its fragments.
- **One machine; Windows untried.** The lab has been exercised on a single macOS machine, as local
  processes and as a compose stack of Linux containers. A node is designed to run on Windows, which
  has not been tried.
- **No contract.** The libraries' interfaces, the messages between coordinator and nodes, and the
  lab may all change.

On the command line, `dtpipe split` offers the points where a job can be cut in two; how a
fragment's boundary is bound to a run is in the reference.

---

See also: [DAG pipelines](guides/dag.md) · [Concepts](concepts.md) ·
[REFERENCE.md](../REFERENCE.md#binding-a-fragments-boundary) ·
[Studio](../REFERENCE.md#studio-dtpipe-studio)
