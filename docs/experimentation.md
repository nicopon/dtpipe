# Experimentation

> **Experimental.** Everything on this page may change or disappear without notice. It carries no
> stability promise, ships in no package, and is not meant for production.

DtPipe runs a pipeline in one process. The experimental part cuts one pipeline into **fragments**
and runs each on a different host, with the Arrow data streaming between them.

## What is there

| | What it is | Where |
|:---|:---|:---|
| **`DtPipe.Coordinator`** | The control plane of a distributed pipeline: it checks a plan, knows which nodes are present, starts every fragment and every transfer between them, and gives the run one verdict | `src/DtPipe.Coordinator/` |
| **`DtPipe.PipelineNode`** | Runs one fragment on its host as a `dtpipe` child process and relays its Arrow streams to the peers | `src/DtPipe.PipelineNode/` |
| **The lab** | A demonstrator that hosts both libraries: a coordinator, four data nodes with their own databases and a runner, as local processes, with a page to assemble, cut, place and run a pipeline | `samples/distributed-lab/` |

The two libraries move the data over [TransportR](https://github.com/nicopon/TransportR), a
separate transport library; nothing else in dtpipe depends on it. The lab, with its test nodes
and their data, is a sample: it sits outside the solution and the build, and no dtpipe project
refers to it.

The lab exists to try the idea and to find where it breaks, not to serve a workload. To see it run,
follow its [README](../samples/distributed-lab/README.md).

## Limits to know first

- **The coordinator is not highly available.** Its state lives in memory: restarting it loses the
  run in flight.
- **One machine, one platform.** It has been exercised on a single macOS machine. Windows has not
  been tried.
- **No contract.** The libraries' interfaces, the messages between coordinator and nodes, and the
  lab may all change.

On the command line, `dtpipe split` offers the points where a job can be cut in two; how a
fragment's boundary is bound to a run is in the reference.

---

See also: [DAG pipelines](guides/dag.md) · [Concepts](concepts.md) ·
[REFERENCE.md](../REFERENCE.md#binding-a-fragments-boundary)
