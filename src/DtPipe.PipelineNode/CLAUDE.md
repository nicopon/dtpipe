# DtPipe.PipelineNode

Mechanics for editing the pipeline node. The rules themselves are in the root `CLAUDE.md`.

A pipeline node runs one fragment of a distributed pipeline under the control of the coordinator
(`src/DtPipe.Coordinator/`). It launches the fragment as a `dtpipe` child process and relays the
Arrow IPC bytes of each edge between the child's stdin/stdout and TransportR.

- **The node is a byte relay.** It references no DtPipe project and never rewrites a batch; the
  child's `arrow:` reader and writer own the format. Put engine logic in dtpipe, never here.
  `ArrowIpcRowCounter` reads the IPC framing only to count rows.
- **The node is the only TransportR client of its fragment.** dtpipe has no TransportR adapter and
  reaches the node only through `arrow:`.
- An edge is a branch alias plus a direction (`EdgeBinding`). stdin and stdout carry one edge each,
  so a fragment takes at most one inbound and one outbound edge. An extra edge needs a named pipe
  (`System.IO.Pipes`), never a POSIX FIFO.
- **Cancel by stream rupture, never by signal.** A fault closes the child's stdio, which its
  `arrow:` endpoint turns into exit 1; the node kills the child after a grace period if that is not
  enough.
- **The node must run on Windows**: no POSIX-only API (FIFO, signals). `[unchecked: nothing runs
  the node on Windows]`
- Keep `MaxInFlightBatches` non-null: a null bound lets a sender race ahead of a slow receiver.
  `[local: MemoryCeilingTests, macOS only]`

`tests/DtPipe.PipelineNode.Tests` spawns the real `dist/release/dtpipe` against an in-process
TransportR hub (`NodeTestHost`); build the binary first. The project is outside `DtPipe.sln`, so CI
never runs it.
