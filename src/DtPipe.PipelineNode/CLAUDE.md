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
- An edge is a branch alias plus a direction (`EdgeBinding`). The first edge of each direction rides
  the child's stdin/stdout; `LaunchChild` gives every further edge its own `NamedPipeServerStream`
  (`System.IO.Pipes`, never a POSIX FIFO — the mechanism has to reach Windows too), created before
  the child starts and wired into it via `--bind-input`/`--bind-output alias=arrow:pipe://<name>`
  (`AliasBindingApplier` does the substitution). `ConnectPipeAsync` races the pipe's own
  `WaitForConnectionAsync()` against the child's exit, so a child that dies before ever dialing in
  cannot block the node forever. `WireAsync` and the pipe dictionary both key on `EdgeBinding.Alias`
  alone, regardless of direction — `ValidateEdges` refuses a duplicate up front.
- **Cancel by stream rupture, never by signal.** A fault closes the child's stdio, which its
  `arrow:` endpoint turns into exit 1; the node kills the child after a grace period if that is not
  enough. `CancelAsync` (the coordinator's `Cancel` push, for a fragment that has not yet opened a
  transfer, or a multi-edge fragment still waiting on one side) reuses exactly this path - it is a
  `Fault(Local, ...)` like any other, not a second teardown mechanism.
- **A reconnect re-issues `Register`, with a bounded retry.** TransportR's own `SignalRDataClient`
  already re-issues `Connect` on `HubConnection.Reconnected`; nothing re-issues the coordinator's own
  `Register` but this node, since TransportR has no reason to know that call exists. The retry exists
  because an immediate `Register` can still find the hub's own bookkeeping for the dropped connection
  not yet superseded and be refused - unlike TransportR's presence heartbeat, nothing else ever
  retries this call. Giving up after every attempt fails is deliberate: the coordinator's own
  disconnect grace period (`DtPipe.Coordinator`) is the backstop.
- **A failed `Exited` report is held, not dropped.** The same race that can refuse a re-`Register`
  can refuse an `Exited` sent on a fresh connection before that connection's own re-`Register` has
  landed (`ResolveFragment` on the hub side has nothing to resolve yet). `ReportExitedWithFallbackAsync`
  keeps the pending `(runId, exitCode)` and `RetryPendingExitedReportAsync` resends it once
  `ReRegisterWithRetryAsync` returns - a single attempt, no further retry: this is the fragment's own
  outcome, and losing it silently would report a run that actually finished as unresolved instead.
  `[unchecked: no test forces this exact ordering]`
- **The node must run on Windows**: no POSIX-only API (FIFO, signals). `[unchecked: nothing runs
  the node on Windows]`
- Keep `MaxInFlightBatches` non-null: a null bound lets a sender race ahead of a slow receiver.
  `[local: MemoryCeilingTests, macOS only]`
- **A relay task tags its own fault with a `FaultOrigin`**: `Local` when its own child process failed
  (an exit code check or a plain I/O exception), `Remote` when the transfer itself failed
  (`TransferFailedException`, or an `OperationCanceledException` - never this code's own doing, since
  no token reaches `SendAsync`/`ReceiveAsync`). The coordinator's outcome rule tells a run's cause
  from its consequences by this, not by which fragment reports first. `[local: PipelineNode.Tests,
  DtPipe.Coordinator.Tests.RunOrchestratorTests]`
- `ConnectAsync` connects and declares the fragment (`Register`) without launching the child; a
  `Launch` push from the coordinator triggers `LaunchAsync`, a `Wire` push triggers `WireAsync`. Once
  every declared edge is wired the node runs itself to completion and reports `Exited` on its own -
  see `Completion`. `StartAsync` still launches immediately, for the hand-wired path with no
  coordinator.

`tests/DtPipe.PipelineNode.Tests` spawns the real `dist/release/dtpipe` against an in-process
TransportR hub (`NodeTestHost`) or a real `DtPipe.Coordinator` hub (`CoordinatorTestHost`, from
`DtPipe.Coordinator.Tests` - referenced as a project, not duplicated); build the binary first. Both
projects are outside `DtPipe.sln`, so CI never runs their tests.
