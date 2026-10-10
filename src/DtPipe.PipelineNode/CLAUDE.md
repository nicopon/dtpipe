# DtPipe.PipelineNode

Mechanics for editing the pipeline node. The rules themselves are in the root `CLAUDE.md`.

A pipeline node is a replica's side of the data plane. Under the control of the coordinator
(`src/DtPipe.Coordinator/`, through the host in `src/DtPipe.PipelineNode.Hosting/`) it launches each run of a
fragment as a `dtpipe` child process and relays the Arrow IPC bytes of each edge between the child's
stdin/stdout and TransportR. It speaks to no coordinator itself.

- **A run is a `FragmentRun`; `PipelineNode` holds what surrounds it, once per replica.** `FragmentRun` owns the
  child, the named pipes of the excess edges, the relays, the first fault and the row counts -
  `LaunchChild`, `ConnectPipeAsync`, `Rupture`, `WatchChild`, `Fault`, `RunToCompletionAsync` and
  `WaitForChildExitAsync` below are its - and knows neither the TransportR client nor the coordinator: it
  is given each transfer handle once resolved, and says how it ended (`FragmentOutcome`). `PipelineNode` holds
  the one client of the replica and the book of the handles it is given, launches runs (`Launch`, from a
  `FragmentLaunch`: the job, the edges, how the child starts - nothing of the replica) and hands a run its
  handles (`WireAsync`). The fault is one value, origin and reason together: a reader never sees a reason
  without its origin. `[local: PipelineNode.Tests]`
- **The node is a byte relay.** It references no DtPipe project and never rewrites a batch; the
  child's `arrow:` reader and writer own the format. Put engine logic in dtpipe, never here.
  `ArrowIpcRowCounter` reads the IPC framing only to count rows.
- **A replica has one TransportR client, shared by all its runs, and is the only TransportR client they
  have.** dtpipe has no TransportR adapter and reaches the node only through `arrow:`. The client bounds each
  transfer it receives (`WithReceiveCapacity`), so a replica running several carries one bound apiece.
- **A transfer handle is claimed once, and an unclaimed one is released.** The handshake delivers a handle on
  its own schedule, before or after the run's `Wire` asks for it. One no run wires within
  `NodeClientOptions.UnclaimedTransferTimeout` (its run was torn down first) is disposed, not kept, and a
  `Wire` whose handle never comes faults its run `FaultOrigin.Remote`. `[local: PipelineNode.Tests]`
  `[unchecked: the release of an unclaimed handle]`
- An edge is a branch alias plus a direction (`EdgeBinding`). The first edge of each direction rides
  the child's stdin/stdout; `LaunchChild` gives every further edge its own `NamedPipeServerStream`
  (`System.IO.Pipes`, never a POSIX FIFO — the mechanism has to reach Windows too), created before
  the child starts and wired into it via `--bind-input`/`--bind-output alias=arrow:pipe://<name>`
  (`AliasBindingApplier` does the substitution). `ConnectPipeAsync` races the pipe's own
  `WaitForConnectionAsync()` against the child's exit, so a child that dies before ever dialing in
  cannot block the node forever. `WireAsync` and the pipe dictionary both key on `EdgeBinding.Alias`
  alone, regardless of direction — `ValidateEdges` refuses a duplicate up front.
- **Cancel by stream rupture, never by signal.** The first fault ruptures the child at once
  (`Rupture`: stdin and every pipe closed, a kill after the grace period), never after the relays
  finish - an outbound relay reading a child with nothing to write finishes only when the kill closes
  that child's stdout. The host's `Cancel` (`ReplicaRuns`) reuses exactly this path, not
  a second teardown mechanism: `Fault(Local, ...)` then run to completion for a fragment not yet fully
  wired, `Fault(Remote, ...)` alone for a fully wired one, which is already completing on its own.
  `[local: CoordinatorAbortTests]`
- **The child is watched independently of the relays.** `WatchChild` faults the fragment
  (`FaultOrigin.Local`) the moment the child exits non-zero: a relay only notices a dead child on its
  next read or write, which a fragment waiting on a silent peer never makes. `RunToCompletionAsync`
  then gives the relays `RelayDrainAfterChildFailureMs` to fail on their own and reports without a
  relay still waiting on its peer; the coordinator's teardown ends that transfer.
  `[local: FaultVerdictTests]`
- **The child's exit is awaited by polling `HasExited` beside `WaitForExitAsync`.**
  `RunToCompletionAsync` uses `WaitForChildExitAsync`: `Process.WaitForExitAsync` alone can stay
  pending after the child has exited, which holds the `Exited` report, and so the run's verdict,
  back for good; a cancelled fragment whose child ends on its own is the case that shows it. `[unchecked: the race is not reproducible on demand]`
- **The client's registration is renewed by restarting its connection.** The groups the hub holds for a client
  are those it registered with; TransportR's client re-registers on every restart of its control connection and
  offers no call for it. `RefreshRegistrationAsync` stops the connection and waits for the client's own
  recovery, under the same client id. Call it with no run in flight. For a client the hub holds a
  registration of, also while the host is not announced as able to take a run; a client the hub does not
  know (`IsRegisteredAsync`, which asks the hub's `Heartbeat`, the only public sign of a registration)
  holds no groups to be stale, and is registered again at any time. `[local: GroupRefreshTests,
  RegistrationTests]`
- **The node must run on Windows**: no POSIX-only API (FIFO, signals). `[unchecked: nothing runs
  the node on Windows]`
- Keep `MaxInFlightBatches` non-null: a null bound lets a sender race ahead of a slow receiver.
  `[local: MemoryCeilingTests, macOS only]`
- **A relay task tags its own fault with a `FaultOrigin`**: `Local` when its own child process failed
  (an exit code check or a plain I/O exception), `Remote` when the transfer itself failed
  (`TransferFailedException`, an `OperationCanceledException` - never this code's own doing, since
  no token reaches `SendAsync`/`ReceiveAsync` - or an `ObjectDisposedException` thrown by the send
  handle itself, disposed under the call when its transfer was torn down). The coordinator's
  outcome rule tells a run's cause from its consequences by this, not by which fragment reports
  first. `[local: PipelineNode.Tests, DtPipe.Coordinator.Tests.RunOrchestratorTests]`
- `ConnectAsync` connects the replica's client and launches nothing; `Launch` spawns a run's child and
  `WireAsync(run, alias, transferId)` hands it one edge's transfer, `true` when it was the last edge the run
  declares.
- **A run is its launcher's to dispose.** Disposing the node closes its client and releases the handles no run
  claimed; it ends no run. `[unchecked]`
- **A test whose verdict depends on how soon a child starts or how soon its end is seen joins
  `ChildExitTimingCollection`**: an elapsed-time bound on a child, or a short run timeout. While a child is being started, the process
  observes no other child's end until that start returns, so such a test beside another class's launch can fail on that
  launch's speed. The collection runs its classes one at a time, after the parallel ones: while one runs, no other class of the project starts
  a child. The children of one test are not separated.
  `[unchecked: a class that depends on child timing without joining the collection]`

`tests/DtPipe.PipelineNode.Tests` spawns the real `dist/release/dtpipe` against an in-process
TransportR hub (`NodeTestHost`, where `HandWiredNode` wires a run by hand) or a real `DtPipe.Coordinator` hub (`CoordinatorTestHost`, from
`DtPipe.Coordinator.Tests` - referenced as a project, not duplicated), whose replicas are `TestNode`s that
answer the node channel with the host's own run protocol; build the binary first. Both
projects are outside `DtPipe.sln`, so `build.yml` never runs their tests; `build.sh` discovers
them, so `agentic-ci.yml` runs them on Linux.
