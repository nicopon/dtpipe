# DtPipe.Coordinator

Mechanics for editing the coordinator. The rules themselves are in the root `CLAUDE.md`.

The coordinator is the central hub of a distributed pipeline. Pipeline nodes
(`src/DtPipe.PipelineNode/`) each run one fragment on a remote host, and each holds a single
connection to the coordinator. The coordinator is the **control plane**: it checks plans, knows who
is present, and opens every transfer. TransportR carries the **data plane**: the Arrow IPC bytes
between nodes.

## TransportR stays a transport

- Keep every dtpipe concept (plan, fragment, edge, run, node) in this project. Change TransportR
  only for a missing transport function. `[unchecked]`
- Put hosting policy in ASP.NET Core extension points on this side, not in TransportR:
  `PeerToPeerHubFilter` is an `IHubFilter`.
- TransportR is referenced as compiled DLLs in `lib/TransportR/`. `MANIFEST.md` records the source
  commit and a hash per file. Refreshing them is a port, not a copy: rebuild, then check each API
  change against this project and `DtPipe.PipelineNode`.
- A private `Reference` does not pull in transitive packages. Declare them in the `.csproj` at the
  version TransportR pins.

## Hosting

`AddCoordinatorHub` registers the flow-control matrix, `IPlanRegistry`, `LoggingHubProgressMonitor`,
the peer-to-peer filter, `INodeRegistry` and `IRunOrchestrator`. It returns TransportR's
`DataHubBuilder` already pointed at `CoordinatorHub`, and the caller finishes it (identity provider,
then `Build()`).

- Register the progress monitor **before** `AddDataHub()`: TransportR adds a no-op default only when
  none is registered.
- Register a hub filter with `AddSignalR(o => o.AddFilter<T>())`. SignalR silently ignores an
  `IHubFilter` registered alone in DI.
- **Peers never address each other.** `PeerToPeerHubFilter` refuses `GetReceivers` and
  `InitTransfer`. Only the coordinator opens a transfer, through `ITransferInitiator`, which calls
  the client directly and so is never seen by the filter.
  `[local: PeerToPeerHubFilterTests]`
- **Check a plan before anything opens.** `PlanRegistry.Register` refuses a plan with any edge the
  flow-control matrix forbids, and names every forbidden edge. The same matrix governs runtime
  transfers. `[local: PlanRegistryTests]`

## The barrier

`RunOrchestrator.RunAsync` is the barrier: it admits a run only once `AdmissionGate` sees every
fragment `RunSpec.Fragments` names registered in `INodeRegistry`, pushes `Launch` to each, waits for
every `Ready`, opens each `RunSpec.Edges` entry through `ITransferInitiator` and pushes `Wire`, then
waits for every `Exited` and applies `DetermineOutcome`. `RunAsync` is an in-process API - no
`RegisterPlan`/`Run`/`Status` network surface yet - and one run at a time; a second call while one is
in flight throws instead of queuing.

- **A fragment's identity comes from its connection, never from a method argument.** `Register`,
  `Ready` and `Exited` all resolve the caller through `INodeRegistry.TryGetByConnection(Context.ConnectionId)`
  (itself seeded from `IStateStore` at `Register` time) - the same rule `ComponentSelector` and
  TransportR's own `Connect` follow, so a fragment can never claim another's report. `[local:
  AdmissionGateTests, RunOrchestratorTests, CoordinatorDrivenTests in DtPipe.PipelineNode.Tests]`
- **A 0 exit code is not proof of delivery.** `DetermineOutcome` also compares each edge's two row
  counts and fails the run, naming the edge, on a mismatch even when every fragment reported 0. A
  fragment attests its own process, not what its peer received. `[local: RunOrchestratorTests]`
- **The cause is the first *locally* faulted fragment, never arrival order.** A peer aborting its own
  transfer can fail a healthy fragment before the fragment that actually died reports `Exited`, so
  `DetermineOutcome` picks the cause by `FaultOrigin.Local`, falling back to first-reported only when
  no report carries it. `[local: RunOrchestratorTests, CoordinatorDrivenTests]`
- **A dropped connection is not yet a lost node.** `CoordinatorHub.OnDisconnectedAsync` only drops the
  `INodeRegistry` entry; a run still waiting on that fragment is not failed, and a reconnect is not
  re-routed to the new connection id yet. `[unchecked]`

`tests/DtPipe.Coordinator.Tests` is outside `DtPipe.sln`, so CI never runs it.
