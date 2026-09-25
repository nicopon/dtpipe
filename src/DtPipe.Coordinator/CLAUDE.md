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

`AddCoordinatorHub` registers the flow-control matrix, `IPlanRegistry`, `LoggingHubProgressMonitor`
and the peer-to-peer filter. It returns TransportR's `DataHubBuilder` already pointed at
`CoordinatorHub`, and the caller finishes it (identity provider, then `Build()`).

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

`tests/DtPipe.Coordinator.Tests` is outside `DtPipe.sln`, so CI never runs it.
