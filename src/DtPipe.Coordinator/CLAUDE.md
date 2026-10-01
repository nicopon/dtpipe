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
- TransportR is referenced as NuGet packages (`TransportR`, `TransportR.Hub.SignalR`). Bumping the
  pinned version is a port, not a copy: check each API change against this project and
  `DtPipe.PipelineNode` before moving it.
- `DefaultFlowControlService`/`FlowControlOptions` (namespace `TransportR.FlowControl`) ship inside
  the `TransportR.Hub.SignalR` package; there is no separate `TransportR.FlowControl` package.
- **The coordinator and every `DtPipe.PipelineNode` it drives run the same TransportR version.** A
  hub and clients of different versions interoperate, but a transfer then fails to resume after a
  transient cut of a node's link, and nothing reports it: deploy the coordinator and the nodes
  together. `[unchecked]`
- **A reverse proxy in front of the hub must accept `Expect: 100-continue`.** The client sends it on
  every write stream, and a proxy that answers `417` fails all of them. `[unchecked]`

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

- **A fragment's identity is never a method argument.** `Register` resolves the caller's `ClientId`
  through `IStateStore.GetClientByConnectionIdAsync(Context.ConnectionId)` - TransportR's own
  `Connect` always precedes it on the one connection a node holds. `Ready` and `Exited` then resolve
  the caller's fragment through `INodeRegistry.TryGetByConnection(Context.ConnectionId)`, seeded by
  that `Register` call, never trusting a fragment name passed as an argument at that point.
  `[local: NodeRegistryTests]`
- **`NodeRegistry` never refuses a fragment name - it tracks one entry per (fragment name, `ClientId`)
  pair, not one per name.** A second, previously-unseen `ClientId` registering an already-live name is
  a new, additional instance (redundancy, horizontal scaling), never a collision; whether every live
  instance of a name agrees on version is instance alignment, checked at admission
  (`AdmissionGate.Resolve`, see "Versions" below), not here. A disconnected instance stays reclaimable
  by its own `ClientId` for its disconnect grace period (see below), and a *live* instance's own
  `ClientId` reclaiming under a new `ConnectionId` is always allowed too - the reconnect's `Register`
  can land before that same connection's own `OnDisconnectedAsync` has run. `Unregister` only ever
  drops the entry it still owns - a disconnect and the re-registration that replaces it race by
  nature, and a stale `Unregister` that removed by name alone would orphan the connection that had
  already reclaimed it. `[local: NodeRegistryTests]`
- **A 0 exit code is not proof of delivery.** `DetermineOutcome` also compares each edge's two row
  counts (`RunResult.EdgeCounts`) and fails the run, naming the edge, on a mismatch even when every
  fragment reported 0 - a fragment attests its own process, not what its peer received. A count
  missing on either side fails closed (never reads as agreement); `RunResult.Describe()` renders
  every edge's two counts regardless of which branch produced the outcome. `[local: RunOrchestratorTests]`
- **The cause is the first *locally* faulted fragment, never arrival order.** A peer aborting its own
  transfer can fail a healthy fragment before the fragment that actually died reports `Exited`, so
  `DetermineOutcome` picks the cause by `FaultOrigin.Local`, falling back to `RunSpec.Fragments`'
  own order - never arrival order - only when no report carries that origin. A fragment the
  coordinator itself told to `Cancel` is excluded from both cause and consequence: its own child was
  killed on command, and reports a plain non-zero `Local` exit indistinguishable on the wire from an
  organic one. `[local: RunOrchestratorTests, CoordinatorDrivenTests]`
- **A fragment's own failure ends the run at once.** `OnExitedAsync` raises the run's abort on the
  first `Exited` that is non-zero with a `FaultOrigin.Local` origin, so the teardown tells every
  fragment still running to `Cancel` without waiting for its peers to notice: a peer whose child has
  nothing to write (an aggregate over a slow source) never would. It acts only on a report that
  arrives: a node reports once its relays have ended, so a fragment whose child died while its
  relay waits on a silent peer reports nothing yet. `[local: FaultVerdictTests]`
- **A transfer the hub cannot open is the cause, as a message.** When `InitTransferAsync` or the
  `Wire` push fails, the run tears down and `Cause` reads `transfer P -> C could not be opened by
  the hub`: no fragment is blamed as unresponsive, and the verdict gives no reason (which side, which
  group, which rule) to whoever reads it; the hub's log holds the detail. A fragment that went
  silent still outranks it. `[local: RunOrchestratorTests, FaultVerdictTests]`
- **A fragment absent from the reports is never read as success.** `DetermineOutcome` takes the
  fragment *names* from `RunSpec.Fragments` (versioning is fully resolved by admission time - the
  outcome rule has no reason to know a pin from a plain name) as well as the reports collected so far,
  precisely so a fragment that never reported `Exited` at all - the pair muet case, distinct from one
  that reported a fault - is still named as the cause even when everyone who *did* report is at 0.
  `[local: RunOrchestratorTests]`
- **A node that is disposed says so before it closes: `Unregister` drops its instance at once.**
  The hub processes a closed connection some time after the client closed it, and an instance still
  in the inventory in between is admittable, with the same version as its replacement: a `Launch` to
  it is never answered and the run ends at `ReadyTimeout`. `CoordinatorHub.Unregister` calls
  `INodeRegistry.Withdraw`, which removes the entry with no grace period and fires `FragmentLost`
  (an in-flight run that admitted it ends at once). The disconnect handling stays the only cover for
  a node that dies without saying so. `Withdraw` ignores a connection that no longer owns its entry,
  like `INodeRegistry.Unregister` (the disconnect handler; the hub method of the same name is the
  node's own announcement). `[local: NodeRegistryTests, RedeployTests]`
- **A dropped connection is a pending loss, not a lost instance, until its own grace period
  elapses.** `CoordinatorHub.OnDisconnectedAsync` marks the `INodeRegistry` entry rather than
  dropping it: an ordinary SignalR reconnect (its own default schedule: 0/2/10/30s) must not fail an
  in-flight run - the earlier attempt that fired an immediate teardown on every disconnect did exactly
  that. Each instance is keyed by (fragment name, `ClientId`) and carries its own grace timer, so a
  *different* `ClientId` registering the same fragment name never touches another instance's pending
  grace - it is simply a new, separate entry; a restarted node's old identity just times out on its
  own schedule like any other drop, while its replacement (if it gets a fresh `ClientId`) registers as
  an ordinary new instance. `RunOrchestrator` subscribes only once its own run is admitted (an
  unadmitted fragment is already `AdmissionGate`'s own named refusal, never this event) and compares
  the reported `ClientId` against the one it admitted, so a name that has moved on to a fresh node -
  or a *different*, non-selected live instance of the same name dropping (see "Versions" below) - is
  never read as *this* run's own fragment going quiet.
  `[local: NodeRegistryTests, CoordinatorAbortTests]`
- **`RunOrchestrator` resolves each `Launch`/`Wire` target's `ConnectionId` fresh, by `ClientId`,
  through `IStateStore.GetClientAsync`** - never from the admission snapshot, and never through
  `INodeRegistry`: TransportR's own state store already tracks the current `ConnectionId` against the
  stable `ClientId` on every reconnect, the same lookup `InitTransferAsync` makes internally at every
  handshake. A `null` `DisconnectedAtUtc` is what "currently reachable" means; a `ConnectionId` is
  never trusted while that field is set, even though the record survives past it for TransportR's own
  (much longer) grace period.
- **A run in which no fragment has failed has no coordinator-side clock; a remote failure starts one.** A
  legitimately long transfer, or a slow consumer, must not be reported failed for taking time, so the
  transfer phase is bounded only by `abort.Token`: the requester's own cancellation, `FragmentLost`, or a
  fragment's own *local* failure reported by `Exited`. A fragment that fails `FaultOrigin.Remote` - its
  transfer failed under it - has not decided the run, but its peers now owe their own report within
  `RunOrchestratorOptions.RemoteFailureGrace`. When it runs out the run, already failed, is torn down
  and every fragment is told to cancel, but the verdict names only the *peers* of the failed fragment
  that had not reported (`PeersOwingAReport`, then `DetermineOutcome`'s `unresponsive`, even if they
  report once told to cancel): never the fragment that failed because of them, a fragment silent only
  because its own peer is, or one on a branch the failure never touched. Keep the grace above
  `NodeRegistryOptions.DisconnectGracePeriod`, so a lost node is named by its loss and not by its
  silence. `ExitTimeout` bounds only the *teardown's* wait for an already-cancelled or
  already-terminated fragment to finish reporting. `[local: RemoteFailureGraceTests, RunOrchestratorTests]`
- **On teardown, every launched fragment still running is told to `Cancel`** after
  `ITransferTerminator.TerminateAsync` has ended each open transfer. A torn-down transfer faults a
  fully wired fragment only once one of its relays touches it, which never happens while its child
  has nothing to write (an aggregate over a slow source): without `Cancel` such a fragment holds the
  verdict until its source ends. A fully wired fragment answers `Cancel` with a
  `FaultOrigin.Remote` exit, a consequence like a mid-flow peer death, and stays out of the
  cancelled set; a fragment *not* fully wired - no transfer yet, or a multi-edge one still waiting on
  `Wire` for one side - joins the cancelled set, excluded from cause and consequence. The fragment
  identified as the cause is never sent `Cancel`, even when it is still
  reachable (one that stops responding right after admission is still connected) - it must stay
  absent from the run's reports for `DetermineOutcome` to name it, not be masked by a `Cancel` it
  happens to still receive. A fragment resolved as unreachable during this pass is excluded from the
  wait that follows for the same reason it was never sent one: it can never report on its own, and
  counting it among the stragglers would wait out the full `ExitTimeout` for nothing.
  `[local: RunOrchestratorTests, CoordinatorAbortTests]`

**Residual, not built:** a `Launch` sent to a `ConnectionId` that drops between resolution and
delivery is not retried - SignalR delivers to a dead connection silently, no exception - but still
falls to `ReadyTimeout`, since the fragment can then never report `Ready`. **A `Wire` dropped the
same way has no such backstop now that the data-transfer phase has no wall clock of its own**: the
fragment simply never reports `Exited`, and only the requester's own cancellation, an unrelated
`FragmentLost` or another fragment's local failure would ever end the wait - `PipelineNode.WireAsync` is not idempotent, so there is no
cheap retry on either side. A fragment's own `Exited` landing on a fresh connection before that
connection's reconnect-triggered `Register` has completed fails `ResolveFragment` with no retry on
the hub side; `PipelineNode` narrows this by holding the report and resending it once its own
re-`Register` succeeds, but a hub-side retry does not exist. Finally, a fragment that goes quiet by
**disposing its node** unregisters first (`FragmentLost` fires at once through `Withdraw`, and the run
ends naming it); the data plane resolves its transfers on its own orderly close. The disconnect grace
period is the path of a node that dies without saying so, the harder case, which `TerminateAsync`
also exists for: a peer that stops responding without ever closing anything. No in-process test can
drop a node's connection without going through its own disposal, so `NodeRegistryTests` alone covers
the grace.

## State and availability

- **The coordinator is not highly available.** Its state - the node registry, the plans, the run in
  flight - and TransportR's own (the default in-memory store) live in the process: a restart loses
  all of it, and the transfers it carried are gone. **Nothing reconstructs that state from what the
  nodes do next**: a node registering again is an ordinary registration, never evidence of a run, and
  no message tells a node to drop what it holds on a guess. A coordinator restart is an operator
  event: the operator restarts the node hosts, which kills their fragment children. High
  availability would be a mode of its own, with the state held in an external persistent store
  (TransportR's hub is highly available only with its Redis store and sticky sessions, and even then
  its transfers are local to the instance), designed as such rather than inferred from reconnections.
  `[local: tools/campaign.py F4 — hosts come back, the reset leaves nothing running]`
- **A node's token is checked when a transfer opens, not while the node sits idle.** A deployed
  pipeline left idle past its nodes' token lifetime has its next run refused (`could not be opened
  by the hub`) until its nodes are recreated: redeploying it does that, with a fresh token. Keep the
  token lifetime above the longest time a deployment sits idle (the lab issues 12 hours).
  `[local: tools/campaign.py S5]`

## Versions

A fragment's version identifies its own YAML: the SHA-256 hash of `PipelineNodeOptions.FragmentJobPath`'s
bytes (`PipelineNode`'s own helper, computed once before `Register`) - never the contract hash a
run's edges are checked against, a separate, still out-of-scope concern. An unpinned
`FragmentPin` is Docker's own "latest": whichever version the fragment's live instance currently
reports. Pinning names an exact version; "rollback" is not a separate mechanism, only pinning to a
version that still - or again - has a live instance behind it
(`AdmissionGateTests.RollbackIsFree...`).

Instance alignment - every live instance of one fragment name agreeing on version - is checked by
`AdmissionGate.Resolve` **unconditionally, even for a pinned request**: two disagreeing instances are
a defect of the peer itself, never something a pin could paper over by picking a side. Per-instance
version filtering (admit whichever instances happen to match, ignore the rest) was considered and
rejected: it would make canary-style partial rollout possible, which nothing here needs, and it would
quietly reopen the "which instance did this run actually use" question the whole design exists to
close - instance selection stays free (any live instance will do) only because every one of them is
guaranteed equivalent.

Four distinct admission refusals, each naming the fragment: **absent** (`AdmissionRefusedException`,
unpinned, no live instance at all) · **unknown version** (`PinnedVersionUnavailableException`,
`Reason: Unknown`, a pin nobody has ever reported) · **retired version** (same exception,
`Reason: Retired`, a pin once reported but nothing currently hosts - operationally: redeploy that
version, or pin to whatever is live) · **misaligned instances**
(`MisalignedInstancesException`, naming every distinct version found - the reading is "the peers
hosting this fragment disagree with each other, fix the deployment before retrying, no pin will help").
`NodeRegistry.HasKnownVersion` is what tells unknown from retired apart; it is never pruned - old
versions are appended to forever, since retention is a deferred product decision, not this registry's
job. `[local: NodeRegistryTests, AdmissionGateTests]`

`tests/DtPipe.Coordinator.Tests` is outside `DtPipe.sln`, so CI never runs it.
