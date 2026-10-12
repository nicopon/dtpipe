# DtPipe.Coordinator

Mechanics for editing the coordinator. The rules themselves are in the root `CLAUDE.md`.

The coordinator is the central hub of a distributed pipeline. A node is an identity; it is carried by one or
more hosts, its **replicas**, each running fragments for a run on a remote machine
(`src/DtPipe.PipelineNode/`, `src/DtPipe.PipelineNode.Hosting/`). The coordinator is the **control
plane**: it checks plans, knows which replicas are present, places each run, and opens every transfer. A
replica holds two connections to it: the **node channel**, a SignalR hub of the host (`NodeChannel`), which
carries every message about a run, and a TransportR client, which carries the **data plane**: the Arrow IPC
bytes between nodes.

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
- **The coordinator and every `DtPipe.PipelineNode` it drives run the same TransportR version; when they
  cannot move at once, the coordinator, which hosts the hub, moves first.** A node's refresh needs a hub of its
  own release: an older hub writes the connection's older expiry between the call's two steps, and its sweep can
  end the transfers of a client it then finds expired. Mixed versions also lose the resume of a transfer after a
  transient cut of a node's link, and nothing reports it. `[unchecked: mixed versions - a refresh against an older hub, the resume after a cut]`
- **A reverse proxy in front of the hub must accept `Expect: 100-continue`.** The client sends it on
  every write stream, and a proxy that answers `417` fails all of them. `[unchecked]`
- **A reverse proxy that filters by path must let `/api/client/*` through**, as it does
  `/api/stream/*`: token renewal travels there, and every connection closes at its token's expiry
  when it is blocked. `[unchecked]`

## Hosting

- **This project is a library, not an application.** It has no `Program`: the host (the server, or any
  ASP.NET Core app) owns the identity provider, the listening address, `MapDataHub` and **the node channel's
  hub**, and calls `AddCoordinatorHub`. The project references `Microsoft.AspNetCore.App` as a framework, so a
  package of it carries no executable. `[CI: validate_distributed_boundary.sh]`
- `AddCoordinatorHub` registers the flow-control matrix, `IPlanRegistry`, `LoggingHubProgressMonitor`, the
  peer-to-peer filter, `ReplicaRegistry`, `ReplicaAdmission`, `IRunOrchestrator` and `ReplicaProtocol`. It
  returns TransportR's `DataHubBuilder` over its plain `CommandHub`, which carries the data plane and nothing
  of dtpipe, and the caller finishes it (identity provider, then `Build()`). `AddNodeLink<THub>` names the hub
  of the node channel, which the orchestrator writes to through `INodeLink`.
- **The node channel's hub calls `ReplicaProtocol`, one line a method, and extends nothing.** The library finds
  a message's replica through its connection; what it cannot know (the rights, an enrolment queue) is the
  host's, decided before it forwards. `[local: ReplicaAdmissionSubjectTests, the server's NodeChannelHubTests]`
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
- **The data hub takes no message about a run.** `Register`, `Unregister`, `Ready` and `Exited` are not
  methods of the hub TransportR serves: a call to one is an error, not a no-op.
  `[local: ReplicaAdmissionSubjectTests, the lab's smoke.py rights - no run message on the data hub]`

## The barrier

`RunOrchestrator.RunAsync` is the barrier. It places every fragment of `RunSpec.Fragments` on a replica of
its node (`ReplicaAdmission`), launches them all (`Launch`, answered), opens each `RunSpec.Edges` entry through
`ITransferInitiator` and hands both ends their transfer (`Wire`, answered), waits for every `Exited` and
applies `DetermineOutcome`. `RunAsync` is an in-process API: the library maps no route for it. Every message to
a replica goes through `INodeLink`, bounded, and is an invocation the replica answers: a replica's client
handles pushes one after another, so a push that takes time delays the next.

A call is one `RunSession`, which `RunOrchestrator` builds and keeps in its table of runs while the call lasts. Its
methods are the phases, in the order of that sentence, then the teardown of a run that was aborted. Two entries run on
the thread of a hub method: `OnExitedAsync` (the session's report slots, its abort and its placement) and the handler of
`ReplicaLost`, subscribed once the run is placed (the placement, the lost fragment, the abort). `RunOrchestrator` keeps
the table, `OnExitedAsync` and the pure rules (`DetermineOutcome`, `PeersOwingAReport`, `IsReportOfItsLaunch`).
`[local: RunPhasesTests]`

- **Several runs may be in flight; each is judged alone.** Each run has its own state, found by run id. A
  lost replica, a local failure, the requester's cancellation and the grace after a remote failure end only
  the run they belong to. A replica serves several runs, even of one fragment. Which runs may overlap is the
  caller's choice; the orchestrator never queues. `[local: ConcurrentRunsTests, RunPhasesTests]`
- **A caller is its connection, never an argument.** `ReplicaProtocol` finds a replica through the connection
  a message arrived on. An `Exited` is heard only from the replica the run placed that fragment on: any
  other, a replica of the same identity included, is answered `NotAdmitted`, and a connection that announced
  nothing `UnknownConnection`. The fragment an `Exited` names finds its slot; it never says who the sender is.
  `[local: ReplicaAdmissionSubjectTests, ConcurrentRunsTests, RunOrchestratorTests]`
- **An `Exited` is also the report of the launch that started the child, or it is not heard.** The orchestrator
  mints a random token for each launch of each fragment, publishes it with the placement, and sends it in
  `LaunchRequest`; the replica returns it in `FragmentExit`. A report is admitted on the run, the fragment, the
  replica and the token together (`RunOrchestrator.IsReportOfItsLaunch`), whatever the order of the report and of
  the answer to the `Launch`; a report with none, or another launch's, is `NotAdmitted`. A run id is reassigned
  after the coordinator restarts, so an earlier child that held the same run id, fragment and replica reports
  under another token. The token is no secret (the replica holds it) and authenticates nobody: the
  connection still names the sender. Never mint it from a counter, or from anything that restarts with the
  process. `[local: ExitBeforeLaunchAnswerTests, RunOrchestratorTests, LaunchTokenWireTests, RunPhasesTests; the lab's smoke.py rights - a foreign report]`
- **Admission binds a fragment to its node's identity.** `RunFragment.Node` is the subject the node's
  connections attest (`ReplicaRegistry` keys a replica by the subject of its connection, never by the name it
  proposes), and the caller supplies it: the library never infers it. A fragment that names no node, or no
  groups, is refused at once (`UnboundFragmentException`); only `AdmissionOptions.AdmitUnboundFragments`, set
  by a host whose hub attests no subject (development, a test with no token), files a replica under the name
  it proposes and reads absent groups as no comparison. A host with an identity provider never sets it.
  `[local: ReplicaAdmissionSubjectTests, ReplicaRegistryTests]`
- **A replica is leased for the run, and nothing moves the run.** The lease `ReplicaRegistry` takes at admission
  lasts until the run ends or the replica is lost, and no path retries a fragment on another replica. The replica
  each fragment ran on is in `RunResult.Replicas`.
  `[local: ReplicaRegistryTests, ReplicaAdmissionSubjectTests]` `[unchecked: no retry on another replica]`
- **A refused launch is the cause, as a message.** A replica that answers `Launch` with a refusal ran nothing:
  the fragment is excluded from cause and consequence, as one the coordinator cancelled, the run is torn down,
  and `Cause` reads `<fragment> could not start on replica <id>: <reason>` with `CauseIsUnresponsive` false.
  A replica that does not answer within `ReadyTimeout`, or has no connection to ask on, is a fragment gone
  quiet instead. `[local: ReplicaAdmissionSubjectTests, CoordinatorAbortTests, RunPhasesTests]`
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
  organic one. A report's origin is one of the two words of `ExitOrigins`, and any other word reads as `Local`;
  `DtPipe.PipelineNode` declares its own `FaultOrigin`, which `FaultOriginWordsTests` compares with this one.
  `[local: RunOrchestratorTests, CoordinatorDrivenTests, ScriptedRunTests, PipelineNode.Tests FaultOriginWordsTests]`
- **A fragment's own failure ends the run at once.** `OnExitedAsync` raises the run's abort on the
  first `Exited` that is non-zero with a `FaultOrigin.Local` origin, so the teardown tells every
  fragment still running to `Cancel` without waiting for its peers to notice: a peer whose child has
  nothing to write (an aggregate over a slow source) never would. It acts only on a report that
  arrives: a node reports once its relays have ended, so a fragment whose child died while its
  relay waits on a silent peer reports nothing yet. The abort is raised with `CancelAsync`, so the teardown
  never runs in the hub method that carried the report, where a replica's messages are serialised.
  `[local: FaultVerdictTests]` `[unchecked: CancelAsync rather than Cancel]`
- **A transfer the hub cannot open is the cause, as a message.** When `InitTransferAsync` or a `Wire`
  fails (or a replica answers that it knows no such edge), the run tears down and `Cause` reads `transfer P
  -> C could not be opened by the hub`: no fragment is blamed as unresponsive, and the verdict gives no
  reason (which side, which group, which rule) to whoever reads it; the hub's log holds the detail. A
  fragment that went silent still outranks it. `[local: RunOrchestratorTests, FaultVerdictTests, ScriptedRunTests]`
- **A fragment absent from the reports is never read as success.** `DetermineOutcome` takes the
  fragment *names* from `RunSpec.Fragments` as well as the reports collected so far, precisely so a
  fragment that never reported `Exited` at all - distinct from one that reported a fault - is still named
  as the cause even when everyone who *did* report is at 0. `[local: RunOrchestratorTests]`
- **A replica that says it is leaving ends its runs at once; one that is merely cut waits out its grace.**
  `ReplicaRegistry.Leave` removes the replica with no grace period and fires `ReplicaLost`; a dropped
  connection (`Disconnected`) only marks the replica, which stays reclaimable by the same host announcing
  again (same instance id, with its runs) for `ReplicaRegistryOptions.DisconnectGracePeriod`, and is lost
  only then. An ordinary SignalR reconnect (its own default schedule: 0/2/10/30s) must not fail an
  in-flight run. A run subscribes to `ReplicaLost` only once it is placed, and compares the lost key with the
  replica it placed each fragment on, so a sibling replica dropping, or a replacement of this one, is never
  read as this run's own fragment going quiet. The disconnect handling stays the only cover for a node
  that dies without saying so. `[local: ReplicaRegistryTests, ReplicaAdmissionSubjectTests, CoordinatorAbortTests, RunPhasesTests]`
- **A run in which no fragment has failed has no coordinator-side clock; a remote failure starts one.** A
  legitimately long transfer, or a slow consumer, must not be reported failed for taking time, so the
  transfer phase is bounded only by `abort.Token`: the requester's own cancellation, `ReplicaLost`, or a
  fragment's own *local* failure reported by `Exited`. A fragment that fails `FaultOrigin.Remote` - its
  transfer failed under it - has not decided the run, but its peers now owe their own report within
  `RunOrchestratorOptions.RemoteFailureGrace`. When it runs out the run, already failed, is torn down
  and every fragment is told to cancel, but the verdict names only the *peers* of the failed fragment
  that had not reported (`PeersOwingAReport`, then `DetermineOutcome`'s `unresponsive`, even if they
  report once told to cancel): never the fragment that failed because of them, a fragment silent only
  because its own peer is, or one on a branch the failure never touched. Keep the grace above
  `ReplicaRegistryOptions.DisconnectGracePeriod`, so a lost node is named by its loss and not by its
  silence. `ExitTimeout` bounds only the *teardown's* wait for an already-cancelled or
  already-terminated fragment to finish reporting. `[local: RemoteFailureGraceTests, RunOrchestratorTests, ScriptedRunTests]`
- **On teardown, every launched fragment still running is told to `Cancel`**, in parallel and each bounded by
  `LinkTimeout`, after `ITransferTerminator.TerminateAsync` has ended each open transfer. A torn-down transfer
  faults a fully wired fragment only once one of its relays touches it, which never happens while its child
  has nothing to write (an aggregate over a slow source): without `Cancel` such a fragment holds the
  verdict until its source ends. A fully wired fragment answers `Cancel` with a
  `FaultOrigin.Remote` exit, a consequence like a mid-flow peer death, and stays out of the
  cancelled set; a fragment *not* fully wired - no transfer yet, or a multi-edge one still waiting on
  `Wire` for one side - joins the cancelled set, excluded from cause and consequence. The fragment
  identified as the cause is never sent `Cancel`, even when it is still
  reachable (one that stops responding right after admission is still connected) - it must stay
  absent from the run's reports for `DetermineOutcome` to name it, not be masked by a `Cancel` it
  happens to still receive. A fragment whose `Cancel` cannot be delivered is unreachable, and one whose replica
  answers that it knows no such run never started: neither can report on its own, and counting them among the
  stragglers would wait out the full `ExitTimeout` for nothing.
  `[local: RunOrchestratorTests, CoordinatorAbortTests, RunPhasesTests, ScriptedRunTests]`

## Replicas and eligibility

A replica is one host carrying a node's identity. `ReplicaRegistry` is the one table of them: keyed by the
subject of the connection and an instance id the host chose once for its process, holding what each
announced, its grace, and the runs the coordinator placed on it (a lease per run and fragment). A node is online
while any replica is connected.

- **A plan, a deployment and a run request name a node, never a replica.** The replica is chosen at admission,
  for the run. `[unchecked]`
- **A replica is eligible for a fragment when** it is connected; it is of the fragment's node; it announces
  `NodeAnnouncement.LaunchToken`; its TransportR client is verified; and the bricks it announces compose the
  version of the plan.
  `ReplicaChooser` is that rule, pure. The client is verified when it is announced, registered, still
  connected, registered under the subject of the replica's connection, and holding exactly the groups the
  rights give the node now: closed by default, so a registration written before the rights changed is out
  until its client registers again. The composition is `FragmentJob.ComposeFor`, the function the replica
  applies again at `Launch`, over the one reading of an announced brick (`FragmentJob.Declared`), so what
  the coordinator calls eligible the replica runs. `[local: ReplicaAdmissionSubjectTests]`
- **A replica at another version is out of that fragment, never a reason to refuse the run.** A rolling
  update leaves replicas at two versions of a brick: the run goes to one that composes the plan. Admission
  refuses only when none does (`NoEligibleReplicaException`, naming each fragment and why, replica by
  replica) after `AdmissionTimeout`. `[local: ReplicaAdmissionSubjectTests]`
- **A replica that does not announce `NodeAnnouncement.LaunchToken` is out of every run.** Its report would carry
  no token and could not be told from a stale one, so it is never placed: a node updates replica by replica
  without interruption, and the runs of a node whose only replica is older are refused, naming the reason,
  until that replica is updated. A host updated before its coordinator is not refused: the older coordinator
  ignores the field and the capability. The order of an update that cannot move at once is the TransportR
  rule above, the coordinator first.
  `[local: LaunchTokenEligibilityTests, LaunchTokenWireTests]` `[unchecked: a host updated before its coordinator]`
- **The client id a replica announces is verified twice.** At the announcement, as a courtesy: one registered
  under another subject, or announced by another replica, is refused. At admission, as the authority: the
  registration is read at the moment of choosing. Only the second sees a client registered after the
  announcement. A replica claiming another identity's client would be handed that identity's transfers.
  `[local: ReplicaAdmissionSubjectTests]`
- **Among the eligible, the replica with the fewest runs is chosen, then the least recently chosen, then the
  lowest instance id**, for every fragment of the run in one step: the choice is read again under the
  registry's lock against the replica as it is, and leased there, or none is. `[local: ReplicaRegistryTests]`
- **The hub reads a client's groups from the rights each time the client registers or renews its token.** A replica
  has it done on demand with `PipelineNode.RefreshRegistrationAsync` (the call's contract is in
  `src/DtPipe.PipelineNode/CLAUDE.md`), and the hub judges each open transfer against the new groups before that call
  returns. Admission does not wait for it: it reads the rights when it chooses, so a replica whose client holds other
  groups than the rights give its node is out until its registration matches.
  `[local: GroupRefreshTests, PipelineNode.Tests RefreshRegistrationTests, ReplicaAdmissionSubjectTests]`

## State and availability

- **The coordinator is not highly available.** Its state - the replica registry, the plans, the runs in
  flight - and TransportR's own (the default in-memory store) live in the process: a restart loses
  all of it, and the transfers it carried are gone. **Nothing reconstructs that state from what the
  nodes do next**: a replica announcing again is an ordinary announcement, never evidence of a run, and
  no message tells a node to drop what it holds on a guess. A coordinator restart is an operator
  event: the operator restarts the node hosts, which kills their fragment children. High
  availability would be a mode of its own, with the state held in an external persistent store
  (TransportR's hub is highly available only with its Redis store and sticky sessions, and even then
  its transfers are local to the instance), designed as such rather than inferred from reconnections.
  `[local: tools/campaign.py F4 — hosts come back, the reset leaves nothing running]`
- **A node renews its own token, and the hub closes a connection whose token ran out.** A node built
  with `AccessTokenProvider` renews ahead of the expiry, so an idle or a long-running node outlives
  its token's lifetime. A node given a static token cannot renew: its connection is closed at the
  expiry. A node that cannot renew has its transfers ended and its connection closed by the hub
  within its access sweep. `[local: tools/campaign.py S3, S5]`

`tests/DtPipe.Coordinator.Tests` is outside `DtPipe.sln`, so `build.yml` never runs it; `build.sh`
discovers it, so `agentic-ci.yml` runs it on Linux.
