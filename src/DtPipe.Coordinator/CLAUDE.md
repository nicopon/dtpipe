# DtPipe.Coordinator

Mechanics for editing the coordinator. The rules themselves are in the root `CLAUDE.md` and
`.claude/rules/distributed-pipelines.md`.

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
- Put hosting policy in the hub's options on this side, not in TransportR's code:
  `AccessOptions.ClientsMayOpenTransfers` is set to `false` by `AddCoordinatorHub`.
- TransportR is referenced as NuGet packages (`TransportR`, `TransportR.Hub`). Bumping the
  pinned version is a port, not a copy: check each API change against this project and
  `DtPipe.PipelineNode` before moving it.
- `DefaultFlowControlService`/`FlowControlOptions` (namespace `TransportR.Hub`) ship inside
  the `TransportR.Hub` package.
- **The coordinator and every `DtPipe.PipelineNode` it drives run the same TransportR version; when they
  cannot move at once, the coordinator, which hosts the hub, moves first.** A node's refresh needs a hub of its
  own release: an older hub writes the connection's older expiry between the call's two steps, and its sweep can
  end the transfers of a client it then finds expired. Mixed versions also lose the resume of a transfer after a
  transient cut of a node's link, and nothing reports it. `[unchecked: mixed versions - a refresh against an older hub, the resume after a cut]`
- **A reverse proxy in front of the hub must accept `Expect: 100-continue`.** The client sends it on
  every write stream, and a proxy that answers `417` fails all of them. `[unchecked]`
- **A reverse proxy that filters by path must let `/api/client/*` through**, as it does
  `/api/stream/*`: token renewal travels there, and every connection closes at its token's expiry
  when it is blocked. The write-transport probe's `GET` and `POST` on `/api/stream/probe/send` are under the
  second prefix; a proxy that blocks them leaves the probe unanswered, and nothing else. `[unchecked]`

## Hosting

- **This project is a library, not an application.** It has no `Program`: the host (the server, or any
  ASP.NET Core app) owns the identity provider, the listening address, `MapDataHub` and **the node channel's
  hub**, and calls `AddCoordinatorHub`. The project references `Microsoft.AspNetCore.App` as a framework, so a
  package of it carries no executable. `[CI: validate_distributed_boundary.sh]`
- `AddCoordinatorHub` registers the flow-control matrix, `IPlanRegistry`, `LoggingHubProgressMonitor`, the
  `AccessOptions` setting that refuses client-opened transfers, `ReplicaRegistry`, `ReplicaAdmission`, `IRunOrchestrator` and `ReplicaProtocol`. It
  returns TransportR's `DataHubBuilder` over its own hub, which carries the data plane and nothing
  of dtpipe, and the caller finishes it (`AddDataHub` has registered the identity provider that validates tokens; the caller's
  `UseDevMode` or its own provider replaces it). `AddNodeLink<THub>` names the hub
  of the node channel, which the orchestrator writes to through `INodeLink`.
- **The node channel's hub calls `ReplicaProtocol`, one line a method, and extends nothing.** The library finds
  a message's replica through its connection; what it cannot know (the rights, an enrolment queue) is the
  host's, decided before it forwards. `[CI: ReplicaAdmissionSubjectTests, the server's NodeChannelHubTests]`
- `AddDataHub()` registers a no-op progress monitor only when none is registered; a host's own, registered
  before it, is kept, and one registered after it wins.
- **A progress monitor implements both write announcements itself.** `OnWriteStreamOpened` and
  `OnWriteTransportProbed` have an empty default on `IHubProgressMonitor`: a monitor without them compiles and says
  nothing. `LoggingHubProgressMonitor` writes one `Information` line for each, and `EventProgressMonitor` (the server)
  delegates to it first, then puts one line on the pages' log, naming the replica that owns the client when the
  registry knows it. `[CI: EventProgressMonitorTests, WriteAnnouncementTests]`
- **Peers never address each other.** With `AccessOptions.ClientsMayOpenTransfers` at `false`, the hub refuses a
  client's `GetReceiversAsync` and `InitTransferAsync`. Only the coordinator opens a transfer, through
  `ITransferInitiator`, which the option does not touch. `[CI: PeerTransferRefusalTests]`
- **Check a plan before anything opens.** `PlanRegistry.Register` refuses a plan with any edge the
  flow-control matrix forbids, and names every forbidden edge. The same matrix governs runtime
  transfers. `[CI: PlanRegistryTests]`
- **The data hub takes no message about a run.** TransportR's hub serves the data plane over HTTP (client registration and
  streams) and has no route for `Register`, `Unregister`, `Ready` or `Exited`: a run is told over the node channel only.
  `[CI: ReplicaAdmissionSubjectTests] [unchecked: no test calls the data hub for a run message]`

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
`[CI: RunPhasesTests]`

- **Several runs may be in flight; each is judged alone.** Each run has its own state, found by run id. A
  lost replica, a local failure, the requester's cancellation and the grace after a remote failure end only
  the run they belong to. A replica serves several runs, even of one fragment. Which runs may overlap is the
  caller's choice; the orchestrator never queues. `[CI: ConcurrentRunsTests, RunPhasesTests]`
- **A caller is its connection, never an argument.** `ReplicaProtocol` finds a replica through the connection
  a message arrived on. An `Exited` is heard only from the replica the run placed that fragment on: any
  other, a replica of the same identity included, is answered `NotAdmitted`, and a connection that announced
  nothing `UnknownConnection`. The fragment an `Exited` names finds its slot; it never says who the sender is.
  `[CI: ReplicaAdmissionSubjectTests, ConcurrentRunsTests, RunOrchestratorTests]`
- **An `Exited` is also the report of the launch that started the child, or it is not heard.** The orchestrator
  mints a random token for each launch of each fragment, publishes it with the placement, and sends it in
  `LaunchRequest`; the replica returns it in `FragmentExit`. A report is admitted on the run, the fragment, the
  replica and the token together (`RunOrchestrator.IsReportOfItsLaunch`), whatever the order of the report and of
  the answer to the `Launch`; a report with none, or another launch's, is `NotAdmitted`. A run id is reassigned
  after the coordinator restarts, so an earlier child that held the same run id, fragment and replica reports
  under another token. The token is no secret (the replica holds it) and authenticates nobody: the
  connection still names the sender. Never mint it from a counter, or from anything that restarts with the
  process. `[CI: ExitBeforeLaunchAnswerTests, RunOrchestratorTests, LaunchTokenWireTests, RunPhasesTests] [local: the lab's smoke.py rights - a foreign report]`
- **Admission binds a fragment to its node's identity.** `RunFragment.Node` is the subject the node's
  connections attest (`ReplicaRegistry` keys a replica by the subject of its connection, never by the name it
  proposes), and the caller supplies it: the library never infers it. A fragment that names no node, or no
  groups, is refused at once (`UnboundFragmentException`); only `AdmissionOptions.AdmitUnboundFragments`, set
  by a host whose hub attests no subject (development, a test with no token), files a replica under the name
  it proposes and reads absent groups as no comparison. A host with an identity provider never sets it.
  `[CI: ReplicaAdmissionSubjectTests, ReplicaRegistryTests]`
- **A replica is leased for the run, and nothing moves the run.** The lease `ReplicaRegistry` takes at admission
  lasts until the run ends or the replica is lost, and no path retries a fragment on another replica. The replica
  each fragment ran on is in `RunResult.Replicas`.
  `[CI: ReplicaRegistryTests, ReplicaAdmissionSubjectTests]` `[unchecked: no retry on another replica]`
- **A refused launch is the cause, as a message.** A replica that answers `Launch` with a refusal ran nothing:
  the fragment is excluded from cause and consequence, as one the coordinator cancelled, the run is torn down,
  and `Cause` reads `<fragment> could not start on replica <id>: <reason>` with `CauseIsUnresponsive` false.
  A replica that does not answer within `ReadyTimeout`, or has no connection to ask on, is a fragment gone
  quiet instead. `[CI: ReplicaAdmissionSubjectTests, CoordinatorAbortTests, RunPhasesTests]`
- **A cursor rides on the launch and on the report, and the library keeps none.** `RunFragment.Cursor` is sent in `LaunchRequest` and is
  no part of a fragment's version; `FragmentExit.Cursors` reaches `FragmentExitReport` under the same rule as the rest of an `Exited`: heard
  from the replica the run placed and under the launch's token. What is kept, and when, is the host's (`CursorKeeper`).
  `[CI: Server.Tests CursorRunTests]`
- **A 0 exit code is not proof of delivery.** `DetermineOutcome` also compares each edge's two row
  counts (`RunResult.EdgeCounts`) and fails the run, naming the edge, on a mismatch even when every
  fragment reported 0 - a fragment attests its own process, not what its peer received. A count
  missing on either side fails closed (never reads as agreement); `RunResult.Describe()` renders
  every edge's two counts regardless of which branch produced the outcome. `[CI: RunOrchestratorTests]`
- **A run result keeps what each end of an edge reported of its protection, and an end that reported nothing is unknown.**
  `FragmentExit.EdgeProtections` reaches `FragmentExitReport` under the same rule as the rest of an `Exited`, kept only as the words of
  `DataPlaneProtections` (`RunOrchestrator.ProtectionsReported`), and `EdgeCount.ProducerProtection` and `ConsumerProtection` are looked up
  from it by each end's own alias, as `Sent` and `Received` are. No level is made of the two: their difference is the reader's. A fragment
  that reported no table, no entry for the alias or a text that is no word leaves its end null, never the level that seals nothing and never
  the level its replica announced. Nothing here refuses, warns or changes a verdict, and `Describe()` renders none of it.
  `[CI: ScriptedRunTests, EdgeProtectionReportWireTests; PipelineNode.Tests CoordinatorDrivenTests]`
- **The cause is the first *locally* faulted fragment, never arrival order.** A peer aborting its own
  transfer can fail a healthy fragment before the fragment that actually died reports `Exited`, so
  `DetermineOutcome` picks the cause by `FaultOrigin.Local`, falling back to `RunSpec.Fragments`'
  own order - never arrival order - only when no report carries that origin. A fragment the
  coordinator itself told to `Cancel` is excluded from both cause and consequence: its own child was
  killed on command, and reports a plain non-zero `Local` exit indistinguishable on the wire from an
  organic one. A report's origin is one of the two words of `ExitOrigins`, and any other word reads as `Local`;
  `DtPipe.PipelineNode` declares its own `FaultOrigin`, which `FaultOriginWordsTests` compares with this one.
  `[CI: RunOrchestratorTests, CoordinatorDrivenTests, ScriptedRunTests, PipelineNode.Tests FaultOriginWordsTests]`
- **When no fault is a fragment's own, the category an end reports of its edge names the cause, and nothing else does.**
  `FragmentExit.EdgeFailures` reaches `FragmentExitReport` under the same rule as the rest of an `Exited`, kept only as the words of
  `TransferFailures` (`RunOrchestrator.FailuresReported`, the filter `ProtectionsReported` applies). `DetermineOutcome` reads them only
  where it would otherwise name the first fault: the first edge of `RunSpec.Edges` an end of which failed end-to-end encryption is the
  cause, as `producer -> consumer`, and every failed fragment its consequence; else the receiver of the first edge whose sender had no
  acknowledgement, the sender a consequence. A cancellation, a silence, a wiring failure and a fragment's own fault, 130 included,
  stay ahead of them, and the outcome stays `Failed`. Reports with no category give the verdict the rule gives without them, to the
  character. `RunResult.CauseKind`, a word of `CauseKinds`, says what every `Cause` names; `CauseIsUnresponsive` stays beside it and
  `Describe()` is unchanged. The coordinator never reads an exception's message to decide.
  `[CI: EdgeFailureVerdictTests, EdgeFailureReportWireTests; PipelineNode.Tests CoordinatorDrivenTests]`
- **A fragment's own failure ends the run at once.** `OnExitedAsync` raises the run's abort on the
  first `Exited` that is non-zero with a `FaultOrigin.Local` origin, so the teardown tells every
  fragment still running to `Cancel` without waiting for its peers to notice: a peer whose child has
  nothing to write (an aggregate over a slow source) never would. It acts only on a report that
  arrives: a node reports once its relays have ended, so a fragment whose child died while its
  relay waits on a silent peer reports nothing yet. The abort is raised with `CancelAsync`, so the teardown
  never runs in the hub method that carried the report, where a replica's messages are serialised.
  `[CI: FaultVerdictTests]` `[unchecked: CancelAsync rather than Cancel]`
- **A transfer the hub cannot open is the cause, as a message.** When `InitTransferAsync` or a `Wire`
  fails (or a replica answers that it knows no such edge), the run tears down and `Cause` reads `transfer P
  -> C could not be opened by the hub`: no fragment is blamed as unresponsive, and the verdict gives no
  reason (which side, which group, which rule) to whoever reads it; the hub's log holds the detail. A
  `HubRequestFailedException` adds the word of its `Category`, read from the property (`RunSession.WiringFailureOf`); a refusal
  adds none, since the hub does not tell a right from a client gone or unknown. A
  fragment that went silent still outranks it. `[CI: RunOrchestratorTests, FaultVerdictTests, ScriptedRunTests]`
- **A fragment absent from the reports is never read as success.** `DetermineOutcome` takes the
  fragment *names* from `RunSpec.Fragments` as well as the reports collected so far, precisely so a
  fragment that never reported `Exited` at all - distinct from one that reported a fault - is still named
  as the cause even when everyone who *did* report is at 0. `[CI: RunOrchestratorTests]`
- **A replica that says it is leaving ends its runs at once; one that is merely cut waits out its grace.**
  `ReplicaRegistry.Leave` removes the replica with no grace period and fires `ReplicaLost`; a dropped
  connection (`Disconnected`) only marks the replica, which stays reclaimable by the same host announcing
  again (same instance id, with its runs) for `ReplicaRegistryOptions.DisconnectGracePeriod`, and is lost
  only then. An ordinary SignalR reconnect (its own default schedule: 0/2/10/30s) must not fail an
  in-flight run. A run subscribes to `ReplicaLost` only once it is placed, and compares the lost key with the
  replica it placed each fragment on, so a sibling replica dropping, or a replacement of this one, is never
  read as this run's own fragment going quiet. The disconnect handling stays the only cover for a node
  that dies without saying so. `[CI: ReplicaRegistryTests, ReplicaAdmissionSubjectTests, CoordinatorAbortTests, RunPhasesTests]`
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
  already-terminated fragment to finish reporting. `[CI: RemoteFailureGraceTests, RunOrchestratorTests, ScriptedRunTests]`
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
  `[CI: RunOrchestratorTests, CoordinatorAbortTests, RunPhasesTests, ScriptedRunTests]`

## Replicas and eligibility

A replica is one host carrying a node's identity. `ReplicaRegistry` is the one table of them: keyed by the
subject of the connection and an instance id the host chose once for its process, holding what each
announced, its grace, and the runs the coordinator placed on it (a lease per run and fragment). A node is online
while any replica is connected.

- **A plan, a deployment and a run request name a node, never a replica.** The replica is chosen at admission,
  for the run. `[unchecked]`
- **A replica is eligible for a fragment when** it is connected; it is of the fragment's node; it does not announce a
  protection of the data plane below the deployment's requirement; its TransportR
  client is verified; and the bricks it announces compose the
  version of the plan.
  `ReplicaChooser` is that rule, pure. The client is verified when it is announced, registered, still
  connected, registered under the subject of the replica's connection, and holding exactly the groups the
  rights give the node now: closed by default, so a registration written before the rights changed is out
  until its client registers again. The composition is `FragmentJob.ComposeFor`, the function the replica
  applies again at `Launch`, over the bricks it declares; the coordinator compares the versions a replica announces
  (`FragmentJob.VersionRefusal`), so what the coordinator calls eligible the replica runs. `[CI: ReplicaAdmissionSubjectTests]`
- **A replica at another version is out of that fragment, never a reason to refuse the run.** A rolling
  update leaves replicas at two versions of a brick: the run goes to one that composes the plan. Admission
  refuses only when none does (`NoEligibleReplicaException`, naming each fragment and why, replica by
  replica) after `AdmissionTimeout`. `[CI: ReplicaAdmissionSubjectTests]`
- **Eligibility is a fragment's, joining is an edge's.** The two ends of an edge join when they announce the same
  level and, where both name a key id, the same one (`ReplicaProtection.Joins`, `EdgeProtection`): TransportR has no
  downgrade, and a transfer between ends set differently fails before its first batch. An edge is a `RunEdge` of the run,
  its producer and consumer fragments as the plan pairs them and the run wires them, so two fragments of one node, whose
  replicas may differ during a rolling update, answer to it too; a fragment's own edges (`FragmentSpec.Edges`) name an alias
  and a direction only. The relation is not transitive (a key, no key, another key), so `ReplicaRegistry.TryReserve` takes
  the fragments in order and gives each the first candidate, in the order of preference below, that still lets every edge join, checked
  by `EdgeProtection.Joinable` (linear in the fragments, their candidates' protections and the edges, once per protection a
  fragment tries); a run whose candidates all protect alike is placed by the order alone. When each fragment has a
  candidate and no choice joins, `NoEligibleReplicaException` names, by `edge P -> C`, each edge that cannot join together
  with the edges before it that could, and what each end does: the words of `ReplicaProtection.Words`, a key id whole.
  `EvaluateAsync` judges no edge; `ReplicaAdmission.EdgeRefusals` judges pairs of connected replicas, edge by edge, for
  the server's arming. `[CI: EdgeProtectionTests]`
- **A deployment's required protection is a floor on a replica's level, and a replica below it is eligible for nothing.**
  `AdmissionOptions.RequiredProtection` is a word of `ProtectionPolicy`, the one order of the levels and the one reading of a
  word (the setting that fills it refuses any other text, and so does the property). `ReplicaAdmission.Refusal` puts
  `ReplicaChooser.ProtectionRefusal` first, so `EvaluateAsync`, `TryReserveAsync` and `Explain` carry its reason, in the words
  of `ReplicaProtection.Words` with no key id; there is no path of its own, and a runner is a node like any other. The replica is
  never refused at the announcement or cut: `ReplicaProtocol` and `ReplicaRegistry` know nothing of the policy. `EdgeRefusals`
  leaves such a replica out of the pairs it judges, so a node whose connected replicas are all below the requirement has none and
  is named by admission, not as an edge that cannot join. `[CI: RequiredProtectionTests; Server.Tests ArmedDeploymentTests]`
- **The client id a replica announces is verified twice.** At the announcement, as a courtesy: one registered
  under another subject, or announced by another replica, is refused. At admission, as the authority: the
  registration is read at the moment of choosing. Only the second sees a client registered after the
  announcement. A replica claiming another identity's client would be handed that identity's transfers.
  `[CI: ReplicaAdmissionSubjectTests]`
- **A replica's protection of the data plane is read, never trusted.** The announcement's level and key id are text a host chose:
  `ReplicaView.DataPlaneProtection` and `KeyId` are the one reading of them (`DataPlaneProtections`), and any other reader goes
  through them. A word it does not know, or none, reads as the level that seals nothing; an id is kept only on the level that
  carries one and only if it has an id's form. The registry keeps the whole announcement and the key never reaches it: the
  node announces a fingerprint. Admission and arming read the level through `ReplicaProtection.Of`, to join an edge's ends,
  and `ProtectionPolicy` reads it to judge a replica against the requirement; no verdict reads it.
  `[CI: DataPlaneProtectionWireTests; Server.Tests NodeChannelHubTests — what the node's API shows of it]`
- **Among the eligible, the replica with the fewest runs is chosen, then the least recently chosen, then the
  lowest instance id**, among those that let every edge of the run join, for every fragment of the run in one step: the
  choice is read again under the registry's lock against the replica as it is, and leased there, or none is.
  `[CI: ReplicaRegistryTests, EdgeProtectionTests]`
- **The hub reads a client's groups from the rights each time the client registers or renews its token.** A replica
  has it done on demand with `PipelineNode.RefreshRegistrationAsync` (the call's contract is in
  `src/DtPipe.PipelineNode/CLAUDE.md`), and the hub judges each open transfer against the new groups before that call
  returns. Admission does not wait for it: it reads the rights when it chooses, so a replica whose client holds other
  groups than the rights give its node is out until its registration matches.
  `[CI: GroupRefreshTests, PipelineNode.Tests RefreshRegistrationTests, ReplicaAdmissionSubjectTests]`

## State and availability

- **The coordinator is one active instance that restarts fast, not a highly available service.** What a host must not lose - the run journal,
  the rights, the armed deployments, the session keys, the pipeline library - is kept outside the process (the server's are its database, SQLite, PostgreSQL or SQL Server, and a
  git remote: `src/DtPipe.Coordinator.Server/CLAUDE.md`), so a restart on an empty state directory loses none of it. What lives in the
  process is lost: the replica registry (rebuilt as replicas announce), the plans, the runs in flight and the transfers they carried,
  and TransportR's own store (the default, in memory). **A restart is read from the host's run journal, never from what the
  nodes do next**: a replica announcing again is an ordinary announcement, never evidence of a run. The host journals each
  run when it is queued, when it starts and when it ends, in a store that outlives the process (the server's is its database);
  after a restart it ends the runs it finds in flight, and asks each
  replica of a node that carried a started run to abandon those fragments (`INodeLink.CancelAsync`, answered false by a
  replica that holds none). No message tells a node to drop what it holds on a guess: the authority is a journaled run.
  A restart that finds no journal, or a replica that never announces again, still falls to the operator, who restarts the
  node hosts, which kills their fragment children. A second instance takes over only as the server's passive standby
  (`Coordinator:Standby`), once the first has let go of its database, and its takeover is a restart: it serves nothing before, and
  the runs in flight are lost. Never two active instances: TransportR's hub is highly available only with its Redis store and
  sticky sessions, its transfers stay local to the instance, and it refuses a transfer a host opens between two instances, so the
  state outside the process serves a fast restart and never an active/active pair.
  `[local: tools/campaign.py F4 - the lost run keeps its id, the abandon reaches the children]`
- **A node renews its own token, and the hub closes a connection whose token ran out.** A node built
  with `AccessTokenProvider` renews ahead of the expiry, so an idle or a long-running node outlives
  its token's lifetime. A node given a static token cannot renew: its connection is closed at the
  expiry. A node that cannot renew has its transfers ended and its connection closed by the hub
  within its access sweep. `[local: tools/campaign.py S3, S5]`

`tests/DtPipe.Coordinator.Tests` is outside `DtPipe.sln`; `run_unit_tests.sh` discovers it, so the test job of
`build.yml` runs it on Linux.
