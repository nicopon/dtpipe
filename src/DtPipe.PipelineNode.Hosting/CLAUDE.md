# DtPipe.PipelineNode.Hosting

Mechanics for editing the node host. The rules themselves are in the root `CLAUDE.md`.

`AddDtPipeNode` makes any .NET host a node: a business service that offers bricks, or the official
runner (`DtPipe.PipelineNode.Runner`, which only calls `RunAnyFragment`). It is a hosted service
(`NodeAgent`), not HTTP middleware: the node opens its connections to the coordinator, it serves
none. One node per process, which is one replica of its node's identity; a second `AddDtPipeNode` throws.

- **A data node composes its job from its own declarations, and the policy is the host's own.**
  The coordinator names bricks (`FragmentSpec.Units`: ids in a closed vocabulary) and the
  version of the job they compose to; the node builds the job with `FragmentJob.ComposeFor` from the
  bricks its owner declared, compares the version, and refuses by name when a brick changed since the
  plan. It never runs text the coordinator sent: a job as written is refused unless the host
  registered a `FragmentGate` or runs any fragment (`AnyFragment`). What a brick runs stays the
  owner's, so naming one by id does not turn matching back into trusting a name. `FragmentPolicy`
  comes from configuration or `RunAnyFragment`, never from a field of a message. `FragmentIntake`
  decides it all before anything is spawned, for `Launch` and for `Check` alike, and the coordinator asks
  the same of the bricks a replica announces before it places a run there.
  `[local: Hosting.Tests FragmentJobTests, FragmentIntakeTests; the lab's smoke.py bricks, sandbox]`
- **What a host offers beyond its bricks is its own registration.** `WithFragmentGate` replaces the
  check a data node makes of a fragment (the host's word, like the policy), and `WithRequest`
  answers a named request of the coordinator; both are announced as capabilities, which the
  coordinator shows and never sets. No product code answers a query or a cut: the lab's host
  registers `lab.query` and `lab.split` itself, and the node says it does not answer any name it
  did not register. `[local: Hosting.Tests DtPipeNodeBuilderTests; the lab's smoke.py lab, sandbox]`
- **A runner runs dtpipe for the coordinator, a data node never does.** `DelegatedJobs` answers
  `ValidateJob` (a library save's job dry-run over one-row samples, writers neutralised) only on a
  host that runs any fragment; any other host says it is no runner. An alias names a file in the request's own directory, so it is a plain word.
  `[local: Hosting.Tests DelegatedJobsTests; the lab's smoke.py bricks, ui_check.py]`
- **A node reads its bricks for a library save's check or a studio's sample, never more.**
  `BrickHost.SampleAsync` reads a source brick it announced, with `output: arrow:-` and `--limit`,
  the bytes its fragment would send; a real bounded read, so `--dry-run`'s source-side guarantees do
  not apply. A source brick's preview is that sample (`PreviewSourceAsync`): its types come from
  `dtpipe inspect` on the sample's own bytes and its values from the same file, never from the
  schema announced at start, which `inspect` reads from the input and query alone. A sink's preview
  reads its table. It refuses by name a sample over `MaxSampleBytes` before
  sending, so an answer stays under the coordinator's channel message limit.
  `CheckSinkAsync` runs `dtpipe contract check` on a sink brick fed from `arrow:-`, which inspects
  the target and writes nothing. Checking a save is the product's, so neither is a registered request.
  `[local: Hosting.Tests BrickPreviewTests; the lab's smoke.py bricks — a library save's check, a brick's preview]`
- **A node announces its dtpipe's version**, what `dtpipe --version` says once at start
  (`DtPipeCommand.VersionAsync`), or none when the binary does not answer: a version is shown beside
  a studio's, never a reason to stop the host. `[local: Hosting.Tests DtPipeVersionTests; the lab's
  smoke.py bricks — versions]`
- **`BrickRules` (`DtPipe.Coordinator.Contracts`) over `JobYaml` (`DtPipe.Jobs`) is the one
  reading of a branch**, shared with the coordinator, so a studio never calls something a brick
  the node then declines. `BrickHost` does not announce a declared brick that fails the structure rule.
  `[unchecked: a declared brick that fails the structure rule]`
- **A variable's value never leaves the node.** The announcement names it, with an engine for a
  database, so the coordinator never holds a path or a secret to open. Variables reach a child through `FragmentLaunch.ChildEnvironment`
  and `DtPipeCommand` (the node's own reads), never through this process's environment: the host
  is someone else's service. `[unchecked]`
- **The token provider returns a fresh token.** TransportR retries a stream refused with 401
  through `NodeClientOptions.AccessTokenProvider`, so `NodeAgent` binds it to
  `OidcTokenClient.GetAsync(fresh: true)`, and asks once before connecting because TransportR
  swallows a provider that throws. The token endpoint comes from the provider's discovery document.
  `[unchecked: the fresh token on retry]`
- **A node the coordinator disconnects connects again.** SignalR's automatic reconnect does not
  cover a connection the server closes (a reconnect the coordinator asked for, an announcement it
  refused), so `hub.Closed` only signals and `ReconnectLoopAsync` starts the connection again and
  announces. Only the host's own stop ends a connect loop: an invocation cut by the coordinator's
  close also throws an `OperationCanceledException`. `[local: the lab's smoke.py rights — identity,
  disabled]`
- **A replica serves several runs, and answers each message as it arrives.** `ReplicaRuns` is the run
  protocol - the one table from the coordinator's `Launch`, `Wire`, `Cancel`, `Check` and `Kill` to a method,
  for the agent and for anything that stands in for one - over a table `(run, fragment) -> FragmentRun`. A
  handler returns as soon as it has the message and waits for neither a run nor a transfer, because the
  node channel's client takes messages one after another: a handler that waited would hold back the next
  message, a `Cancel` included. `[local: PipelineNode.Tests ReplicaRunsTests]`
- **A run ends in one place: it is reported, then thrown away.** `ReplicaRuns` runs a fragment to completion,
  sends `Exited`, and drops the run with its child and its job file. A report the coordinator does not know
  the connection of, or that cannot be delivered, is held and sent once after the next announcement it
  accepts, and a report in flight when one is accepted is sent again at once; `NotAdmitted` is final. The
  report carries the token of the launch that started the child (`LaunchRequest.LaunchToken`, kept with the
  run and copied into `FragmentExit`), a held one included, and the host announces
  `NodeAnnouncement.LaunchToken` for it: the coordinator admits no report without the token, and places
  no run on a replica that does not announce it.
  `[local: PipelineNode.Tests ReplicaRunsTests, CoordinatorDrivenTests, NodeAgentDataPlaneTests]` `[unchecked: the report in flight when an announcement lands]`
- **A run the coordinator never wires is abandoned.** `DtPipeNodeOptions.UnwiredRunTimeout` bounds the wait
  from `Launch` to the last `Wire` asked for, so a coordinator that vanished does not leave a child until the
  host stops. `[local: PipelineNode.Tests ReplicaRunsTests]`
- **An identity the coordinator does not know yet, or has disabled, announces without a data plane client.**
  Its client cannot register (it holds no group), and its announcement is how it becomes known, so `NodeAgent`
  connects the channel first and the client second, and announces without `DataPlaneClientId` when the
  client could not register: the replica is eligible to nothing. `WatchDataPlaneAsync` then connects the
  client as soon as it can and announces again, and registers again a client the hub has stopped knowing (a
  coordinator that lost it), which TransportR would otherwise do at its next presence heartbeat. One
  connection of the client at a time.
  `[local: the lab's smoke.py rights - enrollment, identity disabled, flow matrix run time]`
- **The coordinator closing the channel renews the data plane's registration, without waiting for the runs to
  end.** A client's groups are those it registered with, so once the coordinator has closed the channel `NodeAgent`
  calls `PipelineNode.RefreshRegistrationAsync` (the call's contract is in `src/DtPipe.PipelineNode/CLAUDE.md`)
  before it connects the channel again, runs in flight or not. A refusal is logged and never ends the loop that
  connects the channel. `[local: the lab's smoke.py rights - several groups; tools/campaign.py F6g]`
- **A host whose identity the hub refuses to renew drops its data plane client.** An identity that holds no group
  (disabled, or not known) cannot register, and a refused refresh writes nothing: the hub would keep the client's
  registration, with the groups it had, until the client's next token renewal withdraws them or its token expires. So
  when the refresh is refused with a `HubException`, `NodeAgent` closes the client and announces without a client id,
  and a new client, under a new id, registers as soon as the identity can (`EnsureNodeAsync`, from the loop that
  connects the channel or from `WatchDataPlaneAsync`). Only the client that was refused is dropped, under the lock that
  guards the client's creation. The runs in flight keep their `ReplicaRuns`, which refuses a `Launch` while it holds
  no client and fails at once a run asked to wire after its client was dropped; each run keeps the client it was
  launched with. This is the host's own doing: a host that kept its client would keep its registration. The
  coordinator's own checks are in `src/DtPipe.Coordinator/CLAUDE.md`, "Replicas and eligibility".
  `[local: PipelineNode.Tests NodeAgentDataPlaneTests, ReplicaRunsTests; the lab's smoke.py rights - identity, disabled]`
  `[unchecked: the drop raced by the loop and the watch; a Wire waiting for its handle when the client is dropped;
  the verdict of a run in flight]`
- **A node host works in a directory of its own process.** `DtPipeNodeOptions.StateDirectory` is a
  parent: the host makes `<parent>/<instance id>` for its fragment jobs and its dtpipe runs, so replicas
  that share a configuration never share a directory, and removes it when it stops in order. Its
  databases and variables are not there. The instance id is chosen once per process and sent with every
  `Announce`, with where the process runs. `[local: Hosting.Tests ProcessDirectoryTests — the path]`
  `[unchecked: the removal at an orderly stop]`
- **Stopping says so, then disposes every run before the host exits** (`NodeAgent.StopAsync`): `Leave` ends
  the replica's runs at the coordinator at once, with no grace period to wait out, and no dtpipe child
  outlives the host. `[unchecked]`
- Logs reach the host's own loggers and the coordinator (`NodeLogForwarder`, bounded and lossy: a
  slow coordinator never blocks a relay that logs). The coordinator gets Debug and up, TransportR
  and the framework from Warning.

`tests/DtPipe.PipelineNode.Hosting.Tests` covers the builder and the options; the lab's `smoke.py`
drives the agent against a real coordinator.
