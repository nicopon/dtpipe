# DtPipe.PipelineNode.Hosting

Mechanics for editing the node host. The rules themselves are in the root `CLAUDE.md` and
`.claude/rules/distributed-pipelines.md`.

`AddDtPipeNode` makes any .NET host a node: a business service that offers bricks, or the official
runner (`dtpipe node --runner`, which calls `RunAnyFragment` and offers no brick). It is a hosted service
(`NodeAgent`), not HTTP middleware: the node opens its connections to the coordinator, it serves
none. One node per process, which is one replica of its node's identity; a second `AddDtPipeNode` throws.

- **A data node composes its job from its own declarations, and the policy is the host's own.**
  The coordinator names bricks (`FragmentSpec.Units`: ids in a closed vocabulary) and the
  version of the job they compose to; the node builds the job with `FragmentJob.ComposeFor` from the
  bricks its owner declared, compares the version, and refuses by name when a brick changed since the
  plan. It never runs text the coordinator sent: a job as written is refused unless the host
  registered a `FragmentGate` or runs any fragment (`AnyFragment`), and a runner takes it only when every input and output
  is one of its `arrow:` edges (`RunnerEndpoints`, the rule the planner applies). What a brick runs stays the
  owner's, so naming one by id does not turn matching back into trusting a name. `FragmentPolicy`
  comes from configuration or `RunAnyFragment`, never from a field of a message. `FragmentIntake`
  decides it all before anything is spawned, for `Launch` and for `Check` alike, and the coordinator asks
  whether the versions a replica announces give the plan's version (`FragmentJob.VersionRefusal`) before it places a run there.
  `[CI: Hosting.Tests FragmentJobTests, FragmentIntakeTests] [local: the lab's smoke.py bricks, sandbox]`
- **What a host offers beyond its bricks is its own registration.** `WithFragmentGate` replaces the
  check a data node makes of a fragment (the host's word, like the policy); it is announced as a
  capability, which the coordinator shows and never sets.
  `[CI: Hosting.Tests DtPipeNodeBuilderTests] [local: the lab's smoke.py sandbox]`
- **A runner runs dtpipe for the coordinator, a data node never does.** `DelegatedJobs` answers
  `ValidateJob` (a library save's job dry-run over one-row samples, writers neutralised) only on a
  host that runs any fragment; any other host says it is no runner. An alias names a file in the request's own directory, so it is a plain word.
  A dry run reads real rows, so the job is held to `RunnerEndpoints` first (each sample an inbound edge, each sink an outbound one
  whose contract is saved under `JobCheck.ContractFile` and nowhere else), and the child runs under `--edges-only`, which admits a
  file of its working directory as a bound location or a `contract-save`.
  `[CI: Hosting.Tests DelegatedJobsTests; DtPipe.Tests EdgesOnlyRuleTests] [local: the lab's smoke.py bricks, ui_check.py]`
- **Ending a run's child is the host's own word.** `DtPipeNodeOptions.FaultInjection` is off by default
  (`DtPipeNode__FaultInjection` for a host configured by its environment); the coordinator's `Kill` ends
  the child of a run only on a host that enabled it. Any other host answers `0` and logs a warning that
  names the setting, with or without a run of the fragment in flight, and its handler stays bound.
  `[CI: PipelineNode.Tests FaultInjectionTests, Hosting.Tests FaultInjectionOptionTests] [local: the lab's tools/campaign.py F12, for the environment form]`
- **A node reads its bricks for a library save's check, a studio's sample or a binding's verdict, never more.**
  `BrickHost.SampleAsync` reads a source brick it announced, with `output: arrow:-` and `--limit`,
  the bytes its fragment would send; a real bounded read, so `--dry-run`'s source-side guarantees do
  not apply. A source brick's preview is that sample (`PreviewSourceAsync`): its types come from
  `dtpipe inspect` on the sample's own bytes and its values from the same file, never from a schema read
  from the input and query alone: a node announces none. A sink's preview
  reads its table. It refuses by name a sample over `MaxSampleBytes` before
  sending, so an answer stays under the coordinator's channel message limit.
  `CheckSinkAsync` runs `dtpipe contract check` on a sink brick fed from `arrow:-`, which inspects
  the target and writes nothing. The sample also saves the contract it produced, by the job key `contract-save` set to an absolute path
  in the run's own directory (the command-line flag is not read under `--job`; a relative path is read against the process's
  directory, which concurrent runs share). `CheckSourceAsync` is a binding's verdict on a source: that sample's contract against the
  pinned one, by hash for `contract`, by `dtpipe contract diff --json` for `compatible`, read from the JSON and never from the exit
  code (a command that fails exits as one that finds a break); a sample or a comparison that fails is `unread`, never `breaking`.
  `[CI: Hosting.Tests BrickPreviewTests, BrickCheckTests] [local: the lab's smoke.py bricks — a library save's check, a brick's preview]`
- **A node announces its dtpipe's version**, what `dtpipe --version` says once at start
  (`DtPipeCommand.VersionAsync`), or none when the binary does not answer: a version is shown beside
  a studio's, never a reason to stop the host. `[CI: Hosting.Tests DtPipeVersionTests] [local: the lab's
  smoke.py bricks — versions]`
- **A host runs the dtpipe it names, else the tool package its build placed, else the PATH's.** A path in `DtPipeNodeOptions.DtPipe`
  wins. Left at `dtpipe`, `DtPipeLocation` takes the `DtPipe.dll` of the `dtpipe` package that `build/DtPipe.PipelineNode.Hosting.targets`
  found for the host's `PackageDownload` (a tool package cannot be a `PackageReference`), written to the host's runtimeconfig as
  `DtPipe.ToolDirectory`, else a `dtpipe/` folder beside the host. A `.dll` has no executable, so `DtPipeProgram.Apply`
  (`DtPipe.PipelineNode`) starts it through the dotnet host of the running runtime, with the dll as the first argument, in
  `FragmentRun` and `DtPipeCommand` alike; `NodeAgent` resolves the path once for both. The child needs the ASP.NET Core shared
  framework on the machine. The `PackageDownload` line stays in the host's own project: a package's props are read only after the
  first restore, too late to add it. `[CI: Hosting.Tests DtPipeLocationTests; PipelineNode.Tests DtPipeProgramTests]`
  `[local: samples/arrow-socket-source/spike-node.sh]` `[unchecked: Windows]`
- **This library is the family's one package, and it carries the three projects under it.** The assemblies of `DtPipe.PipelineNode`,
  `DtPipe.Coordinator.Contracts` and `DtPipe.Jobs` ship in its `lib/` (`PrivateAssets="all"` on the references, `BundleProjectOutputs`
  in the project), so the packages those projects use are declared here too: a private reference carries none. The targets file ships
  under `build/` and under `buildTransitive/`, so a host that reaches the package through a class library gets it. Apache.Arrow is
  a `PackageReference`, never a reference to `Apache.Arrow.Serialization`. No solution packs the library, so the solution's pack never
  reaches it; `tests/scripts/validate_node_host_package.sh` packs it, outside every pack glob, and reads the result, and `release.yml`
  runs that script in a step of its own after the solution's push. A host built against a version not yet published takes a local one
  (`-p:PackageVersion`), since a restore serves the cached copy of a version it has. The package is a beta (`<Version>-beta.1`, set in
  the project; the release gives the tag's own suffix when it has one): the family is experimental.
  `[local: validate_node_host_package.sh, spike-node.sh] [CI: validate_distributed_boundary.sh — one packable project, bundled projects private]`
  `[unchecked: the release's step, until a tag runs it]`
- **A source of the host's own is served on a socket the host owns.** `AddArrowSource` declares a source brick that reads
  `arrow:pipe://${{DTPIPE_SRC_<ID>}}` and serves the rows of a callback on that socket (`ArrowSocketServer`); each connection runs the
  callback once. The path is a variable of the node, never part of the brick: the brick's text is its version, and the path differs by
  process. The socket listens from the server's constructor, so it exists before the node announces the brick. The schema is written
  first, before the callback runs, and every batch is held to it by its serialized form (the writer checks nothing; an int32 written
  as a float32 column would arrive as wrong values). The end marker is written only when the callback ends: a callback that throws,
  its own timeout's cancellation included, ends the stream without it, so the child fails and loads no part as a whole, and the host
  is never stopped by it. An I/O error of a write alone means the reader left (`--limit`, a sample, a failed run): leaving the loop
  disposes the callback's enumerator. The socket lives in a folder of the process that only the current user reaches, set before the
  socket exists. Registration refuses an id that is no word, two ids that name one variable, and a path longer than a Unix socket takes.
  `[CI: Hosting.Tests ArrowSourceTests] [local: samples/arrow-socket-source/spike-node.sh, probe]`
  `[unchecked: Windows; a reader that leaves while the callback waits, which the next write finds]`
- **`BrickRules` (`DtPipe.Coordinator.Contracts`) over `JobYaml` (`DtPipe.Jobs`) is the one
  reading of a branch**, shared with the coordinator, so a studio never calls something a brick
  the node then declines. `BrickHost` does not announce a declared brick that fails the structure rule.
  `[unchecked: a declared brick that fails the structure rule]`
- **A brick is announced by its version.** `BrickHost` announces each brick without its text and with
  `BrickRules.VersionOf(kind, text)`, the SHA-256 of its canonical text, so two replicas of a node that declare the same bricks announce
  the same versions. The text is the owner's: a secret written into it would let anyone who sees the version test guesses offline, so a
  brick holds `${{NAME}}` or `keyring://` references, never a value. `dtpipe node` refuses a brick that holds one; a host built on
  this library keeps the rule itself, since the library cannot read the connection-string grammar. `[CI: Hosting.Tests
  DtPipeNodeBuilderTests; Server.Tests BrickFormTests]` `[unchecked: a brick of a .NET host that holds a secret]`
- **A variable's value never leaves the node.** The announcement names it, with an engine for a
  database, so the coordinator never holds a path or a secret to open. Variables reach a child through `FragmentLaunch.ChildEnvironment`
  and `DtPipeCommand` (the node's own reads), never through this process's environment: the host
  is someone else's service. `[unchecked]`
- **The token provider returns a fresh token.** TransportR retries a stream refused with 401
  through `NodeClientOptions.AccessTokenProvider`, so `NodeAgent` binds it to
  `OidcTokenClient.GetAsync(fresh: true)`, and asks once before connecting because TransportR
  swallows a provider that throws. The provider is `Oidc:Authority` or, left out, the first the coordinator's protected resource metadata names (never an HTTP provider for an HTTPS coordinator): its key issuer when it trusts no other; the client is `Oidc:ClientId` or the node's `Name`; the token endpoint comes from the provider's discovery document.
  A provider that answers and refuses the client is a `TokenRefusedException`: the connect loop logs it as a refusal that names the
  provider and the client to add or enable, and only a coordinator that does not answer reads as not reachable.
  `[CI: Hosting.Tests KeyTokenTests]` `[unchecked: the fresh token on retry; the refusal's text]`
- **A secret is the host's value or a file's, read once.** `Oidc:ClientSecret` is the secret, or `@path` to the file that holds it
  (a coordinator's key mounted in a container), read once when `OidcTokenClient` is made (`SecretReference`, the one place): a file
  that cannot be read, or is empty, stops the start by the setting's name, never by its path or its content. A secret that begins
  with `@` is given in a file. `dtpipe node` resolves its own references (`src/DtPipe/CLAUDE.md`) and hands the value over, so
  such a secret cannot pass through it (a coordinator's key never begins with `@`).
  `[CI: Hosting.Tests KeyTokenTests]`
- **A node the coordinator disconnects connects again.** SignalR's automatic reconnect does not
  cover a connection the server closes (a reconnect the coordinator asked for, an announcement it
  refused), so `hub.Closed` only signals and `ReconnectLoopAsync` starts the connection again and
  announces. Only the host's own stop ends a connect loop: an invocation cut by the coordinator's
  close also throws an `OperationCanceledException`. The loop waits between tries (`NodeAgent.RetryDelay`): 1 s,
  doubling to 15 s, so a coordinator that does not answer, or refuses the announcement, is not asked once a second.
  `[CI: Hosting.Tests ConnectRetryTests, PipelineNode.Tests ConnectBackoffTests] [local: the lab's smoke.py rights — identity,
  disabled]`
- **A replica serves several runs, and answers each message as it arrives.** `ReplicaRuns` is the run
  protocol - the one table from the coordinator's `Launch`, `Wire`, `Cancel`, `Check` and `Kill` to a method,
  for the agent and for anything that stands in for one - over a table `(run, fragment) -> FragmentRun`. A
  handler returns as soon as it has the message and waits for neither a run nor a transfer, because the
  node channel's client takes messages one after another: a handler that waited would hold back the next
  message, a `Cancel` included. `[CI: PipelineNode.Tests ReplicaRunsTests]`
- **A sink branch's cursor is read from a file of the node's own.** For a fragment composed from bricks, `FragmentCursors.Arm` adds a
  `state` path (absolute, per run, fragment and alias, in the host's job directory) to every branch that sets `cursor`; the child writes
  the file it always writes, and `ReplicaRuns` reads it once the child has exited with 0 and sends the value in `FragmentExit.Cursors`,
  after `CursorValues` has judged it. A `LaunchRequest.Cursor` is judged by the same rule before the child starts and reaches it as
  `DTPIPE_CURSOR_OVERRIDE`; a refused value is a refused launch. The files are deleted with the run. A fragment written as a job (a
  runner's) is never armed. `[CI: Hosting.Tests FragmentCursorsTests]`
- **A run ends in one place: it is reported, then thrown away.** `ReplicaRuns` runs a fragment to completion,
  sends `Exited`, and drops the run with its child and its job file. A report the coordinator does not know
  the connection of, or that cannot be delivered, is held and sent once after the next announcement it
  accepts, and a report in flight when one is accepted is sent again at once; `NotAdmitted` is final. The
  report carries the token of the launch that started the child (`LaunchRequest.LaunchToken`, kept with the
  run and copied into `FragmentExit`), a held one included: the coordinator admits no report
  without it.
  `[CI: PipelineNode.Tests ReplicaRunsTests, CoordinatorDrivenTests, NodeAgentDataPlaneTests]` `[unchecked: the report in flight when an announcement lands]`
- **A host holds at most `DtPipeNodeOptions.MaxRuns` runs** (0, the default, for no bound): `ReplicaRuns` admits a launch under a
  lock before its child starts, refuses one beyond the bound by name, and gives the place back when the launch fails or the run
  leaves its table. `Check` never counts, since a deployment is armed long before its runs. The product's runner sets it: the steps
  of every flow share its host. `[CI: PipelineNode.Tests ReplicaRunsTests]`
- **A run the coordinator never wires is abandoned.** `DtPipeNodeOptions.UnwiredRunTimeout` bounds the wait
  from `Launch` to the last `Wire` asked for, so a coordinator that vanished does not leave a child until the
  host stops. `[CI: PipelineNode.Tests ReplicaRunsTests]`
- **A host asks for its data plane client only once the coordinator has accepted its announcement.**
  `NodeAgent` connects the channel and announces without `DataPlaneClientId`; when that announcement is
  accepted it connects the client and announces again with the client's id, and a replica announced without
  one is eligible to nothing. The coordinator accepts only an identity it knows and has enabled (any other
  has its connection closed), so a host refused at the announcement asks the IdP for no new token and
  connects no client, however often it is refused. `WatchDataPlaneAsync` acts only while an announcement
  stands: it connects the client as soon as it can and announces again, and registers again a client the hub
  has stopped knowing (a coordinator that lost it), which TransportR would otherwise do at its next presence
  heartbeat. One connection of the client at a time.
  `[CI: PipelineNode.Tests DataPlaneAfterAnnouncementTests, NodeAgentDataPlaneTests] [local: the lab's smoke.py rights - enrollment, identity disabled, flow matrix run time]`
  `[unchecked: a close that lands as an announcement is accepted]`
- **The coordinator closing the channel renews the data plane's registration, without waiting for the runs to
  end.** A client's groups are those it registered with, so once the coordinator has closed the channel `NodeAgent`
  calls `PipelineNode.RefreshRegistrationAsync` (the call's contract is in `src/DtPipe.PipelineNode/CLAUDE.md`)
  before it connects the channel again, runs in flight or not. A refusal is logged and never ends the loop that
  connects the channel. `[local: the lab's smoke.py rights - several groups; tools/campaign.py F6g]`
- **A host whose identity the hub refuses to renew drops its data plane client.** An identity that holds no group
  (disabled, or not known) cannot register, and a refused refresh writes nothing: the hub would keep the client's
  registration, with the groups it had, until the client's next token renewal withdraws them or its token expires. So
  when the refresh is refused with a `HubRequestFailedException`, `NodeAgent` closes the client and announces without a client id,
  and a new client, under a new id, registers as soon as the identity can (`EnsureNodeAsync`, from the loop that
  connects the channel or from `WatchDataPlaneAsync`). Only the client that was refused is dropped, under the lock that
  guards the client's creation. The runs in flight keep their `ReplicaRuns`, which refuses a `Launch` while it holds
  no client and fails at once a run asked to wire after its client was dropped; each run keeps the client it was
  launched with. This is the host's own doing: a host that kept its client would keep its registration. The
  coordinator's own checks are in `src/DtPipe.Coordinator/CLAUDE.md`, "Replicas and eligibility".
  `[CI: PipelineNode.Tests NodeAgentDataPlaneTests, ReplicaRunsTests] [local: the lab's smoke.py rights - identity, disabled]`
  `[unchecked: the drop raced by the loop and the watch; a Wire waiting for its handle when the client is dropped;
  the verdict of a run in flight]`
- **A node host works in a directory of its own process.** `DtPipeNodeOptions.StateDirectory` is a
  parent: the host makes `<parent>/<instance id>` for its fragment jobs and its dtpipe runs, so replicas
  that share a configuration never share a directory, and removes it when it stops in order. Each run of a fragment works in
  `<that>/fragments/<run>-<fragment>/`, its child's own (`FragmentLaunch.RunDirectory`), and a runner's child runs there under
  `--edges-only`. Its
  databases and variables are not there. The instance id is chosen once per process and sent with every
  `Announce`, with where the process runs. `[CI: Hosting.Tests ProcessDirectoryTests — the path]`
  `[local: samples/distributed-lab smoke.py replicas — the removal at an orderly stop]`
- **Stopping says so, then disposes every run before the host exits** (`NodeAgent.StopAsync`): `Leave` ends
  the replica's runs at the coordinator at once, with no grace period to wait out, and no dtpipe child
  outlives the host. `[local: samples/distributed-lab smoke.py replicas — Leave of an idle replica]` `[unchecked: runs in flight at an orderly stop, children that outlive the host]`
- Logs reach the host's own loggers and the coordinator (`NodeLogForwarder`, bounded and lossy: a
  slow coordinator never blocks a relay that logs). The coordinator gets Debug and up, TransportR
  and the framework from Warning.

`tests/DtPipe.PipelineNode.Hosting.Tests` covers the builder and the options; the lab's `smoke.py`
drives the agent against a real coordinator.
