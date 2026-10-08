# DtPipe.PipelineNode.Hosting

Mechanics for editing the node host. The rules themselves are in the root `CLAUDE.md`.

`AddDtPipeNode` makes any .NET host a node: a business service that offers bricks, or the official
runner (`DtPipe.PipelineNode.Runner`, which only calls `RunAnyFragment`). It is a hosted service
(`NodeAgent`), not HTTP middleware: the node opens its connections to the coordinator, it serves
none. One node per process; a second `AddDtPipeNode` throws.

- **A data node composes its job from its own declarations, and the policy is the host's own.**
  The coordinator names bricks (`FragmentDeployment.Units`: ids in a closed vocabulary) and the
  version of the job they compose to; the node builds the job with `FragmentJob.Compose` from the
  bricks its owner declared, compares the version, and refuses by name when a brick changed since the
  plan. It never runs text the coordinator sent: a job as written is refused unless the host
  registered a `FragmentGate` or runs any fragment (`AnyFragment`). What a brick runs stays the
  owner's, so naming one by id does not turn matching back into trusting a name. `FragmentPolicy`
  comes from configuration or `RunAnyFragment`, never from a field of a message. `FragmentIntake`
  decides it all before any `PipelineNode` registers (a registered instance is admissible).
  `[local: Hosting.Tests FragmentJobTests, FragmentIntakeTests; the lab's smoke.py bricks, sandbox]`
- **What a host offers beyond its bricks is its own registration.** `WithFragmentGate` replaces the
  check a data node makes of a fragment (the host's word, like the policy), and `WithRequest`
  answers a named request of the coordinator; both are announced as capabilities, which the
  coordinator shows and never sets. No product code answers a query or a cut: the lab's host
  registers `lab.query` and `lab.split` itself, and the node says it does not answer any name it
  did not register. `[local: Hosting.Tests DtPipeNodeBuilderTests; the lab's smoke.py lab, sandbox]`
- **A runner runs dtpipe for the coordinator, a data node never does.** `DelegatedJobs` answers
  `ValidateJob` (a designer's job dry-run over one-row samples, writers neutralised) and `ExportJob`
  (a command line as a job) only on a host that runs any fragment; any other host says it is no
  runner. An alias names a file in the request's own directory, so it is a plain word.
  `[local: Hosting.Tests DelegatedJobsTests; the lab's smoke.py bricks, ui_check.py]`
- **A node reads its bricks for a designer's check or a studio's sample, never more.**
  `BrickHost.SampleAsync` reads a source brick it announced, with `output: arrow:-` and `--limit`,
  the bytes its fragment would send; a real bounded read like a preview, so `--dry-run`'s
  source-side guarantees do not apply. It refuses by name a sample over `MaxSampleBytes` before
  sending, so an answer stays under the coordinator's channel message limit.
  `CheckSinkAsync` runs `dtpipe contract check` on a sink brick fed from `arrow:-`, which inspects
  the target and writes nothing. Checking a design is the product's, so neither is a registered request.
  `[local: the lab's smoke.py bricks — a designer's check]`
- **A node announces its dtpipe's version**, what `dtpipe --version` says once at start
  (`DtPipeCommand.VersionAsync`), or none when the binary does not answer: a version is shown beside
  a studio's, never a reason to stop the host. `[local: Hosting.Tests DtPipeVersionTests; the lab's
  smoke.py bricks — versions]`
- **`BrickRules` (`DtPipe.Coordinator.Contracts`) over `JobYaml` (`DtPipe.Jobs`) is the one
  reading of a branch**, shared with the coordinator, so the designer never calls something a brick
  the node then declines. `BrickHost` does not announce a declared brick that fails the structure rule.
  `[unchecked: a declared brick that fails the structure rule]`
- **A variable's value never leaves the node.** The announcement names it, with an engine for a
  database, so the coordinator never holds a path or a secret to open. Variables reach a child through `PipelineNodeOptions.ChildEnvironment`
  and `DtPipeCommand` (the node's own reads), never through this process's environment: the host
  is someone else's service. `[unchecked]`
- **The token provider returns a fresh token.** TransportR retries a stream refused with 401
  through `PipelineNodeOptions.AccessTokenProvider`, so `FragmentSupervisor` binds it to
  `OidcTokenClient.GetAsync(fresh: true)`, and asks once before connecting because TransportR
  swallows a provider that throws. The token endpoint comes from the provider's discovery document.
  `[unchecked: the fresh token on retry]`
- **A node the coordinator disconnects connects again.** SignalR's automatic reconnect does not
  cover a connection the server closes (a reconnect the coordinator asked for, an announcement it
  refused), so `hub.Closed` only signals and `ReconnectLoopAsync` starts the connection again and
  announces. Only the host's own stop ends a connect loop: an invocation cut by the coordinator's
  close also throws an `OperationCanceledException`. `[local: the lab's smoke.py rights — identity,
  disabled]`
- **A `PipelineNode` serves one run**: `FragmentSupervisor.RearmAsync` replaces it with a fresh
  instance under the new generation; every `FragmentStatus` echoes the generation of the command it
  answers.
- **Stopping disposes every fragment before the host exits** (`NodeAgent.StopAsync`), and each
  `PipelineNode` unregisters before it disconnects, so no dtpipe child outlives the host and no
  closed instance stays admissible.
- Logs reach the host's own loggers and the coordinator (`NodeLogForwarder`, bounded and lossy: a
  slow coordinator never blocks a relay that logs). The coordinator gets Debug and up, TransportR
  and the framework from Warning.

`tests/DtPipe.PipelineNode.Hosting.Tests` covers the builder and the options; the lab's `smoke.py`
drives the agent against a real coordinator.
