# DtPipe.PipelineNode.Hosting

Mechanics for editing the node host. The rules themselves are in the root `CLAUDE.md`.

`AddDtPipeNode` makes any .NET host a node: a business service that offers bricks, or the official
runner (`DtPipe.PipelineNode.Runner`, which only calls `RunAnyFragment`). It is a hosted service
(`NodeAgent`), not HTTP middleware: the node opens its connections to the coordinator, it serves
none. One node per process; a second `AddDtPipeNode` throws.

- **The policy is the host's own.** `FragmentPolicy` comes from configuration or `RunAnyFragment`,
  never from a field of `FragmentDeployment` or any other message: a brick id or a skip flag sent
  by the coordinator would turn matching back into trusting a name. `NodeAgent.DeployAsync` reads
  every branch through `BrickRules.FragmentRefusal` before any `PipelineNode` registers (a
  registered instance is admissible), except under `AnyFragment`. `[local: the lab's smoke.py
  bricks, sandbox]`
- **Lab requests are the host's to answer.** The coordinator's lab mode asks a node to read its own
  datasets (`LabRequests`: a query, and `dtpipe split` of a branch that reads them, whose sample
  run executes the branch's transformers on the node); the node answers only when its own
  `DtPipeNodeOptions.LabMode` is on, off by default, and queries only a variable given an engine. `[local: Hosting.Tests
  LabRequestsTests; the lab's smoke.py, whose checks read the warehouse this way]`
- **`BrickRules` and `JobYaml` (`DtPipe.Coordinator.Contracts`) are the one reading of a branch**,
  shared with the coordinator, so the designer never calls something a brick the node then
  declines. `BrickHost` does not announce a declared brick that fails the structure rule.
  `[unchecked: a declared brick that fails the structure rule]`
- **A variable's value never leaves the node**, except a dataset given an engine, whose value the
  coordinator opens itself. Variables reach a child through `PipelineNodeOptions.ChildEnvironment`
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
