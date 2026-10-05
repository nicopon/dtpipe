# samples/distributed-lab

Mechanics for editing the lab. What it is and how to run it: `README.md`.

- **The lab uses dtpipe, it never changes it.** Nothing in `DtPipe.sln` moves for the lab. It
  reaches the engine through the binary (`split`, `--export-job`, `--job`, `inspect`, `--dry-run`)
  and hosts the distributed family (`src/DtPipe.Coordinator*`, `src/DtPipe.PipelineNode*`) as a
  consumer: product code a lab need may shape, never code written for the lab alone. A need of the
  engine is reported to the user, not patched in. `[unchecked]`
- **Lab nodes are disposable hosts of `DtPipe.PipelineNode.Hosting`.** `Lab.NodeHost` reads
  `nodes/<node>.json` into `AddDtPipeNode`; a node file whose `role` is `runner` starts the official
  `DtPipe.PipelineNode.Runner` instead, configured through its environment. `lab.sh exec-node` is
  the one way a node starts, for `lab.sh` and `tools/campaign.py`. Node mechanics:
  `src/DtPipe.PipelineNode.Hosting/CLAUDE.md`. `[local: smoke.py]`
- **Cuts inside a branch belong to `dtpipe split`.** `PlanBuilder` only regroups whole branches
  and adds `arrow:-` endpoints or relay branches; it never edits an option or moves a transformer.
  `AutoPlacer` only chooses a placement for `PlanBuilder` and cuts nothing. `[local: smoke.py]`
- **A brick is matched by content, never by name.** A branch is a brick when its canonical form
  (`JobYaml.Canonical`) equals the one its node declares; a pipeline stays a plain dtpipe job with
  no reference to a brick in it. `DesignService.Decompose` may take an imported branch apart
  around the bricks it contains (authoring, before anything is saved); the planner never does.
  `[local: smoke.py, ui_check.py]`
- **A data node only reads and writes, and runs only its bricks.** `NodeAgent.DeployAsync` reads
  every branch through `BrickRules` (`DtPipe.Coordinator.Contracts`, the reading `BrickCatalog.Match` uses)
  before any `PipelineNode` registers, and refuses the fragment with a `Failed` status in the
  deployment's generation. Structure, always: a branch carries only `input`, `output`, `from`,
  `provider-options` and the volume settings `dtpipe split` writes, no processor, and reads at most
  one branch; a key not on that list is refused, so a key dtpipe adds later waits for a decision.
  Bricks, unless the host runs in sandbox mode: the branch is also one of the node's own bricks, a
  bare `input: arrow:-`, or a `from` + `output: arrow:-` relay of a branch of the fragment. A source
  brick sent straight to another node gains `output: arrow:-`, which `FormOf` sets aside. Sandbox
  (`LAB_NODE_SANDBOX=1` in the host's environment, which `Lab.NodeHost` maps to
  `FragmentPolicy.AnyReadOrWrite`; the Lab view's mode) keeps only the structure: any reader or
  writer, whatever it reads or writes. The runner hosts no data and runs any fragment; what contains
  it is network isolation, not this rule. The switch is the host's own, never a field of anything
  the coordinator sends; only `1` or `true` turns it on. `BrickRules.Processors`
  is the one list of processors, shared with `JobGraph`. `DeploymentManager.WaitRegisteredAsync`
  fails on the first `Failed` main instance of the current generation and `DeployAsync` then
  undeploys the plan's fragments. `BrickHost` does not announce a declared brick that would fail
  the structure rule itself, so the designer never places one the node then declines. `lab.sh smoke`
  restarts the hosts strict or sandbox per pass. `[local: smoke.py bricks, smoke.py sandbox]`
  `[unchecked: a declared brick that fails the structure rule]`
- **A fragment declares each edge alias once**, so a branch that receives an edge under its own
  alias (a cut's tail) sends on through a relay, never with `output: arrow:-` on itself.
  `[local: smoke.py lab]`
- **The server writes the job, the page never does.** `DesignService.Compose` is the only writer
  of a designer pipeline's YAML; the page sends cards and reads back the text. `[unchecked]`
- **A `PipelineNode` serves one run.** `DeploymentManager` re-arms every fragment of a pipeline
  after its run, and holds that deployment's gate from before it reports the run finished until
  the fragments are registered again, so no run starts on a spent instance. Runs of every
  pipeline wait in one queue, since `RunOrchestrator` refuses a second one in flight.
  `[local: smoke.py]`
- **Ready means the main instance is registered.** A node unregisters from the coordinator before
  it disconnects, so a closed instance is never left for admission to pick.
  `WaitRegisteredAsync` requires the main instance in the deployment's current generation, held by
  the registry at the fragment's version. Every `Deploy` and `Rearm` carries a new generation,
  echoed in `FragmentStatus`. `[local: smoke.py]`
- **A node is who its token says; its group is what the coordinator says.** Every client of the
  coordinator authenticates with `Lab.Idp` (client credentials), which the coordinator trusts for its
  audience (`TrustedIdp`). The token names the client (`sub`) and grants nothing:
  `RightsIdentityProvider` replaces TransportR's `IClientIdentityProvider` and gives the subject its
  identity's group, none for an unknown or disabled one. TransportR records that group at `Connect`
  and at each renewal, so an identity edit applies when the node connects again. `LabHub.Announce`
  refuses a host announcing a node its identity is not bound to, or a disabled one.
  `[local: smoke.py rights]`
- **The IdP's signing key survives an IdP restart.** A node host reuses its cached token until
  shortly before it expires, so `Lab.Idp` reads its key from `.state/idp/signing-key.pem` (generated
  once, readable by its owner only, outside git). `lab.sh idp --Idp:AccessTokenLifetimeSeconds=N`
  restarts it with shorter tokens for trials. `[local: tools/campaign.py S3, S5]`
- **`Lab.Idp` says who, never what.** Its tokens carry a subject, a name and an email; no group,
  role or permission enters a token or the clients file, because what a caller may do belongs to
  the coordinator. It stands alone: no reference to dtpipe, TransportR or the coordinator.
  `[local: tools/idp_check.py]`
- **A disconnected host is forgotten fragment by fragment.** `NodeInventory.Disconnected` drops what
  the host last reported: a killed host would otherwise leave its fragments "Launched" for good. A host
  that only lost its connection reports them all again when it announces itself.
  `[local: tools/campaign.py F3]`
- **One source of rights.** `RightsStore` holds the identities, the flow matrix and the brick
  policies; `RightsFlowControl` replaces TransportR's `IFlowControlService`, so the hub checks the
  matrix as it is now on every transfer, and every plan (`/api/plan`, `/api/deploy`, `AutoPlacer`,
  a library deploy) goes through `RightsStore.WithPolicies`. Never add a second matrix.
  `[local: smoke.py rights]`
- A node's tokens come from the IdP its `Oidc:Authority` names, through the discovery document:
  `lab.sh` starts `Lab.Idp` first and points the coordinator (`--authority`) and every node at it.
  The secrets live in `idp-clients.json` (the IdP) and the node files (the clients), never in the
  rights document. `[local: smoke.py rights]`
- The library is a git repository of its own under `.state/library`, driven by the `git` command
  line with a fixed identity; it never touches the dtpipe repository around it. It starts from
  `library-seed/`. Run history lives in `.state/runs/`, outside git. Deployments are in memory.
- The pages are served from `Lab.Coordinator/wwwroot` in the sources, not from the build output:
  `index.html` + `js/` (the coordinator's views, ES modules, no build) and `lab.html` + `app.js`
  (the Lab view). `window.lab.store` exists for `tools/ui_check.py`. The page and `/api` are not
  authenticated: the lab has one operator.
- `tools/campaign.py` plays a fixed matrix of faults (a killed child or host, a cancellation, a
  coordinator restart, network faults through toxiproxy, revoked rights, rapid cycles) against the
  running lab and judges every trial on the same invariants: it ends, the verdict is right, nothing
  is left running, the lab still runs a reference pipeline. It needs the lab seeded with
  `LAB_SCALE=60` (shorter runs end before a timed fault reaches them), strict nodes, and the
  toxiproxy binaries (`TOXIPROXY_DIR`) for the network cells. Its security cells call the hub as a
  peer would (`campaign_security.py`, a SignalR client over long polling) and start the coordinator
  with shorter tokens through `lab.sh idp`. It records, it never fixes. It starts only on a bench it can judge
  (`campaign_bench.py`): only the lab's own processes, a machine with no build, test run or TransportR
  process, and the binary the hosts run starts and reports the full hash of the frozen commit with a
  clean tracked tree. Every call retried on `EADDRINUSE`/`EADDRNOTAVAIL` and every port-exhaustion line
  in the lab's logs is counted into the trial's `I7`, never absorbed. `[local: tools/campaign.py with
  CAMPAIGN_PREFLIGHT=report prints the checks without refusing]`
- Verify a change with `./lab.sh smoke`, and a page change with `./lab.sh ui` too: it drives
  headless Chrome over the DevTools protocol (`tools/cdp.py`), because Chrome's own `--screenshot`
  stops the page before its fetches finish. `tools/cdp.py` also clicks and drags with the mouse.
