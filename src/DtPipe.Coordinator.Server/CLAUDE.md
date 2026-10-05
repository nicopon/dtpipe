# DtPipe.Coordinator.Server

Mechanics for editing the coordinator's server. The rules themselves are in the root `CLAUDE.md`;
the hub, the barrier and admission are `DtPipe.Coordinator`'s (`src/DtPipe.Coordinator/CLAUDE.md`).

The server hosts `DtPipe.Coordinator` (`AddCoordinatorHub`) and adds what a person needs around it:
the node channel (`NodeChannel`, `NodeChannelHub`), bricks, the designer, the git-backed library,
automatic distribution, deployments and the run queue, rights, and the pages. Its settings are the
`Coordinator` configuration section (`CoordinatorServerOptions`), from the environment first.

- **The server trusts an IdP for who, and decides what.** `TrustedIdp` validates the tokens of
  `Coordinator:Authority` for `Coordinator:Audience`; a token names its client (`sub`) and grants
  nothing. `RightsIdentityProvider` replaces TransportR's `IClientIdentityProvider` and gives the
  subject its identity's group in `RightsStore`, none for an unknown or disabled one. TransportR
  records that group at `Connect` and at each token renewal, so an identity edit applies when the
  node connects again. `NodeChannelHub.Announce` refuses a host announcing a node its identity is
  not bound to, or a disabled one. `[local: the lab's smoke.py rights]`
- **A person's rights are the server's configuration, never the token's.** People sign in through
  the provider (`Coordinator:Ui:ClientId`, a confidential authorization-code client, PKCE, the code
  in the query so the Lax correlation cookies ride the redirect) into the `dtpipe-coordinator`
  cookie. `CoordinatorRoles` (an `IClaimsTransformation`) reads `Coordinator:Roles:Admin|Designer|
  Reader` (lists of the claim `Coordinator:Roles:Claim`, `email` by default) on every request:
  cumulative roles, none for anyone unlisted (403, never reader by default). `/api` and the pages
  accept the cookie only; the default policy, which the node channel and TransportR's hub and
  streams use, accepts a service's bearer token only, so a person's session never reaches the
  data plane and a service's token never reaches `/api`. Only a page navigation
  (`TrustedIdp.IsPageNavigation`) is sent to the provider; any other request is told 401.
  `[local: the lab's smoke.py rights — roles and sessions]`
- **A request that changes something carries `X-Dtpipe-Csrf`** (`RequireCsrfHeader`, on every
  `/api` group): another site cannot set it without a CORS preflight this server never answers.
  The pages' `api()` helpers send it. Never add a cookie-authenticated route outside those groups.
  `[local: the lab's smoke.py rights]`
- **Every rights change names its author** (`AuditEntry.By`, the signed-in email). The session
  cookie's keys live in `keys/` in the state directory, so a restart signs nobody out.
- **Lab mode is off by default, and everything only a bench needs sits behind it.**
  `LabModeEndpoints` maps the Lab view's scenario catalog and manual planning (`/api/plan`,
  `/api/deploy`), fault injection and `/api/query`, for admins; the pages under `wwwroot/lab/`
  answer 404 without it (and 403 to anyone but an admin), and `GET /api/info` tells the pages
  which mode they run in. Never add a bench-only
  route outside `MapLabMode`. `[unchecked]`
- **The server reads sources itself in three places**: `split` (planning a cut), `--dry-run`
  (`/design/validate`) and `/api/query`. `DtPipeCli` gives dtpipe every dataset path a node
  announced, so all three work only where the server shares the nodes' file system. A node that
  announces a variable without an engine keeps its path to itself. `[unchecked]`
- **A new state directory starts from the seeds, or empty.** `Coordinator:LibrarySeed` (one
  `.yaml` per pipeline) and `Coordinator:RightsSeed`; with neither, the library has no pipeline
  and the rights no identity and an empty matrix: nobody may send anything until an administrator
  says so. The library is a git repository of its own under `library/` in the state directory,
  driven by the `git` command line with a fixed identity (the image needs `git`); run history lives
  in `runs/`, outside git; deployments are in memory.
- **Cuts inside a branch belong to `dtpipe split`.** `PlanBuilder` only regroups whole branches
  and adds `arrow:-` endpoints or relay branches; it never edits an option or moves a transformer.
  `AutoPlacer` only chooses a placement for `PlanBuilder` and cuts nothing. `[local: the lab's smoke.py]`
- **A brick is matched by content, never by name.** A branch is a brick when its canonical form
  (`JobYaml.Canonical`) equals the one its node declares; a pipeline stays a plain dtpipe job with
  no reference to a brick in it. `DesignService.Decompose` may take an imported branch apart
  around the bricks it contains (authoring, before anything is saved); the planner never does.
  `[local: the lab's smoke.py, ui_check.py]`
- **A fragment declares each edge alias once**, so a branch that receives an edge under its own
  alias (a cut's tail) sends on through a relay, never with `output: arrow:-` on itself.
  `[local: the lab's smoke.py lab]`
- **The server writes the job, the page never does.** `DesignService.Compose` is the only writer
  of a designer pipeline's YAML; the page sends cards and reads back the text. `[unchecked]`
- **A `PipelineNode` serves one run.** `DeploymentManager` re-arms every fragment of a pipeline
  after its run, and holds that deployment's gate from before it reports the run finished until
  the fragments are registered again, so no run starts on a spent instance. Runs of every
  pipeline wait in one queue, since `RunOrchestrator` refuses a second one in flight.
  `[local: the lab's smoke.py]`
- **Ready means the main instance is registered.** A node unregisters from the coordinator before
  it disconnects, so a closed instance is never left for admission to pick.
  `WaitRegisteredAsync` requires the main instance in the deployment's current generation, held by
  the registry at the fragment's version. Every `Deploy` and `Rearm` carries a new generation,
  echoed in `FragmentStatus`. `[local: the lab's smoke.py]`
- **A disconnected host is forgotten fragment by fragment.** `NodeInventory.Disconnected` drops what
  the host last reported: a killed host would otherwise leave its fragments "Launched" for good. A host
  that only lost its connection reports them all again when it announces itself.
  `[local: the lab's tools/campaign.py F3]`
- **One source of rights.** `RightsStore` holds the identities, the flow matrix and the brick
  policies; `RightsFlowControl` replaces TransportR's `IFlowControlService`, so the hub checks the
  matrix as it is now on every transfer, and every plan (`/api/plan`, `/api/deploy`, `AutoPlacer`,
  a library deploy) goes through `RightsStore.WithPolicies`. Never add a second matrix.
  `[local: the lab's smoke.py rights]`
- **A data node only reads and writes, and runs only its bricks**: the node enforces it
  (`src/DtPipe.PipelineNode.Hosting/CLAUDE.md`), and the server reads a branch the same way
  (`BrickRules`, `DtPipe.Coordinator.Contracts`) so the designer never places a brick its node then
  declines. `BrickRules.Processors` is the one list of processors, shared with `JobGraph`.
  `DeploymentManager.WaitRegisteredAsync` fails on the first `Failed` main instance of the current
  generation and `DeployAsync` then undeploys the plan's fragments. `[local: the lab's smoke.py
  bricks, sandbox]`
- The pages are served from the `wwwroot` published beside the binary (`dotnet publish`): `index.html`
  + `js/` (the coordinator's views, ES modules, no build) and `lab/` (the Lab view, lab mode only).
  `window.lab.store` exists for the lab's `tools/ui_check.py`. `/healthz` is the only anonymous
  route. Verify a change with the lab's `./lab.sh smoke`, and a page change with
  `./lab.sh ui` too.
