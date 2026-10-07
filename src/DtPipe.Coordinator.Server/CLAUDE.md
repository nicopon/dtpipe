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
  subject its identity's groups in `RightsStore`, none for an unknown or disabled one. TransportR
  records those groups at `Connect` and at each token renewal, so an identity edit applies when the
  node connects again. The node name belongs to the rights, never to the host: `NodeChannelHub`
  files a known host under its identity's name whatever it proposes, and everything it reports
  under the name its connection announced. A token no identity knows is no node: its host waits in
  `EnrollmentQueue` (bounded, expired, never evicting a waiting subject), and what it proposes is
  untrusted text, cut here and escaped by the page. A disabled identity is refused. A node whose
  identity is gone or renamed leaves the inventory with its host.
  `[local: DtPipe.Coordinator.Server.Tests, the lab's smoke.py rights]`
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
- **A refused page gives a way out.** A person with no role who opens a page is signed out here
  and at the provider and lands on `/account/denied`: keeping the provider's session would sign
  the same person in again on the next try. A person who lacks only the admin role gets a 403 page
  linking back to `/` and to `/account/logout`. `AccessPage` writes both; a message it shows is
  HTML-encoded first. `[local: the lab's smoke.py rights — roles and sessions]`
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
- **The server never opens a node's data.** What reads it asks the node that hosts it, through
  `NodeRequests`: a brick preview, `/api/query` and a cut (the named requests `lab.query` and `lab.split`, which only a lab host registers), and a
  designer's check (`DesignValidator`): each source brick's node sends its first row as Arrow IPC
  (`SampleBrick`, written by the `arrow:-` writer its fragment uses), a runner dry-runs the job as
  the runner will see it (`ValidateJob`), and each sink brick's node checks its target against the contract that
  reaches it (`CheckSink`). A branch that reaches a node's data without being its brick is refused
  before anything runs. **The server runs no dtpipe**: a designer's check and the import of a
  command line (`ExportJob`) go to a runner, and with none online they refuse. A node announces a
  variable's name, never its value. `[local: the lab's smoke.py bricks — a designer's check, a sink that refuses;
  sandbox — a cut on its node]`
- **A new state directory starts from the seeds, or empty.** `Coordinator:LibrarySeed` (one
  `.yaml` per pipeline) and `Coordinator:RightsSeed`; with neither, the library has no pipeline
  and the rights no identity and an empty matrix: nobody may send anything until an administrator
  says so. The library is a git repository of its own under `library/` in the state directory,
  driven by the `git` command line with a fixed identity (the image needs `git`); run history lives
  in `runs/`, outside git; deployments are in memory.
- **Cuts inside a branch belong to `dtpipe split`**, run on the node whose data the branch reads
  (`BranchCut`: the branch alone, its two halves back); a branch that reads no node's data is not
  cut. `PlanBuilder` puts the consumer half in the branch's place, only regroups whole branches
  and adds `arrow:-` endpoints or relay branches; it never edits an option or moves a transformer.
  `AutoPlacer` only chooses a placement for `PlanBuilder` and cuts nothing. `[local: the lab's smoke.py]`
- **A brick is matched by content, never by name.** A branch is a brick when its canonical form
  (`BrickRules.Canonical`) equals the one its node declares; a pipeline stays a plain dtpipe job with
  no reference to a brick in it. `DesignService.Decompose` may take an imported branch apart
  around the bricks it contains (authoring, before anything is saved); the planner never does.
  `[local: the lab's smoke.py, ui_check.py]`
- **A fragment declares each edge alias once**, so a branch that receives an edge under its own
  alias (a cut's tail) sends on through a relay, never with `output: arrow:-` on itself.
  `[local: the lab's smoke.py lab]`
- **The designer's model is dtpipe's** (`DtPipe.Jobs`, rule in the root `CLAUDE.md`). The server
  offers `BrickCatalog` as its `IBrickLookup` and maps the page's `check` to the job's `lab-check:`
  directive (`PageDesignModel`); it adds no rule of its own to the cards. A brick's canonical form
  and version are pinned by `BrickFormTests`: changing either gives every brick a new version.
  `[local: DtPipe.Coordinator.Server.Tests, the lab's smoke.py, ui_check.py]`
- **A designer's placement is the library's placement.** `/api/design/place` composes the cards
  and hands the job to `AutoPlacer` like `/api/library/{id}/distribute`, saves nothing and accepts
  an id not yet in the library. It cuts nothing, so it asks no node and starts no `dtpipe`; keep
  it so. `[local: the lab's smoke.py library]`
- **A `PipelineNode` serves one run.** `DeploymentManager` re-arms every fragment of a pipeline
  after its run, and holds that deployment's gate from before it reports the run finished until
  the fragments are registered again, so no run starts on a spent instance.
  `[local: the lab's smoke.py]`
- **One run at a time per data node, first come first served.** `RunDispatch` decides, at each
  enqueue and each finish, which waiting runs `DeploymentManager` sends: one that no run already
  sent, and no older run still waiting, shares a data node with. A run's data nodes are its plan's
  fragment nodes that the inventory does not list as runners; a node it does not know counts
  (fail-closed). A run holds them from being sent until it finishes, its wait on its deployment's
  gate included, and is never sent twice. A runner never counts, so its fragments of two runs share
  its host with no resource bound. With no id, `/api/runs/cancel` refuses (409) while several runs
  are in flight rather than guess. `[local: RunDispatchTests, the lab's smoke.py library and sandbox]`
- **Ready means the main instance is registered.** A node unregisters from the coordinator before
  it disconnects, so a closed instance is never left for admission to pick.
  `WaitRegisteredAsync` requires the main instance in the deployment's current generation, held by
  the registry at the fragment's version. Every `Deploy` and `Rearm` carries a new generation,
  echoed in `FragmentStatus`. `[local: the lab's smoke.py]`
- **A disconnected host is forgotten fragment by fragment.** `NodeInventory.Disconnected` drops what
  the host last reported: a killed host would otherwise leave its fragments "Launched" for good. A host
  that only lost its connection reports them all again when it announces itself.
  `[local: the lab's tools/campaign.py F3]`
- **One source of rights.** `RightsStore` holds the identities and the flow matrix;
  `RightsFlowControl` replaces TransportR's `IFlowControlService`, so the hub checks the matrix as
  it is now on every transfer, and the planner and a deployment check every edge against it. A
  node in several groups sends only if every one of its groups may send to every group of the
  receiver, on both sides, and an edge carries the groups of both ends. Never add a second matrix.
  `RightsStore` reads only a document that says `"version": 2`; a saved plan without its groups is
  read as no plan.
  `[local: the lab's smoke.py rights]`
- **A data node only reads and writes, and runs only its bricks, which it composes by id.**
  `PlanBuilder` gives a data node's fragment as `FragmentUnit`s and composes the job with
  `FragmentJob` (`DtPipe.Coordinator.Contracts`), the function the node calls again on its own
  declarations, so both hash the same bytes; a branch that is none of the node's bricks is a planner
  error naming why (`BrickRules`), never a job sent to run. Only the Lab view's manual planning
  (`allowRaw`) sends a job as written, and only to a host that announces `raw-fragments`; the library
  and `AutoPlacer` never do. A saved plan whose fragments do not say how they reach their node
  (`FragmentForm`) is read as no plan. `BrickRules.Processors` is the one list of processors, shared with `JobGraph`.
  `DeploymentManager.WaitRegisteredAsync` fails on the first `Failed` main instance of the current
  generation and `DeployAsync` then undeploys the plan's fragments. `[local: the lab's smoke.py
  bricks, sandbox]`
- The pages are served from the `wwwroot` published beside the binary (`dotnet publish`): `index.html`
  + `js/` (the coordinator's views, ES modules, no build) and `lab/` (the Lab view, lab mode only).
  `window.lab.store` exists for the lab's `tools/ui_check.py`. `/healthz` is the only anonymous
  route. Verify a change with the lab's `./lab.sh smoke`, and a page change with
  `./lab.sh ui` too.
