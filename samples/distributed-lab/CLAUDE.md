# samples/distributed-lab

Mechanics for editing the lab. What it is and how to run it: `README.md`.

- **The lab uses dtpipe, it never changes it.** No file under `src/` and nothing in `DtPipe.sln`
  moves for the lab. It reaches dtpipe through the binary (`split`, `--export-job`, `--job`,
  `inspect`, `--dry-run`) and hosts `DtPipe.Coordinator` / `DtPipe.PipelineNode` as libraries. A
  need the libraries do not meet is reported to the user, not patched in. `[unchecked]`
- **Cuts inside a branch belong to `dtpipe split`.** `PlanBuilder` only regroups whole branches
  and adds `arrow:-` endpoints or relay branches; it never edits an option or moves a transformer.
  `AutoPlacer` only chooses a placement for `PlanBuilder` and cuts nothing. `[local: smoke.py]`
- **A brick is matched by content, never by name.** A branch is a brick when its canonical form
  (`JobYaml.Canonical`) equals the one its node declares; a pipeline stays a plain dtpipe job with
  no reference to a brick in it. `DesignService.Decompose` may take an imported branch apart
  around the bricks it contains (authoring, before anything is saved); the planner never does.
  `[local: smoke.py, ui_check.py]`
- **The server writes the job, the page never does.** `DesignService.Compose` is the only writer
  of a designer pipeline's YAML; the page sends cards and reads back the text. `[unchecked]`
- **A `PipelineNode` serves one run.** `DeploymentManager` re-arms every fragment of a pipeline
  after its run, and holds that deployment's gate from before it reports the run finished until
  the fragments are registered again, so no run starts on a spent instance. Runs of every
  pipeline wait in one queue, since `RunOrchestrator` refuses a second one in flight.
  `[local: smoke.py]`
- **Ready means no stale instance is live.** A closed instance stays live in `NodeRegistry` until
  the hub sees its disconnect, with the same version as its replacement; admission may pick it and
  its `Launch` is never answered. `WaitRegisteredAsync` therefore requires the main instance in the
  deployment's current generation and every live `ClientId` to be one a node host reports now.
  Every `Deploy` and `Rearm` carries a new generation, echoed in `FragmentStatus`. `[unchecked]`
- **A node is who its token says.** Every client of the coordinator authenticates with the
  embedded IDP (`EmbeddedIdp`, OpenIddict, client credentials); a node's group is its identity's,
  carried as `transportr:group:<g>` and read by TransportR's `JwtIdentityProvider`, never the one a
  node declares. `LabHub.Announce` refuses a host announcing a node its identity is not bound to.
  `[local: smoke.py rights]`
- **One source of rights.** `RightsStore` holds the identities, the flow matrix and the brick
  policies; `RightsFlowControl` replaces TransportR's `IFlowControlService`, so the hub checks the
  matrix as it is now on every transfer, and every plan (`/api/plan`, `/api/deploy`, `AutoPlacer`,
  a library deploy) goes through `RightsStore.WithPolicies`. Never add a second matrix.
  `[local: smoke.py rights]`
- `PipelineNodeOptions` has no credential: a node host gives each pipeline node a hub URL carrying
  its token in the path (`/t/<token>/…`), which `EmbeddedIdp.UseTokenInPath` turns back into a
  bearer header before routing. It must stay the first middleware after the conflict handler.
  `[unchecked]`
- The library is a git repository of its own under `.state/library`, driven by the `git` command
  line with a fixed identity; it never touches the dtpipe repository around it. It starts from
  `library-seed/`. Run history lives in `.state/runs/`, outside git. Deployments are in memory.
- The pages are served from `Lab.Coordinator/wwwroot` in the sources, not from the build output:
  `index.html` + `js/` (the coordinator's views, ES modules, no build) and `lab.html` + `app.js`
  (the Lab view). `window.lab.store` exists for `tools/ui_check.py`. The page and `/api` are not
  authenticated: the lab has one operator.
- Verify a change with `./lab.sh smoke`, and a page change with `./lab.sh ui` too: it drives
  headless Chrome over the DevTools protocol (`tools/cdp.py`), because Chrome's own `--screenshot`
  stops the page before its fetches finish. `tools/cdp.py` also clicks and drags with the mouse.
