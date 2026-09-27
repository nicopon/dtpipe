# samples/distributed-lab

Mechanics for editing the lab. What it is and how to run it: `README.md`.

- **The lab uses dtpipe, it never changes it.** No file under `src/` and nothing in `DtPipe.sln`
  moves for the lab. It reaches dtpipe through the binary (`split`, `--export-job`, `--job`) and
  hosts `DtPipe.Coordinator` / `DtPipe.PipelineNode` as libraries. A need the libraries do not
  meet is reported to the user, not patched in. `[unchecked]`
- **Cuts inside a branch belong to `dtpipe split`.** `PlanBuilder` only regroups whole branches
  and adds `arrow:-` endpoints or relay branches; it never edits an option or moves a transformer.
  `[local: smoke.py]`
- **A `PipelineNode` serves one run.** `LabRunner` re-arms every fragment after each run and holds
  the deploy gate from before it reports the run finished until the fragments are registered
  again, so no run starts on a spent instance. `[local: smoke.py]`
- **Ready means no stale instance is live.** A closed instance stays live in `NodeRegistry` until
  the hub sees its disconnect, with the same version as its replacement; admission may pick it and
  its `Launch` is never answered. `WaitRegisteredAsync` therefore requires the main instance in the
  current generation and every live `ClientId` to be one a node host reports now. Every `Deploy`
  and `Rearm` carries a new generation, echoed in `FragmentStatus`. `[unchecked]`
- The page is served from `Lab.Coordinator/wwwroot` in the sources, not from the build output.
- Verify a change with `./lab.sh smoke`, and a page change with `./lab.sh ui` too: it drives
  headless Chrome over the DevTools protocol (`tools/cdp.py`), because Chrome's own `--screenshot`
  stops the page before its fetches finish. `tools/cdp.py` also clicks, for a scripted scenario.
