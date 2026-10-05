# samples/distributed-lab

Mechanics for editing the lab. What it is and how to run it: `README.md`. The coordinator it runs
is the product's, `src/DtPipe.Coordinator.Server` (its rules: that project's `CLAUDE.md`); the
nodes are `src/DtPipe.PipelineNode.Hosting` hosts (`src/DtPipe.PipelineNode.Hosting/CLAUDE.md`).

- **The lab uses dtpipe, it never changes it.** Nothing in `DtPipe.sln` moves for the lab. It
  reaches the engine through the binary (`split`, `--export-job`, `--job`, `inspect`, `--dry-run`)
  and hosts the distributed family (`src/DtPipe.Coordinator*`, `src/DtPipe.PipelineNode*`) as a
  consumer: product code a lab need may shape, never code written for the lab alone. A need of the
  engine is reported to the user, not patched in. `[unchecked]`
- **The lab runs the server in lab mode, on its own seeds.** `lab.sh exec-coordinator` is the one
  way the server starts, for `lab.sh` and `tools/campaign.py`: `Coordinator:LabMode`, the scenarios
  in `pipelines/`, the library from `library-seed/`, the rights from `rights-seed.json`, the state in
  `.state/`. A lab scenario, brick or pipeline is named under `samples/` only.
  `[CI: validate_distributed_boundary.sh]`
- **Sandbox is a lab node's own switch.** `LAB_NODE_SANDBOX=1` (or `true`) in `Lab.NodeHost`'s
  environment maps to `FragmentPolicy.AnyReadOrWrite`, the Lab view's mode; `lab.sh nodes
  strict|sandbox` restarts the hosts in a mode, and `lab.sh smoke` does so per pass. The runner
  runs any fragment in both. `[local: smoke.py bricks, sandbox]`
- **Lab nodes are disposable hosts of `DtPipe.PipelineNode.Hosting`.** `Lab.NodeHost` reads
  `nodes/<node>.json` into `AddDtPipeNode`; a node file whose `role` is `runner` starts the official
  `DtPipe.PipelineNode.Runner` instead, configured through its environment. `lab.sh exec-node` is
  the one way a node starts, for `lab.sh` and `tools/campaign.py`. Node mechanics:
  `src/DtPipe.PipelineNode.Hosting/CLAUDE.md`. `[local: smoke.py]`
- **The IdP's signing key survives an IdP restart.** A node host reuses its cached token until
  shortly before it expires, so `Lab.Idp` reads its key from `.state/idp/signing-key.pem` (generated
  once, readable by its owner only, outside git). `lab.sh idp --Idp:AccessTokenLifetimeSeconds=N`
  restarts it with shorter tokens for trials. `[local: tools/campaign.py S3, S5]`
- **`Lab.Idp` says who, never what.** Its tokens carry a subject, a name and an email; no group,
  role or permission enters a token or the clients file, because what a caller may do belongs to
  the coordinator. It stands alone: no reference to dtpipe, TransportR or the coordinator.
  `[local: tools/idp_check.py]`
- A node's tokens come from the IdP its `Oidc:Authority` names, through the discovery document:
  `lab.sh` starts `Lab.Idp` first and points the coordinator (`--authority`) and every node at it.
  The secrets live in `idp-clients.json` (the IdP) and the node files (the clients), never in the
  rights document. `[local: smoke.py rights]`
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
