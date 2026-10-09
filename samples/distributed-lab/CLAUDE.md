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
- **Lab tools sign in as people, through the IdP.** `tools/lab_session.py` follows the real flow
  (login form, code back to `/signin-oidc`) and sends the CSRF header; every tool reaches `/api`
  through it (`smoke.call`, `ui_check.py`, `nodes_report.py`, `lab.sh`). `ui_check.py` also signs its
  browser in through the IdP's form, so the flow a person follows runs on every pass. `lab.sh exec-coordinator` lists `labadmin`, `labdesigner` and
  `labreader` (`@dtpipe.test`) in the three roles; any other `{user}` / `{user}pwd` signs in with
  none. `[local: smoke.py rights — roles and sessions]`
- **Sandbox is a lab node's own switch.** `LAB_NODE_SANDBOX=1` (or `true`) in `Lab.NodeHost`'s
  environment registers a fragment gate that accepts any reader or writer, the Lab view's mode; `lab.sh nodes
  strict|sandbox` restarts the hosts in a mode, and `lab.sh smoke` does so per pass. The runner
  runs any fragment in both. `[local: smoke.py bricks, sandbox]`
- **Lab nodes are disposable hosts of `DtPipe.PipelineNode.Hosting`.** `Lab.NodeHost` reads
  `nodes/<node>.json` into `AddDtPipeNode`; a node file whose `role` is `runner` starts the official
  `DtPipe.PipelineNode.Runner` instead, configured through its environment. `lab.sh exec-node` is
  the one way a node starts, for `lab.sh` and `tools/campaign.py`. Every lab node registers `lab.query` and
  `lab.split` (`Lab.NodeHost/LabRequests.cs`), so `smoke.py` reads the warehouse through `/api/query`; its witness reads the databases
  `seed.sh` wrote, located from the node files, never from the coordinator. Node mechanics:
  `src/DtPipe.PipelineNode.Hosting/CLAUDE.md`. `[local: smoke.py]`
- **The IdP's signing key survives an IdP restart.** A node host reuses its cached token until
  shortly before it expires, so `Lab.Idp` reads its key from `.state/idp/signing-key.pem` (generated
  once, readable by its owner only, outside git). `lab.sh idp --Idp:AccessTokenLifetimeSeconds=N`
  restarts it with shorter tokens for trials. `[local: tools/campaign.py S3, S5]`
- **`Lab.Idp` says who, never what.** Its tokens carry a subject, a name and an email; no group,
  role or permission enters a token or the clients file, because what a caller may do belongs to
  the coordinator. A client's `audience` and `refreshTokens` are protocol, not a right. It stands
  alone: no reference to dtpipe, TransportR or the coordinator. `[local: tools/idp_check.py]`
- **`dtpipe-studio` is the studio's client**: native and public, registered at
  `http://127.0.0.1/callback` with no port, so any loopback port is accepted (RFC 8252), never at
  an extra origin; its people's access tokens name the coordinator's audience, and it may refresh
  them. `lab.sh exec-coordinator` names it in `Coordinator:Studio:ClientId`; `coordinator-ui`'s
  tokens name no audience. `lab_session.studio_tokens` signs a person in through it, the code read
  off the redirect to the loopback. Its refresh tokens live in the IdP's memory: an IdP restart
  signs every studio out. `[local: tools/idp_check.py, smoke.py rights — studio bearer]`
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
  peer would (`campaign_security.py`, a SignalR client over long polling); S3 and S5 restart the IdP
  with shorter tokens through `lab.sh idp`, check the lifetime it now issues, and restart the hosts
  themselves (`lab.sh nodes` leaves hosts already in their mode, and their tokens, alone). Every
  wait for the hosts names the ones still offline, so a reference check that reads the warehouse
  through node-4 never fails for a node-4 not yet back. It records, it never fixes. It starts only on a bench it can judge
  (`campaign_bench.py`): only the lab's own processes, a machine with no build, test run or TransportR
  process, and the binary the hosts run starts and reports the full hash of the frozen commit with a
  clean tracked tree. Every call retried on `EADDRINUSE`/`EADDRNOTAVAIL` and every port-exhaustion line
  in the lab's logs is counted into the trial's `I7`, never absorbed. `[local: tools/campaign.py with
  CAMPAIGN_PREFLIGHT=report prints the checks without refusing]`
- **The compose stack mounts nothing across services.** `compose.yaml` gives each service its own
  directory under `.stack/` (a node its database, the coordinator its state, the IdP its key) and
  the runner none: the stack is the proof that the coordinator reads no node's data, so a volume
  shared between two services voids it. A node seeds its own database on first start
  (`docker/node-entrypoint.sh`, `seed.sh` with its name) and refuses a scale other than the one it
  recorded. `[local: lab.sh stack smoke]`
- **The IdP has one name for browsers and containers.** The proxy carries `idp.localtest.me` as a
  network alias and publishes the same port, so a token names one issuer everywhere; the authority
  stays `http://`, a choice of the lab. `idp-clients.json` registers only the local lab's URIs;
  the stack hands the IdP its coordinator's origin (`Idp:ExtraOrigins`, port included), so any
  `LAB_STACK_HTTP_PORT` works unedited. The coordinator is browsed at `coordinator.localtest.me`,
  never `127.0.0.1` or `localhost`, which the local lab's cookies hold. Nodes reach the coordinator
  directly, never through the proxy. The issuer spells a port only when `LAB_STACK_HTTP_PORT` is set:
  the IdP names itself without a default port, and `dtpipe studio` refuses an authority that is not
  that name character for character. `[local: lab.sh stack studio]`
- **The `coordinator` and `runner` image targets hold nothing of the lab**: lab content enters
  through `coordinator-lab`, `node`, `idp` or `compose.yaml`'s environment, so those two targets
  can leave `samples/` unchanged. Every .NET service runs under `init: true`: dotnet as PID 1 hangs
  on its own crash, and the nodes start dtpipe children. `[unchecked]`
- **The stack is a target of the lab's checks, never a second set.** `lab.sh stack smoke|ui|studio`
  run `smoke.py`, `tools/ui_check.py` and `tools/studio_check.py` against `STACK_URL`;
  `LAB_TARGET=stack` is the only switch, read where the stack differs (the library directory, the
  coordinator's log, the enrollment host, the version of each node's own binary). A new check that
  starts a process or reads a path of the local lab takes its stack form there too.
  `[local: lab.sh stack smoke, ui, studio]`
- Verify a change with `./lab.sh smoke`, a page change with `./lab.sh ui` too, and a change to what
  `dtpipe studio` calls with `./lab.sh studio` (`tools/studio_check.py`, outside CI); one that touches
  the images, `compose.yaml` or a check's stack form with `./lab.sh stack smoke|ui|studio` as well. The page checks
  drive headless Chrome over the DevTools protocol (`tools/cdp.py`), because Chrome's own
  `--screenshot` stops the page before its fetches finish. `tools/cdp.py` also clicks and drags with the mouse.
