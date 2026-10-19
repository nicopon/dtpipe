# samples/distributed-lab

Mechanics for editing the lab. What it is and how to run it: `README.md`. The coordinator it runs
is the product's, `src/DtPipe.Coordinator.Server` (its rules: that project's `CLAUDE.md`); the
nodes are `src/DtPipe.PipelineNode.Hosting` hosts (`src/DtPipe.PipelineNode.Hosting/CLAUDE.md`).

- **The lab uses dtpipe, it never changes it.** Nothing in `DtPipe.sln` moves for the lab. It
  reaches the engine through the binary (`split`, `--export-job`, `--job`, `inspect`, `--dry-run`)
  and hosts the distributed family (`src/DtPipe.Coordinator*`, `src/DtPipe.PipelineNode*`) as a
  consumer: product code a lab need may shape, never code written for the lab alone. A need of the
  engine is reported to the user, not patched in. `[unchecked]`
- **The lab runs the coordinator on `Coordinator:Store=redis`**, whatever the product's default is: it is the store that shares state between coordinators.
- **The lab's coordinator state, its library apart, is in a Redis whose settings are fixed once per state directory.** `lab.sh up` records the Redis
  (`LAB_REDIS`, else a `redis-server` of its own on `LAB_REDIS_PORT` with its data under `.state/redis`, fsynced on every write,
  and a message naming what is missing when there is none) and a random key prefix in `.state/redis.env`; `exec-coordinator`
  reads it back, so `tools/campaign.py` restarts the coordinator on the same journal and a reset lab starts a new one even on a
  Redis that outlives it. `Coordinator:Initialize` is true here only: a lab begins where Redis holds nothing. The stack's
  `redis` service keeps its data in `.stack/redis`, its own directory like every service's. `[local: lab.sh up, tools/campaign.py F4]`
- **The lab's library is pushed to a bare repository beside its state.** `exec-coordinator` creates `.state/library.git` when it is missing
  (`git init --bare`, nothing of the server) and passes it as `Coordinator:Library:Remote:Url`, so `tools/campaign.py` restarts the coordinator on the
  same remote and `lab.sh reset` takes it with the state. The stack's coordinator mounts `.stack/library.git` for the same purpose and its
  entrypoint (`docker/coordinator-entrypoint.sh`) creates the repository on first start, as a node seeds its own database. The remote is the one
  directory the stack adds beside a service's own: it stands for a git server a deployment would run, and only the coordinator mounts it. `[local: lab.sh up, lab.sh stack up, smoke.py library]`
  `[unchecked: that a restart with the local copy removed reads the library and an armed deployment back, shown by a throwaway script only]`
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
- **`monitor-1` is the lab's supervisor**: a client of `idp-clients.json` and an identity of kind `monitor` in
  `rights-seed.json`, which reads `/health` and nothing else. A stack keeps its rights (in Redis) across restarts, so
  `smoke.py bricks` adds the identity through `POST /api/rights/identities` when the rights predate the seed.
  It arms `customers-mirror` for the check and undeploys it. `[local: smoke.py bricks - health, ui_check.py]`
- **`launcher-1` is the lab's scheduler**: a client of `idp-clients.json` and an identity of kind `launcher` in
  `rights-seed.json` that lists `customers-mirror`, which starts its runs through `POST /service/runs` and reads them, and nothing
  else. `smoke.py bricks` creates the identity, or completes its list, when the rights predate the seed; it launches twice with one
  key and expects one run. `[local: smoke.py bricks - a service launches a run]`
- **Lab nodes are disposable hosts of `DtPipe.PipelineNode.Hosting`.** `Lab.NodeHost` reads
  `nodes/<node>.json` into `AddDtPipeNode`; a node file whose `role` is `runner` starts the official
  `DtPipe.PipelineNode.Runner` instead, configured through its environment. `lab.sh exec-node` is
  the one way a node starts, for `lab.sh` and `tools/campaign.py`. Every lab node registers `lab.split`
  (`Lab.NodeHost/LabRequests.cs`). `smoke.py` reads the warehouse and its witness reads the databases
  `seed.sh` wrote, located from the node files, never from the coordinator. Every lab node also enables
  fault injection (`FaultInjection` in `Lab.NodeHost`'s options, `DtPipeNode__FaultInjection` in the
  environment of the runner `lab.sh exec-node` starts and of `runner-1` in `compose.yaml`), so the kill
  route of lab mode reaches the hosts of a plan. Node mechanics: `src/DtPipe.PipelineNode.Hosting/CLAUDE.md`.
  `[local: smoke.py, for the two requests; tools/campaign.py F1 (the data hosts) and F12 (the runner `lab.sh` starts), named]`
  `[unchecked: runner-1 of compose.yaml enabling it]`
- **The lab is stopped only when nothing of it runs.** A process of the lab is one whose command line names
  the state directory (`.state/`, or `LAB_STATE`). `lab.sh down` stops the processes the pid files name, then
  every other one, and answers "Lab stopped" only when none is left; otherwise it lists them and fails. `up`
  refuses to start over a process of the lab that no pid file names and that does not descend from one that a
  pid file names (a host's `dtpipe` children are not refused), or, when `lsof` is installed, over a port the
  coordinator or the IdP needs that something else holds; `status` shows those processes.
  `[local: tools/validate_lab_down.sh]`
- **`LAB_REPLICAS` starts more hosts of one node's identity** (`<node>.r<n>`, `lab.sh`'s header): each
  makes its own working directory and the databases stay shared. `lab.sh down`, `nodes` and `status` also
  take in any replica a pid file records, so a run without the variable that started them stops them; a switch
  of mode without it stops a replica and starts none, so export it to every call. `LAB_NODE_FILE` makes
  `exec-node` read another declaration of the node, which is how `smoke.py replicas` starts a replica at another
  version of a brick. That pass is named only, refuses the compose stack (it starts hosts), and leaves the hosts
  started as the lab starts them. `[local: smoke.py replicas, tools/campaign.py F13]`
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
  coordinator restart, network faults through toxiproxy, revoked rights, rapid cycles; F6g, a node's
  groups changed under a run, F12, a runner host's child killed while it starts another run's, and F13, the
  replica of node-4 that executes a fragment killed (the lab started with `LAB_REPLICAS=node-4=2`), are
  outside the matrix and played only when named, and naming a cell plays only the cells named) against the
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
  process, every armed deployment binding only nodes the rights give an identity, and every host's binary
  (`--dtpipe` for a data node, `DtPipeNode__DtPipe` in its environment for the runner) starting and
  reporting the full hash of the frozen commit with a clean tracked tree. Every call retried on `EADDRINUSE`/`EADDRNOTAVAIL` and every port-exhaustion line
  in the lab's logs is counted into the trial's `I7`, never absorbed. `[local: tools/campaign.py with
  CAMPAIGN_PREFLIGHT=report prints the checks without refusing]`
- **The compose stack mounts nothing across services.** `compose.yaml` gives each service its own
  directory under `.stack/` (a node its database, the coordinator its state and its library remote, the IdP its key) and
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
  the IdP names itself without a default port, and `dtpipe studio` refuses an authority that differs
  from that name by more than a trailing slash, an explicit `:80` included. `[local: lab.sh stack
  studio]`
- **The lab's images extend the product's, never the other way.** `deploy/Dockerfile` builds the coordinator, the runtime and the
  runtime with `dtpipe`; `compose.yaml` builds each as a service of `scale: 0` and hands it to `docker/Dockerfile` as a named build
  context (`additional_contexts`), where `coordinator-lab`, `node` and `idp` add lab content. `runner-1` is the product's `runner`
  target as built. Lab content enters through those three targets or `compose.yaml`'s environment, never `deploy/`. Every .NET
  service runs under `init: true`: dotnet as PID 1 hangs on its own crash, and the nodes start dtpipe children. `[unchecked]`
- **The stack's images are built from the commit HEAD names, and its services run unprivileged.** `compose()` in `lab.sh` sets
  `SOURCE_REVISION` (`${SOURCE_REVISION:-}` in `compose.yaml`: a `:?` would fail `down` and `exec` too; an empty value
  builds images labelled `unknown`, which the `versions` guard refuses), `LAB_UID` and `LAB_GID`. The services that mount a `.stack/` directory (`node-N`, `idp`, `coordinator`,
  `redis`) run as that user and with `HOME=/tmp`, so nothing in `.stack/` is owned by root; `lab.sh stack up` creates the directories
  first, as docker would create them as root. The runner mounts nothing and keeps the image's own user (1654). A throwaway container
  of the stack (the enrollment check) runs as the invoking user too. `smoke.py bricks - versions` requires every node's `dtpipe` and
  the revision label of the coordinator, runner and node images to be HEAD; a tree that is dirty at the same HEAD is not seen.
  `[local: lab.sh stack smoke]` `[unchecked: Docker on Linux, where a bind mount keeps its owner]`
- **The stack is a target of the lab's checks, never a second set.** `lab.sh stack smoke|ui|studio`
  run `smoke.py`, `tools/ui_check.py` and `tools/studio_check.py` against `STACK_URL`;
  `LAB_TARGET=stack` is the only switch, read where the stack differs (the library directory, the
  coordinator's log, the enrollment host, the revision each node's own binary and image must carry). A new check that
  starts a process or reads a path of the local lab takes its stack form there too.
  `[local: lab.sh stack smoke, ui, studio]`
- Verify a change with `./lab.sh smoke`, a page change with `./lab.sh ui` too, and a change to what
  `dtpipe studio` calls with `./lab.sh studio` (`tools/studio_check.py`, outside CI); one that touches
  the images, `compose.yaml` or a check's stack form with `./lab.sh stack smoke|ui|studio` as well. The page checks
  drive headless Chrome over the DevTools protocol (`tools/cdp.py`), because Chrome's own
  `--screenshot` stops the page before its fetches finish. `tools/cdp.py` also clicks and drags with the mouse.
