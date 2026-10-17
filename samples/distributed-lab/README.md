# dtpipe distributed lab

A self-contained demonstrator for dtpipe's distributed pipelines: one coordinator, four data
nodes each with its own database, and a runner, run as plain local processes or as a compose
stack ([below](#the-compose-stack)).
Every node authenticates with a test identity provider, `Lab.Idp`, which says who it is; the
coordinator decides what it may do.

Each data node offers **bricks**: reads and writes its owner preconfigured on its database. The
coordinator's page assembles bricks and processing steps into a pipeline by drag and drop, keeps
it in a git-backed **library**, places every brick on its node and every other step on the
runner, deploys the fragments and runs them, live. A second page, the **Lab view**, cuts and
places a job by hand and injects faults.

The lab **uses** dtpipe and changes nothing in it. Its coordinator is the product's server,
`src/DtPipe.Coordinator.Server`, run in **lab mode** (the Lab view, fault injection and the query
panel exist only then); its runner is the official `DtPipe.PipelineNode.Runner`; its data nodes are
disposable hosts of `DtPipe.PipelineNode.Hosting`. They drive the `dtpipe` binary like a user
(`split`, `--export-job`, `--job`). None of it is in `DtPipe.sln`: `build.yml` never builds it.

## Quick start

Prerequisites: the .NET 10 SDK, `python3`, a bash shell, network access to nuget.org for the
TransportR `0.5.1` packages (restored like any other dependency of `DtPipe.Coordinator`), and a
built binary:

```bash
./build.sh                          # at the repository root: produces dist/release/dtpipe
cd samples/distributed-lab
./lab.sh up                         # build, seed the four databases, start everything
open http://127.0.0.1:5180          # sign in as labadmin / labadminpwd; the Lab view is /lab/
./lab.sh smoke                      # every pipeline, distributed, against its witness
./lab.sh down
```

| Command | Does |
|---|---|
| `./lab.sh up` | builds the lab, seeds the databases on first use, starts the IdP, the coordinator, then the nodes (`LAB_REPLICAS="node-4=2,runner-1=2"` starts that many hosts of one identity for those nodes: the first is named for the node, the others `node-4.r2`, …; each makes its own directory under the node's, the databases stay shared) |
| `./lab.sh status` | which processes run, and what the coordinator sees of each node |
| `./lab.sh smoke [lab\|sandbox\|library\|rights\|faults\|bricks] [id…]` | runs the end-to-end check, optionally one pass or some pipelines only; restarts the node hosts in the mode each pass needs |
| `LAB_REPLICAS=node-4=2 ./lab.sh smoke replicas` | the replica model against node-4 carried by two hosts: a run names its replica, a stopped one leaves at once and removes its directory, a cut one is listed disconnected then lost after its grace, an updated one is passed over, and a run is refused when none left composes its plan; played only when named, local lab only, and the variable must be exported to every `lab.sh` call that restarts hosts |
| `./lab.sh nodes strict\|sandbox` | restarts the node hosts in that mode after undoing every deployment (`LAB_NODE_SANDBOX=1 ./lab.sh up` starts them in sandbox mode) |
| `./lab.sh studio` | drives `dtpipe studio` against the lab in headless Chrome: open from the library, debug a pipeline of bricks (watch, play, reset), import, run and stop, publish and a refusal; screenshots to `.state/studio/`, outside CI |
| `./lab.sh ui` | drives both pages in headless Chrome, the library without any designer entry, a pipeline's page with its job read only; screenshots to `.state/ui/` |
| `./lab.sh seed` | rebuilds the databases (`LAB_SCALE=5` multiplies the row counts) |
| `./lab.sh logs` | follows every log |
| `./lab.sh stack up\|down\|status\|nodes\|smoke\|ui\|studio\|logs` | the same commands against [the compose stack](#the-compose-stack) |
| `./lab.sh down` / `reset` | stops everything, including a process of the lab whose pid file was lost (it says "Lab stopped" only when none is left, and otherwise lists them and fails) / also deletes `.state/` |

Every page and `/api` call needs a person signed in through the IdP, with a role: `labadmin`
(`labadminpwd`) may do everything, `labdesigner` everything but the rights and the Lab view,
`labreader` look. The lab's tools sign in as `labadmin` through the same flow.

`LAB_PORT` changes the coordinator's port (default `5180`), `LAB_IDP_PORT` the IdP's (`5190`),
`DTPIPE` the binary. Everything the lab writes lives
under `.state/`: databases, fragment jobs, plans, logs, build output.

## The compose stack

The same lab in containers, each service on its own data: no service mounts another's
directory, and the coordinator never reads a node's database. Prerequisites: Docker with
Compose, and port 80 free. The images build dtpipe for Linux themselves, so no `./build.sh` is
needed to start it.

```bash
cd samples/distributed-lab
./lab.sh stack up                   # build the images, seed each node on first start, start all
open http://lab.localtest.me        # the home page: links, health, test accounts
./lab.sh stack smoke                # the same checks as the local lab's, against the stack
./lab.sh stack down                 # stops everything; .stack/ keeps the data
```

`docker compose up -d --build` and `down` do the same as `stack up` and `stack down`.
`*.localtest.me` resolves to `127.0.0.1`: the host needs a DNS that answers for it. A proxy (Caddy)
publishes the stack's only port and routes by host name: `coordinator.localtest.me` to the
coordinator, `idp.localtest.me` to the IdP, `lab.localtest.me` to the home page. Inside the
network the proxy carries the same names, so the IdP has one issuer for browsers and containers.
The nodes reach the coordinator directly; the data plane never crosses the proxy. The
coordinator's distinct host name also keeps its session cookie apart from the local lab's, so both
run side by side. Browse the coordinator at that name, never `127.0.0.1`.

| Directory | Mounted by | Holds |
|---|---|---|
| `.stack/idp/` | `idp` | its signing key |
| `.stack/coordinator/` | `coordinator` | library, runs, rights, session keys |
| `.stack/node-N/` | `node-N` | its database, seeded on first start, and its fragment jobs |

The runner mounts nothing. `LAB_SCALE` (default `1`) multiplies the seeded rows for every node: a
node's rows reference the others' counts, so a node seeded at another scale refuses to start; delete
`.stack/` to seed again. `LAB_NODE_SANDBOX=1 ./lab.sh stack up` starts the data nodes in sandbox
mode. `LAB_STACK_HTTP_PORT` moves the proxy off port 80, outside and inside: tokens name the issuer
with that port, and the IdP accepts the coordinator's redirects at it (`Idp:ExtraOrigins`). Leave
it unset for port 80: the IdP names itself without a default port, and `dtpipe studio` refuses an
authority that differs from that name by more than a trailing slash, an explicit `:80` included.

`./lab.sh stack smoke`, `ui` and `studio` run the scripts the local lab runs, from the host with
`./build.sh`'s binary, pointed at the coordinator's name; `stack nodes strict|sandbox` recreates
the four node containers in a mode, after undoing every deployment, as `lab.sh nodes` restarts the
local hosts. The witness reads the databases the nodes seeded under `.stack/`. Where the stack
differs, `smoke.py` follows `LAB_TARGET=stack`, which `lab.sh` sets:

- the library check reads the coordinator's own directory, `.stack/coordinator/library`;
- the coordinator's log is the container's output (`docker compose logs coordinator`);
- the enrollment check's host is a throwaway container of the `node` image on the stack's network,
  reaching the coordinator at `http://coordinator:8080`;
- the version check compares each node with the `dtpipe` of its own container: the image builds it
  from the tree without its git metadata, so it carries no revision suffix.

The containers run in UTC whatever the host's zone; the witness runs in the host's. The campaign
(`tools/campaign.py`) is not played on the stack.

## The nodes

| Node | Group | Database | Holds |
|---|---|---|---|
| node-1 | `crm` | SQLite `crm.sqlite` | `customers` (20 000) |
| node-2 | `sales` | DuckDB `sales.duckdb` | `orders` (300 000, current year) |
| node-3 | `catalog` | SQLite `catalog.sqlite` | `products` (500), `legacy_orders` (100 000) |
| node-4 | `warehouse` | DuckDB `warehouse.duckdb` | nothing until a pipeline writes it |
| runner-1 | `runner` | none | every step between the bricks |

Each data node's bricks are in its `nodes/<node>.json`, under `bricks`: a source is an `input`
and its reader options, a sink an `output` and its writer options (a table, a strategy, a key).
The node inspects each source's schema itself (`dtpipe inspect`) and reads previews on request:
the coordinator never opens a brick's database.

`seed/seed.sh` builds them with dtpipe itself (`generate:`, `--fake`, a DuckDB `--sql`);
row-seeded fakes and hashed values make every seed identical. A node publishes its database as a
variable (`LAB_CRM_DB`, …) that its `dtpipe` children see, so a job names
`sqlite:${{LAB_CRM_DB}}` and never a path: the fragment runs against whichever node hosts it.

A data node is `Lab.NodeHost`, a plain .NET host that adds a node to itself with
`DtPipe.PipelineNode.Hosting` (`AddDtPipeNode`), as any business service would. `runner-1` is the
official runner, `DtPipe.PipelineNode.Runner`, configured only through its environment
(`DtPipeNode__Name`, `DtPipeNode__CoordinatorUrl`, `DtPipeNode__Oidc__Authority`, …): `lab.sh`
starts it that way for a node file whose `role` is `runner`.

A node's groups are not in its file, nor in its token: the coordinator gives them (below). The flow matrix says
which group may send to which: every data group may feed the `runner`, the `runner` feeds
`warehouse`, and the direct links the Lab view's scenarios use stay.

## Rights and the identity provider

Tokens come from `Lab.Idp` (below), which the coordinator trusts for its audience. Each node host
holds a client id and secret in its `nodes/<node>.json`, which `idp-clients.json` registers; the
token it obtains names it (`sub`) and grants nothing. The coordinator gives that subject its groups,
which TransportR's hub then enforces: a node sends to another only if every group of the sender may
send to every group of the receiver. What is allowed lives in one document, `.state/rights.json`,
started from `rights-seed.json` and edited in the **Access** view:

| | Enforced |
|---|---|
| **Who**: identities, each a client of the IdP with a node name and one or more groups | by the coordinator (no group for an unknown or disabled client, from its next connection or token renewal) and by the control channel (a host is filed under its identity's node name, whatever it proposes; a disabled identity is refused) |
| **To whom**: the flow matrix between groups | by the hub on **every transfer**, live, and on every plan |

Every change is audited. A node host gives its pipeline node a token provider
(`NodeClientOptions.AccessTokenProvider`) that asks the IdP for the node's token. Adding a node
takes its client in `idp-clients.json` (then an IdP restart); its identity then comes from the Access view, or by approval:
a host whose token no identity knows is not a node. It waits in the Access view's *Waiting for approval* card,
with the name, description and bricks it proposes (and the token's claims listed by
`Coordinator:Identity:LabelClaims`, none by default), and an administrator accepts it by giving it a node name and
groups, or sets it aside for 24 hours. The list lives in memory, holds 50 hosts, and never evicts one that waits.

### The stand-alone test IdP (`Lab.Idp`)

The identity provider of the lab, standing in for whatever provider a deployment trusts. It says
**who** a caller is, never what the caller may do: its tokens carry a subject, a name and an email,
no group and no role. `lab.sh` starts it on `LAB_IDP_PORT`, its signing key in
`.state/idp/signing-key.pem` (generated once, readable by its owner only), so tokens in circulation
survive its restart.

| Caller | Flow | Token |
|---|---|---|
| a service (node, runner) | `client_credentials`, client id and secret from `idp-clients.json` | `sub` = client id, `aud` = `Idp:Audience` |
| a front end (`coordinator-ui`) | `authorization_code` + PKCE, through the IdP's login form | `sub`, `name`, `email` = `<user>@Idp:EmailDomain` |

Any user name of letters, digits, `.`, `_` or `-` signs in with that name followed by `pwd`
(`toto` / `totopwd`). Settings are the `Idp` configuration section, from the environment
(`Idp__Issuer`) or the command line (`--Idp:Issuer`):

| Setting | Default |
|---|---|
| `Issuer` | `http://localhost:5190/` (the lab passes `http://127.0.0.1:5190`): the one address browsers and services both reach |
| `ClientsFile` | `clients.json` in the content root; the lab's is `idp-clients.json` |
| `SigningKeyFile` | `.state/signing-key.pem` in the content root, created on first start |
| `EmailDomain` · `Audience` · `AccessTokenLifetimeSeconds` | `dtpipe.test` · `dtpipe-coordinator` · `43200` |

```bash
./lab.sh up                         # starts it on 5190 with the lab's clients
python3 tools/idp_check.py          # discovery, both flows, and the refusals that matter
```

## The coordinator's page

| View | Does |
|---|---|
| Nodes | every node, its bricks (schema, version, preview), the fragments it hosts, the flow matrix |
| Library | the pipelines kept, their plan and deployment state, last run, git history, diff, restore; a pipeline's page shows its plan, its fragments and its job, read only. A pipeline is written with `dtpipe studio --coordinator` and published here |
| Distribution | the plan placed automatically, drawn in lanes (sources, runner, sinks); save it, deploy, run |
| Runs | deployments, the runs in flight and the queue, the journal with each run's report, node events, a query on any database |
| Access | the trusted IdP, the hosts waiting for approval, identities (node name, groups, enabled, reconnect), the editable flow matrix, the audit |

**A pipeline is a plain dtpipe job**, one branch per card, that also runs whole on one machine
(`smoke.py` uses it as the witness). A branch *is* a brick when its content equals the brick's:
no reference in the YAML, and a brick its owner changes stops matching. `dtpipe studio` takes a job apart around the bricks it contains (a branch reading a brick,
transforming, then writing a brick becomes three cards).

**Distribution is automatic and happens at each launch**: a brick runs on the node its binding names,
every other branch on a runner the coordinator picks then, and a source wired straight into a sink
goes directly, without the runner. `PlanBuilder` then builds the fragments exactly as for a placement
chosen by hand. Nothing of the plan is saved: deploying a pipeline arms it at the commit the library
holds, with its bindings, and each run plans from that job and those bindings against the rights and
the nodes as they are then. The run's journal records the plan it ran.

**A binding is held to its brick by a mode.** `strict` needs the branch to be the brick, word for word.
In `contract`, `compatible` and `tolerant` the brick's owner may change it: saving the pipeline (or its
bindings) runs the designer's check and pins what it saw of each brick, its version and the contract it
produces or is handed. At the next launch, a brick still at its pinned version is `unchanged`; a changed
one is `tolerated` in `tolerant`, and otherwise its node judges it against the pinned contract
(`contract`: the same schema; `compatible`: a schema that still satisfies its consumers). A breaking
verdict, or a node that cannot tell, refuses the run and names the binding; a verdict is computed once
per version of the brick, and the run's journal keeps it.

**A data node only reads and writes, and runs only its bricks.** The coordinator sends a data node
the ids of its bricks, in order, and the version of the job they make; the node composes the job
from what its owner declared and refuses the fragment, at once and by name, when a brick changed
since the plan. A fragment is made of a source brick, a sink brick, the bare `input: arrow:-` end
of an edge, and relays of a branch onto an edge, and the planner refuses, naming why, any branch of
a data node that is none of them: everything between a read and a write belongs on a runner, which
hosts no data and runs what it is given. The **Lab view** places branches by hand, including reads
and writes no brick declares, so it needs the sandbox mode: `./lab.sh nodes sandbox` (the variable
`LAB_NODE_SANDBOX=1` in the host process's environment; the Nodes view shows a `raw fragments`
chip). A sandbox node accepts a job as written, which still may only read and write, through any
reader or writer. The switch belongs to the host's own environment, and only the Lab view's manual
planning ever sends a job as written.

**The library** is a git repository under `.state/library`, started from `library-seed/`: each
save and each save of bindings is a commit. Armed deployments are kept apart, in `.state/deployments`. A seed `<id>.yaml` brings its `<id>.bindings.json`, which binds each source and sink branch to a
node and a brick and a mode (`strict`: the branch must stay that brick, word for word; `contract`, `compatible`, `tolerant`: the brick may change, judged against what the last check pinned); the runner is never bound. Several pipelines may be deployed at once. Their runs are in
flight together when they share no data node; otherwise they wait their turn in the order they came
(the runner never counts). Finished runs are kept in `.state/runs/`.


## Lab view scenarios (`/lab/`)

| Pipeline | Shows | Try |
|---|---|---|
| `01-customers-anonymized` | a cut inside a branch, by `dtpipe split`: read on node-1, anonymized on the runner, loaded on node-4 | move the ✂ one stage later, after `fake`: the head keeps that transformer and node-1 refuses to run it |
| `02-revenue-by-country` | a three-source join on the runner, three inbound edges on one node | move `revenue` to node-3: the plan is fine and the node refuses to run a join |
| `03-order-history` | `--merge` of a DuckDB and a SQLite source, on the runner | read the source queries: each casts to the shared schema, dtpipe converts nothing |
| `04-slow-sensor-stream` | a 30-second throttled run | kill a fragment mid-run, cancel |

Every refusal the page shows is the coordinator's own: `PlanRegistry` for the flow matrix,
`ReplicaAdmission` for a node with no replica that can run a fragment, `RunOrchestrator` for the
cause of a failed run and the row counts of each edge.

A job header carries three lab directives among its comments: `# lab-cut: branch@stage` and
`# lab-place: unit=node` preload the page, `# lab-check: <SQL>` is what `smoke.py` compares
between the witness and the lab's warehouse.

## How it works

```
 browser ──HTTP / SSE──▶ DtPipe.Coordinator.Server :5180 (lab mode)
                          ├─ AddCoordinatorHub()  TransportR hub (bytes), replica registry, admission, runs
                          ├─ /node-channel        the control channel to the node hosts
                          ├─ /api                 bricks, design, library, plan, deploy, runs, events
                          └─ DeploymentManager    deployments, the run queue
                                  ▲                      ▲
          /node-channel (SignalR) │                      │ TransportR (Arrow IPC bytes)
      ┌─────────────┬─────────────┼─────────────┬────────┴────┬─────────────┐
      │ node-1      │ node-2      │ node-3      │ node-4      │ runner-1    │  AddDtPipeNode
      │ bricks; a run of a fragment here is a dtpipe child, relayed by the replica's client │
      └─────────────┴─────────────┴─────────────┴─────────────┴─────────────┘
```

**Planning** (`PlanBuilder`) turns a monolithic job into one fragment per node, in two steps; the
Lab view gives it cuts and a placement, the Library only the placement `AutoPlacer`
chose:

1. *Cuts inside a branch* are `dtpipe split --at k`, run by the node whose data the branch reads.
   The lab keeps both halves as dtpipe wrote them, so where each option lands, and the secret
   check, stay dtpipe's decisions.
2. *Regrouping whole branches* is the lab's own rewrite. A `from`/`ref` dependency that crosses two
   nodes becomes an `arrow:-` input branch on the consumer's side and an `arrow:-` output on the
   producer's — the shape `split` gives a cut. A branch that already writes somewhere, is also
   read locally, or feeds several nodes gets one relay branch (`from: x`, `output: arrow:-`) per
   remote reader. No option is edited.

The plan is then checked before anything is deployed: every fragment exchanges at least one edge
(a pipeline node completes only once all its edges are wired), every `${{VAR}}` it uses is hosted
by its node, and every edge passes the flow matrix (`IPlanRegistry`).

**Deploying** is the coordinator's: it asks every connected replica of each node whether it would run
the fragments of the plan (`Check`, which spawns nothing and leaves nothing on the node), and the
deployment is ready when every fragment has a replica that says so and that a run could be placed on.
A fragment is named `<pipeline>@<node>` and versioned by the SHA-256 of its job. **Running** is one
call to `IRunOrchestrator.RunAsync`: it places each fragment on a replica of its node (the
connected one with the fewest runs whose bricks compose the plan's version), launches it, and wires
the edges. A replica serves several runs, one dtpipe child each, and the run's journal says which
replica ran each fragment.

## Limits

- Everything runs on one machine, but nothing relies on it: the coordinator never opens a node's
  database. A cut, a brick preview and a library save's check are each read by the
  node that hosts the data; only `smoke.py`'s witness reads the seeded files directly.
- People sign in through `Lab.Idp`: `labadmin`, `labdesigner` and `labreader` (password: the name
  followed by `pwd`) hold the coordinator's three roles; any other name signs in with none. Tokens last 12 hours
  (the IdP's `Idp:AccessTokenLifetimeSeconds` changes it). Each node renews its token before it expires, so a
  deployment can sit idle, or run, past its tokens' lifetime. A node that cannot renew (the
  identity provider unreachable for longer than the renewal advance) is closed by the hub and its
  runs end.
- A transfer the hub refuses under the flow matrix shows in the run's verdict as an unresponsive
  fragment: the coordinator's log carries the refusal.
- One run at a time per data node: the runner's fragments of two runs share its host with no bound
  on either. The coordinator is not highly available: a restart loses every deployment and the runs
  in flight (the library and the finished runs of the run journal survive), and the node hosts keep
  running the lost runs' fragments until they are restarted. After a coordinator restart, restart the lab (`./lab.sh down`, then
  `./lab.sh up`); nothing reconstructs a run from the nodes reconnecting.
- A studio knows a source brick's columns, not those after a step: dtpipe offers no schema of
  a job's branch without running it.
- `lab.sh` needs bash. The node host itself uses no POSIX-only API, but the lab is not tested on
  Windows.
- `--compute` and the other JavaScript transformers retain memory per row; keep their volumes
  modest.

## API

Every route below needs the session cookie of a person with a role (reader for a `GET`, designer to
change a pipeline, deploy or run, admin for the rights and the lab-mode routes) and, unless it is a
`GET`, the header `X-Dtpipe-Csrf`. `/healthz` is anonymous; `/account/login` and `/account/logout`
sign a person in and out.

| Endpoint | |
|---|---|
| `GET /api/me`, `/api/info` | who is signed in and with which roles; whether the server runs in lab mode |
| `GET /api/nodes`, `/api/bricks`, `/api/flow-matrix`, `/api/pipelines`, `/api/state` | what the pages draw |
| `POST /api/bricks/{node}/{id}/preview` | a few rows, read by the node that owns the brick |
| `GET /api/library` · `GET`/`PUT`/`DELETE /api/library/{id}` | pipelines; `PUT {yaml, message}` commits what a check accepted: `dtpipe --dry-run 1` over one row per source brick sent by its node, then each sink brick's node checks its target (422 and nothing written otherwise) |
| `GET /api/library/{id}/history`, `/at/{commit}` | commits; the job and the diff at one |
| `PUT /api/library/{id}/bindings` | `{bindings: {alias: {node, brick?, mode?}}, message?}` → binds the bricks of the job; a check like a save's runs first (422 with its report); the server resolves a missing `brick` of a strict binding from the job's text; `mode` is `strict`, `contract`, `compatible` or `tolerant`, and the check writes the pins |
| `GET /api/library/{id}/plan` | the plan a launch would make now, with the runner it would use; nothing is written |
| `POST /api/library/{id}/deploy` | arms the pipeline at the commit the library holds, with its bindings (409 if the bindings do not hold or no node could run the plan) |
| `POST /api/plan` | Lab view: `{pipelineId, yaml, cuts, placement}` → units, fragments, edges, errors |
| `POST /api/deploy` | Lab view: the same body; plans, then deploys (409 if not deployable) |
| `GET /api/deployments` · `DELETE /api/deployments/{id}` | what is deployed; undeploy |
| `GET /api/rights` · `PUT /api/rights/matrix` | rights, the flow matrix |
| `POST /api/rights/identities` · `PUT`/`DELETE /api/rights/identities/{id}` · `POST …/{id}/reconnect` | identities; reconnect drops a node's control connection so it fetches a new token |
| `POST /api/rights/pending/{subject}/accept` (`{name, groups, description?}`) · `…/ignore` | a host waiting for approval: accept it as an identity, or set it aside for 24 hours |
| `POST /api/runs` · `GET /api/runs`, `/api/runs/queue` | `{pipelineId?}` queues a run; the journal; in flight and queued |
| `POST /api/runs/cancel` | `{runId?}`: a run in flight or queued; with no id, the one in flight (409 when several are) |
| `POST /api/fragments/{name}/kill` | fault injection: ends the fragment's child on every replica of its node whose host enabled it (every lab host does) |
| `GET /api/events` | server-sent events: nodes, deployments, logs, bytes, runs |
