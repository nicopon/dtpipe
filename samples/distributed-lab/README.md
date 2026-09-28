# dtpipe distributed lab

A self-contained demonstrator for dtpipe's distributed pipelines: one coordinator, four data
nodes each with its own database, and a runner, run as plain local processes (no container).
Every node authenticates with the coordinator's own identity provider, which decides its group.

Each data node offers **bricks**: reads and writes its owner preconfigured on its database. The
coordinator's page assembles bricks and processing steps into a pipeline by drag and drop, keeps
it in a git-backed **library**, places every brick on its node and every other step on the
runner, deploys the fragments and runs them, live. A second page, the **Lab view**, cuts and
places a job by hand and injects faults.

The lab **uses** dtpipe and changes nothing in it. It drives the `dtpipe` binary like a user
(`split`, `--export-job`, `--job`) and hosts `DtPipe.Coordinator` and `DtPipe.PipelineNode` as
libraries, exactly as their own tests do. It sits outside `DtPipe.sln`: CI never builds it.

## Quick start

Prerequisites: the .NET 10 SDK, `python3`, a bash shell, the TransportR `0.1.0` packages in your
NuGet cache (the same requirement as `DtPipe.Coordinator`), and a built binary:

```bash
./build.sh                          # at the repository root: produces dist/release/dtpipe
cd samples/distributed-lab
./lab.sh up                         # build, seed the four databases, start everything
open http://127.0.0.1:5180          # the coordinator's page; the Lab view is /lab.html
./lab.sh smoke                      # every pipeline, distributed, against its witness
./lab.sh down
```

| Command | Does |
|---|---|
| `./lab.sh up` | builds the lab, seeds the databases on first use, starts the coordinator then the nodes |
| `./lab.sh status` | which processes run, and what the coordinator sees of each node |
| `./lab.sh smoke [lab\|library\|rights] [id…]` | runs the end-to-end check, optionally one pass or some pipelines only |
| `./lab.sh ui` | drives both pages in headless Chrome, a designer scenario included; screenshots to `.state/ui/` |
| `./lab.sh seed` | rebuilds the databases (`LAB_SCALE=5` multiplies the row counts) |
| `./lab.sh logs` | follows every log |
| `./lab.sh down` / `reset` | stops everything / also deletes `.state/` |

`LAB_PORT` changes the port (default `5180`), `DTPIPE` the binary. Everything the lab writes lives
under `.state/`: databases, fragment jobs, plans, logs, build output.

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
row-seeded fakes and hashed values make every seed identical. A node publishes its database as an
environment variable (`LAB_CRM_DB`, …) that its `dtpipe` children inherit, so a job names
`sqlite:${{LAB_CRM_DB}}` and never a path: the fragment runs against whichever node hosts it.

A node's group is not in its file: the coordinator's IDP gives it (below). The flow matrix says
which group may send to which: every data group may feed the `runner`, the `runner` feeds
`warehouse`, and the direct links the Lab view's scenarios use stay.

## Rights and the identity provider

The coordinator carries its own IDP, after TransportR's SimpleIdp: OpenIddict, the
client-credentials flow, `POST /connect/token`. Each node host holds a client id and secret in its
`nodes/<node>.json`; the token it obtains names it (`sub`) and carries its group as the
`transportr:group:<group>` scope, which TransportR's hub reads. What is allowed lives in one
document, `.state/rights.json`, started from `rights-seed.json` and edited in the **Access** view:

| | Enforced |
|---|---|
| **Who**: identities, each bound to one group and to the one node it may announce | by the IDP (no token for an unknown or disabled client) and by the control channel (an announcement of another node is refused) |
| **To whom**: the flow matrix between groups | by the hub on **every transfer**, live, and on every plan |
| **What**: per source brick, the groups its rows may be sent to as they leave their node | on every plan (the hub sees groups, not bricks) |

Every change is audited. A pipeline node has no credential option of its own, so its host hands
it a hub URL carrying the token in its path (`/t/<token>/…`); the coordinator turns it back into a
bearer header. Signing keys are ephemeral: a coordinator restart invalidates every token, and the
hosts fetch new ones as they reconnect.

## The coordinator's page

| View | Does |
|---|---|
| Nodes | every node, its bricks (schema, version, preview), the fragments it hosts, the flow matrix |
| Library | the pipelines kept, their plan and deployment state, last run, git history, diff, restore, import (YAML or a command line) |
| Designer | a palette of bricks and steps (Transform, SQL, Merge), a canvas wired port to port, an inspector per card, the composed YAML to copy or edit |
| Distribution | the plan placed automatically, drawn in lanes (sources, runner, sinks); save it, deploy, run |
| Runs | deployments, the run in flight and the queue, the journal with each run's report, node events, a query on any database |
| Access | the IDP, identities (group, bound node, enabled, reconnect), the editable flow matrix, brick policies, the audit |

**A pipeline is a plain dtpipe job**, one branch per card, that also runs whole on one machine
(`smoke.py` uses it as the witness). A branch *is* a brick when its content equals the brick's:
no reference in the YAML, and a brick its owner changes stops matching. Importing a job takes
each branch apart around the bricks it contains (a branch reading a brick, transforming, then
writing a brick becomes three cards).

**Distribution is automatic**: a brick runs on its node, every other branch on the runner, and a
source wired straight into a sink goes directly, without the runner. `PlanBuilder` then builds
the fragments exactly as for a placement chosen by hand. A saved plan records the hash of the job
it came from; a job changed since is refused at deployment until it is distributed again.

In the designer, a wire is moved or removed by its input end: drag it off the input port and drop
it on another input, or anywhere else to remove it. A selected wire also shows a × to remove it.

**The library** is a git repository under `.state/library`, started from `library-seed/`: each
save and each saved plan is a commit. Several pipelines may be deployed at once; their runs wait
in one queue, since the coordinator runs one at a time. Finished runs are kept in `.state/runs/`.


## Lab view scenarios (`/lab.html`)

| Pipeline | Shows | Try |
|---|---|---|
| `01-customers-anonymized` | a cut inside a branch, by `dtpipe split` | move the ✂: the personal columns cross the network or not |
| `02-revenue-by-country` | a three-source join, three inbound edges on one node | move `revenue` to node-3 (products stay local), then to node-2 (refused) |
| `03-order-history` | `--merge` of a DuckDB and a SQLite source | read the source queries: each casts to the shared schema, dtpipe converts nothing |
| `04-slow-sensor-stream` | a 30-second throttled run | kill a fragment mid-run, cancel, add a misaligned instance, pin a version |

Every refusal the page shows is the coordinator's own: `PlanRegistry` for the flow matrix,
`AdmissionGate` for a misaligned instance or an unknown pinned version, `RunOrchestrator` for the
cause of a failed run and the row counts of each edge.

A job header carries three lab directives among its comments: `# lab-cut: branch@stage` and
`# lab-place: unit=node` preload the page, `# lab-check: <SQL>` is what `smoke.py` compares
between the witness and the lab's warehouse.

## How it works

```
 browser ──HTTP / SSE──▶ Lab.Coordinator :5180
                          ├─ AddCoordinatorHub()  TransportR hub: registry, admission, runs, bytes
                          ├─ /lab                 the lab's control channel to the node hosts
                          ├─ /api                 bricks, design, library, plan, deploy, runs, events
                          └─ DeploymentManager    deployments, the run queue, re-arming
                                  ▲                      ▲
                   /lab (SignalR) │                      │ TransportR (Arrow IPC bytes)
      ┌─────────────┬─────────────┼─────────────┬────────┴────┬─────────────┐
      │ node-1      │ node-2      │ node-3      │ node-4      │ runner-1    │  Lab.NodeHost
      │ bricks, and a PipelineNode per fragment deployed here, each running a dtpipe child │
      └─────────────┴─────────────┴─────────────┴─────────────┴─────────────┘
```

**Planning** (`PlanBuilder`) turns a monolithic job into one fragment per node, in two steps; the
Lab view gives it cuts and a placement, the Distribution view only the placement `AutoPlacer`
chose:

1. *Cuts inside a branch* are `dtpipe split --at k`. The lab keeps both halves as dtpipe wrote
   them, so where each option lands, and the secret check, stay dtpipe's decisions.
2. *Regrouping whole branches* is the lab's own rewrite. A `from`/`ref` dependency that crosses two
   nodes becomes an `arrow:-` input branch on the consumer's side and an `arrow:-` output on the
   producer's — the shape `split` gives a cut. A branch that already writes somewhere, is also
   read locally, or feeds several nodes gets one relay branch (`from: x`, `output: arrow:-`) per
   remote reader. No option is edited.

The plan is then checked before anything is deployed: every fragment exchanges at least one edge
(a pipeline node completes only once all its edges are wired), every `${{VAR}}` it uses is hosted
by its node, and every edge passes the flow matrix (`IPlanRegistry`).

**Deploying** sends each fragment's YAML to its node host, which writes it and opens a
`PipelineNode` for it; the fragment registers with the coordinator under the name
`<pipeline>@<node>`, versioned by the SHA-256 of its YAML. **Running** is one call to
`IRunOrchestrator.RunAsync`. A `PipelineNode` serves one run, so the lab re-arms every fragment
afterwards (a fresh instance each) before it accepts the next run.

## Limits

- Everything runs on one machine, which the lab uses as a shortcut: `split` samples the sources
  and the query panel opens a node's file from the coordinator's process.
- The page and `/api` are not authenticated: only the nodes are. Tokens last 12 hours; a
  deployment idle longer than that must be redeployed.
- A transfer the hub refuses under the flow matrix shows in the run's verdict as an unresponsive
  fragment: the coordinator's log carries the refusal.
- One run at a time, as `RunOrchestrator` allows: runs queue. Deployments do not survive a
  coordinator restart; the library and the run journal do.
- Cancelling a run reaches a fragment's outbound relay only on its next write: a fragment whose
  child emits late (a SQL join over a slow source) holds the verdict until then.
- The designer knows a source brick's columns, not those after a step: dtpipe offers no schema of
  a job's branch without running it.
- `lab.sh` needs bash. The node host itself uses no POSIX-only API, but the lab is not tested on
  Windows.
- `--compute` and the other JavaScript transformers retain memory per row; keep their volumes
  modest.

## API

| Endpoint | |
|---|---|
| `GET /api/nodes`, `/api/bricks`, `/api/flow-matrix`, `/api/pipelines`, `/api/state` | what the pages draw |
| `POST /api/bricks/{node}/{id}/preview` | a few rows, read by the node that owns the brick |
| `POST /api/design/compose`, `/decompose`, `/validate` | cards → job, job → cards, `dtpipe --dry-run 1` over the job |
| `GET /api/library` · `GET`/`PUT`/`DELETE /api/library/{id}` | pipelines; `PUT {yaml, layout, message}` commits |
| `GET /api/library/{id}/history`, `/at/{commit}` | commits; the job, layout and diff at one |
| `POST /api/library/{id}/distribute` | `{save?, message?}` → the automatic plan, saved as a commit on request |
| `POST /api/library/{id}/deploy` | deploys the saved plan (409 if the job changed since) |
| `POST /api/plan` | Lab view: `{pipelineId, yaml, cuts, placement}` → units, fragments, edges, errors |
| `POST /api/deploy` | Lab view: the same body; plans, then deploys (409 if not deployable) |
| `GET /api/deployments` · `DELETE /api/deployments/{id}` | what is deployed; undeploy |
| `POST /connect/token` | the embedded IDP: `grant_type=client_credentials`, `client_id`, `client_secret` |
| `GET /api/rights` · `PUT /api/rights/matrix` · `PUT /api/rights/bricks/{node}/{id}` | rights, the flow matrix, a brick's policy (`{groups}` or `null`) |
| `POST /api/rights/identities` · `PUT`/`DELETE /api/rights/identities/{id}` · `POST …/{id}/reconnect` | identities; reconnect drops a node's control connection so it fetches a new token |
| `POST /api/runs` · `GET /api/runs`, `/api/runs/queue` | `{pipelineId?, pins?}` queues a run; the journal; in flight and queued |
| `POST /api/runs/cancel` | `{runId?}`: the run in flight, or a queued one |
| `POST /api/fragments/{name}/kill`, `/variant` | fault injection |
| `POST /api/pipelines/from-command` | `{args: [...]}` → YAML through `--export-job` |
| `POST /api/query` | `{variable, sql}` → CSV of a node's database |
| `GET /api/events` | server-sent events: nodes, fragment states, logs, bytes, runs |
