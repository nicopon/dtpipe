# dtpipe distributed lab

A self-contained demonstrator for dtpipe's distributed pipelines: one coordinator and four
pipeline nodes, each with its own database, run as plain local processes (no container). A web
page shows a dtpipe job as a graph, lets you cut it, place each piece on a node, deploy the
fragments and run them, live.

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
open http://127.0.0.1:5180          # the page
./lab.sh smoke                      # every pipeline, distributed, against its witness
./lab.sh down
```

| Command | Does |
|---|---|
| `./lab.sh up` | builds the lab, seeds the databases on first use, starts the coordinator then the nodes |
| `./lab.sh status` | which processes run, and what the coordinator sees of each node |
| `./lab.sh smoke [id…]` | runs the end-to-end check, optionally on some pipelines only |
| `./lab.sh ui` | opens the page in headless Chrome, renders every pipeline, screenshots to `.state/ui/` |
| `./lab.sh seed` | rebuilds the databases (`LAB_SCALE=5` multiplies the row counts) |
| `./lab.sh logs` | follows every log |
| `./lab.sh down` / `reset` | stops everything / also deletes `.state/` |

`LAB_PORT` changes the port (default `5180`), `DTPIPE` the binary. Everything the lab writes lives
under `.state/`: databases, fragment jobs, plans, logs, build output.

## The four nodes

| Node | Group | Database | Holds |
|---|---|---|---|
| node-1 | `crm` | SQLite `crm.sqlite` | `customers` (20 000) |
| node-2 | `sales` | DuckDB `sales.duckdb` | `orders` (300 000, current year) |
| node-3 | `catalog` | SQLite `catalog.sqlite` | `products` (500), `legacy_orders` (100 000) |
| node-4 | `warehouse` | DuckDB `warehouse.duckdb` | nothing until a pipeline writes it |

`seed/seed.sh` builds them with dtpipe itself (`generate:`, `--fake`, a DuckDB `--sql`);
row-seeded fakes and hashed values make every seed identical. A node publishes its database as an
environment variable (`LAB_CRM_DB`, …) that its `dtpipe` children inherit, so a job names
`sqlite:${{LAB_CRM_DB}}` and never a path: the fragment runs against whichever node hosts it.

`flow-matrix.json` says which group may send to which. `crm` and `sales` may feed `catalog` and
`warehouse`, `catalog` may feed `warehouse`, and nothing leaves `warehouse`.

## Scenarios

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
                          └─ /api                 catalog, plan, deploy, run, query, events
                                  ▲                      ▲
                   /lab (SignalR) │                      │ TransportR (Arrow IPC bytes)
      ┌───────────────┬───────────┴───┬───────────────┬──┴────────────┐
      │ node-1        │ node-2        │ node-3        │ node-4        │  Lab.NodeHost
      │ PipelineNode ×n, one per fragment deployed here, each running a dtpipe child │
      └───────────────┴───────────────┴───────────────┴───────────────┘
```

**Planning** (`PlanBuilder`) turns a monolithic job into one fragment per node, in two steps:

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
- `PipelineNode` connects without TransportR groups, so every node shares one runtime group; the
  per-node matrix is enforced on the plan, not on each transfer.
- One deployed plan and one run at a time, as `RunOrchestrator` allows; no persistence across a
  coordinator restart.
- `lab.sh` needs bash. The node host itself uses no POSIX-only API, but the lab is not tested on
  Windows.
- `--compute` and the other JavaScript transformers retain memory per row; keep their volumes
  modest.

## API

| Endpoint | |
|---|---|
| `GET /api/nodes`, `/api/pipelines`, `/api/flow-matrix`, `/api/state` | what the page draws |
| `POST /api/plan` | `{pipelineId, yaml, cuts, placement}` → units, fragments, edges, errors |
| `POST /api/deploy` | the same body; plans, then deploys (409 if not deployable) |
| `POST /api/runs`, `/api/runs/cancel` | `{pins: {fragment: version}}` optional |
| `POST /api/fragments/{name}/kill`, `/variant` | fault injection |
| `POST /api/pipelines/from-command` | `{args: [...]}` → YAML through `--export-job` |
| `POST /api/query` | `{variable, sql}` → CSV of a node's database |
| `GET /api/events` | server-sent events: nodes, fragment states, logs, bytes, runs |
