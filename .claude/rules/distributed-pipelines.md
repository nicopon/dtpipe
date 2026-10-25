---
paths:
  - "src/DtPipe.Coordinator*/**"
  - "src/DtPipe.PipelineNode*/**"
  - "tests/DtPipe.Coordinator*/**"
  - "tests/DtPipe.PipelineNode*/**"
  - "deploy/**"
  - "samples/distributed-lab/**"
  - "src/DtPipe.Jobs/Coordinator/**"
  - "src/DtPipe/Cli/Node/**"
  - "tests/scripts/validate_distributed_boundary.sh"
---

# Distributed pipelines

The rules of the distributed family. The root `CLAUDE.md` holds the rules every task needs, and each
project of the family has a nested `CLAUDE.md` with its mechanics. Each rule ends with its guard, as
there.

## The projects

| Project | Role |
|---|---|
| `src/DtPipe.Coordinator` | Central hub of a distributed pipeline: the control plane over TransportR |
| `src/DtPipe.PipelineNode` | Runs one distributed-pipeline fragment under the coordinator's control |
| `src/DtPipe.Coordinator.Server` | The coordinator's server, a library a host adds (`AddCoordinatorServer`): API, pages, planner, library, rights |
| `src/DtPipe.Coordinator.Host` | The product's coordinator: a `Program` that adds the server and nothing else, the image's entry point |
| `src/DtPipe.Coordinator.Contracts` | Node channel messages, `BrickRules`: what coordinator and nodes share |
| `src/DtPipe.PipelineNode.Hosting` | `AddDtPipeNode`: makes any .NET host a node (bricks, fragments, tokens) |
| `src/DtPipe.PipelineNode.Launcher` | The assembly of `dtpipe node`: the CLI's only reach into the family, loaded when a node starts and by no other command |
| `src/DtPipe.PipelineNode.Runner` | The official runner: a node that hosts no data and runs any fragment |
| `src/DtPipe.PipelineNode.TrialNode` | The trial recipe's data node: two bricks declared in a file, a model of a team's own host |
| `deploy` | The Dockerfile of the coordinator's and the runner's images, and `deploy/README.md`: what a deployment provides |

The **distributed family** is every
project named `DtPipe.Coordinator*` or `DtPipe.PipelineNode*`: it references TransportR as NuGet
packages and stays outside `DtPipe.sln`, so `build.yml` builds only what `src/DtPipe` reaches through
`DtPipe.PipelineNode.Launcher` and tests none of it; `build.sh` discovers its test projects, so
`agentic-ci.yml` does.
`DtPipe.Coordinator` and `DtPipe.PipelineNode` are libraries; a host owns `Program`, authentication
and the address, and an executable of the family is such a host. Nothing else in `src/` names
TransportR; only the family, its tests and the lab do. The family references `DtPipe.Jobs` and no
other project of the solution, so none of its hosts runs the engine in its own process: every fragment runs as a `dtpipe` child. The lab under
`samples/distributed-lab` is a host, and it and its sample data are named nowhere else; its bench (the Lab view, planning
by hand, fault injection) is its own coordinator's, `Lab.Coordinator`, and nothing of it is under `src/`.
`[CI: validate_distributed_boundary.sh — names, references and project shape, not behaviour]`

## The rules

A distributed pipeline runs its fragments on pipeline nodes. A node is an identity carried by one or more
hosts, its **replicas**, each driving `dtpipe` child processes for the runs placed on it. The coordinator is
the control plane, over each replica's node channel; TransportR carries the data plane.
- **Peers never address each other**: only the coordinator opens a transfer, and it checks a plan's
  edges against the flow-control matrix before any opens.
- **A replica has one TransportR client at a time for all its runs, and is the only TransportR client they
  have.** The node is a byte relay: dtpipe reaches it through `arrow:` and references no TransportR
  assembly.
- **A node's replicas must be interchangeable**: one database, the same variables, the same bricks outside
  a rolling update. Nothing checks it; any eligible replica may run a fragment. `[unchecked]`
- **A fragment runs on a replica of its node's identity, and only that replica speaks for it.** Admission
  places each fragment on a replica connected under the subject of its node whose bricks compose the plan's
  version and whose TransportR client holds exactly the node's groups. The replica is chosen once for the
  run and never changed: one that is lost ends the runs placed on it, and none resumes on another. The run's
  result names the replica of each fragment. The coordinator knows a message's sender by its connection
  alone, so an `Exited` from any other replica, of another identity or of the same, is not heard.
- **Cancel by stream rupture, never by signal**: the node must run on Windows.
- **A node host decides what it runs**: its `FragmentPolicy` comes from its own configuration, never
  from a message the coordinator sends. So does whether it ends a run's child (`FaultInjection`): a host
  that did not enable it answers the coordinator's `Kill` with `0` and logs why.
- **A person never reaches the data plane, a service never `/api`**: told apart by the claims of a
  token the issuer made from the identity's kind, whichever issuer (a service's `sub` is its `client_id`, a person's `client_id`
  the studio's), each scheme failing the other's tokens. `dtpipe
  studio` reaches `/api` with a person's token, through the routes and records of
  `DtPipe.Jobs/Coordinator`, whose contract number both sides check. A service has a kind the rights
  give it: a node announces on the node channel, a **monitor** (a supervisor) reads `/health` and nothing
  else, a **launcher** (a scheduler) starts the runs of the pipelines its identity lists and reads them, on
  `/service` and nowhere else; the last two have no group and no node, so a valid token of theirs is refused at
  the announcement and on `/api`, and neither reaches the other's route.
- **The product's images carry the revision they were built from and run unprivileged.** `deploy/Dockerfile` takes `SOURCE_REVISION`
  (the full commit id): a value given is refused if malformed or if a `dtpipe` or host does not report it, and labels the image
  with it (`org.opencontainers.image.revision`); left out, the build warns and the label says `unknown`. `.git` is not in the
  build context, so nothing else can supply it. The lab's stack is the strict case: its guard requires HEAD. Every image
  runs as `1654:1654` with `/state` owned by that user. The release pushes the coordinator and the runner only, to GHCR, multi-arch, tagged with
  the version, its major.minor and `latest`; the trial recipe's data node is built by `deploy/compose.trial.yaml`, never pushed. The lab's images extend these and the product's names no lab; `deploy/`
  is scanned like `src/`. **The server listens in plain HTTP and reads no forwarded header**: a TLS proxy in front is
  told to it by `Coordinator:PublicUrl`, the address people reach it at, so it does not turn into `http://` return addresses.
  `[CI: validate_distributed_boundary.sh — deploy/ names no lab]` `[local: PublicUrlTests; the lab's smoke.py bricks - versions; the lab's tools/linux_replay.sh uid, a named volume of another uid]`
  `[unchecked: the release's push to GHCR, until a tag runs it; a Linux Docker host, where tools/linux_replay.sh is the replay; a real TLS proxy; a bind mount of another uid; deploy/README.md is in no documentation check, so keep its lines to 100 columns]`
- **A key is never stored, logged, returned or compared but as its hash, and the hash never leaves the store.** The call that
  makes a key is the only answer that carries it, and a key the deployment gives (`Coordinator:Bootstrap`) is read once and hashed;
  the rights keep its SHA-256, which no route answers, and no log carries the key, a part of it or its hash. A presented key is
  judged by that hash, in constant time, never by the client id it names; `/connect/token` and the sign-in form
  (`POST /account/login`) are the only routes that take one in. `[local: Server.Tests IdentityKeyRoutesTests, IdentityKeyTests,
  KeyIssuerTests, PersonKeyTests, KeyOnlyCoordinatorTests — the logs of a bootstrapped start and of its keys at work]`
  Detail: `src/DtPipe.Coordinator.Server/CLAUDE.md`.
- **No page offers to copy a command that holds a key.** The Access page is the only one that asks for a key: it shows it once, in
  the one element its Copy button copies, which closing the window empties; every command it shows names a variable or a reference
  the person fills (`--client-secret '${{VAR}}'`, `keyring://`, `--key '${{VAR}}'`), never the key, and nothing of a page keeps it in
  storage or in the address. `[local: Server.Tests KeyWindowTests — the pages' text, not their behaviour]`
- **A cursor is the coordinator's: read when a run starts, kept only at the verdict of the whole run.** One cursor per branch that
  writes, keyed by (library pipeline, alias), kept by `CursorKeeper` in the store that holds its state. The child never reaches the
  coordinator or a file a brick names: the node gives each sink branch that sets `cursor` a state file of its own, reads it once the
  child exits with 0 and sends it in `Exited`; the coordinator hands the kept value to the fragment holding the one source upstream of
  the sink as `DTPIPE_CURSOR_OVERRIDE`. A failed, cancelled or interrupted run advances nothing. A cursor is tied to the versions of its
  sink brick (which holds the column) and its source brick: a change refuses the run by name. A launch beside a run of the same
  pipeline is refused, never queued, once a cursor exists. A value is substituted into the source's SQL, so `CursorValues` is the one
  rule the node, the coordinator and the route apply. `BrickRules` admits `cursor` on a sink, never `state`, never `cursor` on a source
  (the half of a `dtpipe split` cut that reads advances a cursor before the sink has written), and refuses a `cursor://` token that names a
  file. `[local: Server.Tests CursorRunTests, CursorRoutesTests, CursorRulesTests; Hosting.Tests FragmentCursorsTests; validate_doc_examples.sh]`
  `[unchecked: an older node, which ignores the value it is handed]` Detail: `src/DtPipe.Coordinator.Server/CLAUDE.md`.
- **The coordinator issues the tokens of the identities a key verifies, and trusts them by their issuer like any other.**
  Its issuer is `Coordinator:PublicUrl`, beside an external provider (another issuer) or alone. One grant, client credentials, the key as the client secret: every refusal of
  the client reads the same. The record's kind makes the token: a service's lasts `Coordinator:Auth:ServiceTokenLifetime`; a
  **person**'s (an identity of kind `person`, whose role the rights hold) is the studio's client's, names the key it was
  exchanged with and lasts `Coordinator:Auth:PersonTokenLifetime`, and it and the session the sign-in form opens are judged
  against the rights on every request, so a revoked key ends both at the next one. `Coordinator:Auth:PersonKeys=false` refuses
  people a key by name. The exchange is refused while
  `PublicUrl` is plain HTTP unless `Coordinator:Auth:AllowHttpKeys`, which the log and every page then name. **A token opens
  an identity only when its issuer is the one that verifies it**: the key issuer's a `key` identity, the provider's any
  other. The key issuer's tokens pass the same checks with the signing keys of the store, never through a request to its own
  address. **Without a provider** (`Coordinator:Authority` unset) it is the only issuer the server names and trusts: the
  provider's schemes are not registered, an identity of the provider is refused when it is added and at start, and a start
  needs `PublicUrl`, a way to exchange a key and `Coordinator:Bootstrap:AdminKey`. **The server makes no key**: what
  `Coordinator:Bootstrap` names (the first administrator's key, identities with theirs, each `@path` or a value of the
  environment, never of the command line) is added at start where the rights lack it, in one change the audit names
  `Coordinator:Bootstrap`, and never replaces an identity. The product's recipe (`deploy/compose.yaml`) has a one-off service
  write its keys into a volume that the coordinator and the runner read by `@path`. `[local: Server.Tests KeyIssuerTests,
  PersonKeyTests, KeyOnlyCoordinatorTests; Hosting.Tests KeyTokenTests]` `[unchecked: a real
  TLS proxy; two instances that bootstrap at the same first start; deploy/compose.yaml, until a Docker host runs it]` Detail:
  `src/DtPipe.Coordinator.Server/CLAUDE.md`.
- **The CLI hosts a node only through one assembly, and never a hub.** `dtpipe` is the child a node drives, reached through
  `arrow:`; `dtpipe node` is a node whose bricks are the branches of the pipeline grammar (`src/DtPipe/CLAUDE.md`). The
  ordinary CLI references no TransportR assembly: only `DtPipe.PipelineNode.Launcher` carries it, and `Cli/Node/NodeHostEntry.cs`
  is the one file that names it, so the runtime loads it when `dtpipe node` runs and for no other command. The CLI references no
  other project of the family, maps no SignalR hub, and `dtpipe studio` is a client of the coordinator's `/api`, only when it is
  given `--coordinator`. A node started by the CLI takes no setting from a message and no secret from an argument: the client
  secret is a reference (`keyring://`, `${{NAME}}`) read once at start.
  `[CI: validate_distributed_boundary.sh — names and references, one file naming the Launcher, no SignalR hub under src/DtPipe;
  NodeCommandTests in DtPipe.Tests — no TransportR loaded after --version, --help or node --help]`
  `[local: DtPipe.PipelineNode.Tests NodeCommandTests — a node started by the command announces its bricks and shows its secret nowhere]`
  `[unchecked: a listener of the CLI that is no SignalR hub]`

`[local: DtPipe.Coordinator.Tests, DtPipe.PipelineNode.Tests, DtPipe.Coordinator.Server.Tests, the lab's
smoke.py]`
`[unchecked: Windows]` Detail: `src/DtPipe.Coordinator/CLAUDE.md`, `src/DtPipe.PipelineNode/CLAUDE.md`,
`src/DtPipe.PipelineNode.Hosting/CLAUDE.md`.
