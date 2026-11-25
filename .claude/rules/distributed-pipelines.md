---
paths:
  - "src/DtPipe.Coordinator*/**"
  - "src/DtPipe.PipelineNode*/**"
  - "tests/DtPipe.Coordinator*/**"
  - "tests/DtPipe.PipelineNode*/**"
  - "deploy/**"
  - "distributed-lab/**"
  - "src/DtPipe.Jobs/Coordinator/**"
  - "src/DtPipe/Cli/Node/**"
  - "tests/scripts/validate_distributed_boundary.sh"
  - "tests/scripts/validate_runner_egress.sh"
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
| `src/DtPipe.Coordinator.Host` | The product's coordinator: a `Program` that adds the server and nothing else, the image's entry point, and `dtpipe-coordinator`, the executable `build.sh` publishes |
| `src/DtPipe.Coordinator.Contracts` | Node channel messages, `BrickRules`: what coordinator and nodes share |
| `src/DtPipe.PipelineNode.Hosting` | `AddDtPipeNode`: makes any .NET host a node (bricks, fragments, tokens), `AddArrowSource`: a source of the host's own. The family's one package |
| `src/DtPipe.PipelineNode.Launcher` | The assembly of `dtpipe node` (a data node, or the runner with `--runner`): the CLI's only reach into the family, loaded when a node starts and by no other command |
| `deploy` | The Dockerfile of the coordinator's and the runner's images, the runner's entry point (`runner-entrypoint.sh`: its network rules), and `deploy/README.md`: what a deployment provides |

The **distributed family** is every
project named `DtPipe.Coordinator*` or `DtPipe.PipelineNode*`: it references TransportR as NuGet
packages and stays outside `DtPipe.sln`, so the solution builds only what `src/DtPipe` reaches through
`DtPipe.PipelineNode.Launcher`; `run_unit_tests.sh` discovers the family's test projects, so `build.sh`
and the test job of `build.yml` and of `release.yml` run them. `build.sh` also publishes the coordinator self-contained as the folder
`dist/release/coordinator/` (executable `dtpipe-coordinator`, set by the Host's `AssemblyName`, never by a
command-line `-p:AssemblyName`) and starts it once, on a throwaway state, until `/livez` answers. It is a
folder, never a single file: `wwwroot` and the native SQLite library sit beside the executable. Its
library is a git repository, so the machine it runs on needs `git` on the `PATH`. `release.yml` attaches the same folder to each release as `dtpipe-coordinator-<platform>`
(`.tar.gz`, `.zip` on Windows) `[unchecked: until a tag runs it]` `[local: build.sh — the coordinator's publish and its /livez answer]`.
`DtPipe.Coordinator` and `DtPipe.PipelineNode` are libraries; a host owns `Program`, authentication
and the address, and an executable of the family is such a host. Nothing else in `src/` names
TransportR; only the family, its tests and the lab do. The family references `DtPipe.Jobs` and no
other project of the solution, so none of its hosts runs the engine in its own process: every fragment runs as a `dtpipe` child. The lab under
`distributed-lab` is a host, and it and its sample data are named nowhere else; its bench (the Lab view, planning
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
  a rolling update, the same protection of the data plane. Any eligible replica may run a fragment. The protection is compared:
  each replica announces its level and the id of its key (a fingerprint the node derives, never the key), and replicas that
  differ are flagged in the coordinator's log and by the node's API, never refused, since a rolling update passes through that.
  `[CI: Coordinator.Tests DataPlaneProtectionWireTests; Server.Tests NodeChannelHubTests; Hosting.Tests EndToEndKeyTests;
  PipelineNode.Tests EndToEndEncryptionTests]` `[unchecked: the rest]`
- **A fragment runs on a replica of its node's identity, and only that replica speaks for it.** Admission
  places each fragment on a replica connected under the subject of its node whose bricks compose the plan's
  version and whose TransportR client holds exactly the node's groups. The replica is chosen once for the
  run and never changed: one that is lost ends the runs placed on it, and none resumes on another. The run's
  result names the replica of each fragment. The coordinator knows a message's sender by its connection
  alone, so an `Exited` from any other replica, of another identity or of the same, is not heard.
  **Eligibility is a fragment's, joining is an edge's.** TransportR has no downgrade, so the two ends of
  every edge must protect the data plane alike: one level, and one key id where both name one. That
  relation is not transitive, so admission chooses the replicas of a run together, in its order of
  preference among the choices that join every edge, and refuses a run no choice joins before anything is
  launched, naming an edge and what each end does. Arming, and the registration of a plan fixed by hand,
  refuse an edge whose two nodes each have a connected replica when no pair of them joins; a node with
  none is left to admission. `[CI: Coordinator.Tests EdgeProtectionTests, ReplicaAdmissionSubjectTests;
  Server.Tests ArmedDeploymentTests]`
- **The plan of a pipeline says how each of its edges is protected, by the one rule of joining, and a page never judges it.**
  `EdgeProtection.Judge` is that rule applied to the replicas connected now (`ReplicaAdmission.EdgeJudgments`); `EdgeRefusals` is
  its refusals, so the sentence arming gives and the plan's field are one judgment, never two. `GET /api/library/{id}/plan`
  carries it beside `plan` (`Launch.EdgeProtections`, an `EdgeJudgment` per edge): the two fragments, the level the edge would be
  held at (the weakest level is a level like another), and the refusal. An edge with neither is unknown, never in clear: an end with no offer
  (no connected replica, or every one below the required protection) or ends that join at several levels. The edges of a plan
  the planner refuses are not judged. The judgment holds words, key fingerprints and fragment names, never a key. A page draws
  the judgment: it compares no two announced levels and reads no cause's sentence to learn which edge it names. `POST /api/edges/protections`
  gives the same judgment of a job text the library need not hold, which `dtpipe studio` shows before a publication and never acts on.
  `[CI: Coordinator.Tests EdgeJudgmentTests; Server.Tests LaunchRoutesTests]` `[unchecked: a page that judges an edge itself]`
- **A deployment may require a protection of the data plane, and a replica below it is visible and eligible for nothing.**
  `Coordinator:DataPlane:RequiredProtection` is a floor on the level a replica announces, ordered by `ProtectionPolicy`, which is also
  the one reading of the setting (a text that is no word stops the start); unset, it requires nothing. A replica
  below it stays connected and admitted as a replica, never refused or cut at its announcement, since a rolling update passes through
  it: it is listed as under the policy by the node's API and the log, told once per level it announces, and refused by
  `ReplicaChooser.ProtectionRefusal` for every fragment, a runner's included, by the paths eligibility already has. Arming does not
  count it as an end of an edge, so a node whose connected replicas are all below it reads as a node with no eligible replica, never
  as an edge that cannot join. The policy judges the level and no key or fingerprint: whether two ends hold the same key stays the
  edge's. `[CI: Coordinator.Tests RequiredProtectionTests; Server.Tests RequiredProtectionSettingTests, NodeChannelHubTests,
  ArmedDeploymentTests]`
- **A run reports the protection each end of each edge applied, as its own client reports it, and an absence reads unknown.** A node
  reads `TransferInfo.Protection` from its TransportR client when it wires an edge, while the client lists the transfer, and sends it by
  alias in `FragmentExit.EdgeProtections`, in the words of `DataPlaneProtections` read through the one table `EndToEndKey` also announces
  with; never from its setting or its announcement, which the client may not apply. The coordinator keeps each end apart
  (`EdgeCount.ProducerProtection`, `ConsumerProtection`, looked up by each end's own alias like the row counts, never one level made of
  both) in the run's result, which the journal and the run routes carry. An end that reported nothing (an older node, a transfer the
  client no longer lists, a fragment that wired nothing, a text that is no word) is null: never the level that seals nothing and never
  the level the replica announced. The field carries a word and no key, fingerprint or path, and the coordinator reports it and judges
  nothing by it. `[CI: PipelineNode.Tests EndToEndEncryptionTests, CoordinatorDrivenTests; Hosting.Tests EndToEndKeyTests;
  Coordinator.Tests ScriptedRunTests, EdgeProtectionReportWireTests; Server.Tests RunJournalTests, StateWireTests]`
- **An edge an end of which failed end-to-end encryption is the run's cause, read from the category each end reports, and
  reports without one give the verdict the rule gives without categories.** A node keeps, by alias, the category its own client gave each transfer's
  failure (`TransferFailedException.Category`, never its message) and sends it in `FragmentExit.EdgeFailures`, in the words of
  `TransferFailures` through the one table `TransferFailureWords`; a category the table does not know, and a failure that has none,
  leave no entry. The coordinator keeps only those words and reads them only where every fault is a consequence of a transfer:
  the first edge of the run an end of which failed the encryption is the cause, as `producer -> consumer`, with every failed fragment
  its consequence, and else the receiver of an edge whose sender had no acknowledgement. A cancellation, a silence, a wiring failure
  and a fragment's own fault are judged before them, and such a run stays a failure. `RunResult.CauseKind` (`CauseKinds`) says what
  the cause names, so the journal reads the reason from it rather than guess; a result kept without it is read by its reports. An edge the
  hub could not open adds the word of the hub's category, a refusal none. No exception's text decides a cause, and none enters
  `EdgeFailures` or `CauseKind`.
  `[CI: PipelineNode.Tests EndToEndEncryptionTests, CoordinatorDrivenTests; Coordinator.Tests EdgeFailureVerdictTests,
  EdgeFailureReportWireTests, ScriptedRunTests; Server.Tests RunJournalTests, StateWireTests]`
- **Cancel by stream rupture, never by signal**: the node must run on Windows.
- **A runner hosts no data: a runner's fragment reads and writes its `arrow:` edges and nothing else.** The fragments of every
  run share the runner's host, so a file, a database, a checkpoint or a cursor file it reached would carry rows between two
  flows the coordinator never judged together. `RunnerEndpoints` (`DtPipe.Coordinator.Contracts`) is the one rule: every input
  and output is `arrow:-` on an edge of its alias and direction, no branch names a place on disk, no `duck-init`; processors and
  transformers stay. The planner refuses a fragment that breaks it, naming the branch and the endpoint, and the runner refuses
  it again before it launches one (`FragmentIntake`). The engine enforces it a third time: a runner's child runs under
  `--edges-only` (`src/DtPipe/CLAUDE.md`), which refuses any other reader or writer, runs DuckDB without external access and
  reads no file and no variable of the host; the node starts it with the host's environment cut to what `EdgesOnlyEnvironment`
  keeps, and none of the node's variables.
  Every fragment's child works with a run directory of its own as its temp directory, removed once it has ended; a runner's
  child also works in it. `[CI: Server.Tests PlanRightsTests; Hosting.Tests FragmentIntakeTests; PipelineNode.Tests
  FragmentIsolationTests; DtPipe.Tests EdgesOnlyRuleTests, DuckDBSqlProcessorEdgesOnlyTests]` `[unchecked: Windows]`
- **A runner of the rights is open without a row, and only the rights say who is one.** A node identity flagged `Runner` hosts no
  data and serves every group: `RightsStore.Allowed` allows any flow to or from a group it holds, and the closure of origins
  still decides what crosses it. The flag is an administrator's word kept in the rights, never a host's announcement, so a group of
  a runner belongs to runners alone (refused on every write and every load) and a host that announces data under such an identity
  is refused. A runner not flagged keeps its explicit rows. `[CI: Server.Tests RunnerFlowTests, NodeChannelHubTests,
  MatrixEdgesTests]` `[unchecked: the page's checkbox; the lab's runner is not flagged, so its explicit rows stay replayed]`
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
  the announcement and on `/api`, and neither reaches the other's route. A designer still reads rows: a brick's sample
  and preview, of any brick whatever the flow matrix says of its node, through the control plane and logged by caller,
  node and brick (`src/DtPipe.Coordinator.Server/CLAUDE.md`); the matrix judges transfers, never a person.
- **The product's images carry the revision they were built from and run unprivileged.** `deploy/Dockerfile` takes `SOURCE_REVISION`
  (the full commit id): a value given is refused if malformed or if a `dtpipe` or host does not report it, and labels the image
  with it (`org.opencontainers.image.revision`); left out, the build warns and the label says `unknown`. `.git` is not in the
  build context, so nothing else can supply it. The lab's stack is the strict case: its guard requires HEAD. Every process that
  serves or runs a pipeline runs as `1654:1654`, with `/state` owned by that user. The release pushes the coordinator and the runner only, to GHCR, multi-arch, tagged with
  the version, its major.minor and `latest`. The lab's images extend these and the product's names no lab; `deploy/`
  is scanned like `src/`. **The server listens in plain HTTP and reads no forwarded header**: a TLS proxy in front is
  told to it by `Coordinator:PublicUrl`, the address people reach it at, so it does not turn into `http://` return addresses.
  `[CI: validate_distributed_boundary.sh — deploy/ names no lab]` `[CI: PublicUrlTests] [local: the lab's smoke.py bricks - versions; the lab's tools/linux_replay.sh uid, a named volume of another uid]`
  `[CI: build.yml and release.yml — their test job runs run_unit_tests.sh; images needs the release's]` `[unchecked: the release's push to GHCR, until a tag runs it; a Linux Docker host, where tools/linux_replay.sh is the replay; a real TLS proxy; a bind mount of another uid; deploy/README.md is in no documentation check, so keep its lines to 100 columns]`
- **The lab's stack is replayed on PostgreSQL, and on SQLite when SQLite's own ground changes.** A change to the server, the images
  or the stack ends with `lab.sh stack up`, `smoke` and `restart-check` on the stack's default store, PostgreSQL. A change to
  `src/DtPipe.Coordinator.Server/Stores/**`, to `DtPipe.Coordinator.Server.csproj` or to `deploy/Dockerfile` replays it on SQLite
  too (`LAB_STACK_STORE=sqlite` on a purged `.stack/`, with `restart-check restore`): only that replay holds the state directory
  as `deploy/README.md` backs it up, the exclusive WAL file on a bind mount and the image's native SQLite library. Other documents
  cite this rule and never restate it. `[local: lab.sh stack restart-check, on PostgreSQL always and on SQLite at the trigger]`
  `[unchecked: that the trigger names every file that can break SQLite]`
- **The runner's image starts as root, its `dtpipe` never does.** `deploy/runner-entrypoint.sh` installs the rules of
  `DTPIPE_EGRESS_ALLOW` (CIDR masks, comma-separated: out, the masks and the loopback; in, nothing but replies), reads them
  back, then starts `dtpipe node --runner` through `setpriv` as `1654:1654` with no capability, an empty bounding set and
  `no_new_privs`. A list it cannot apply stops the start; an empty list or `DTPIPE_EGRESS_RULES=off` installs nothing and warns.
  The rules take addresses, never names: a name resolved once is a rule that outlives the address. Every other image, the
  coordinator's, keeps `USER 1654:1654`. **A step never reads the runner's identity**: a client secret given as `@path` is
  read by the entry point, as root, so the runner's key is root's alone (`deploy/compose.yaml`), and so is an end-to-end key
  given as `@path` (`DtPipeNode__EndToEndEncryption__PreSharedKey`), and the runner makes itself not
  dumpable (`NotDumpable`, Linux) before it starts a step, so a step, under the same user, reads neither its environment nor its
  memory. The runner's state directory, the other runs' jobs and spill files included, stays that user's.
  `[local: validate_runner_egress.sh, on the images of a checkout]`
  `[unchecked: a Linux Docker host, Kubernetes; DNS through Docker's resolver, which the rules cannot see; the key's owner and
  the runner's dumpability]`
- **A key is never stored, logged, returned or compared but as its hash, and the hash never leaves the store.** The call that
  makes a key is the only answer that carries it, and a key the deployment gives (`Coordinator:Bootstrap`) is read once and hashed;
  the rights keep its SHA-256, which no route answers, and no log carries the key, a part of it or its hash. A presented key is
  judged by that hash, in constant time, never by the client id it names; `/connect/token` and the sign-in form
  (`POST /account/login`) are the only routes that take one in. `[CI: Server.Tests IdentityKeyRoutesTests, IdentityKeyTests,
  KeyIssuerTests, PersonKeyTests, KeyOnlyCoordinatorTests — the logs of a bootstrapped start and of its keys at work]`
  Detail: `src/DtPipe.Coordinator.Server/CLAUDE.md`.
- **No page offers to copy a command that holds a key.** The Access page is the only one that asks for a key: it shows it once, in
  the one element its Copy button copies, which closing the window empties; every command it shows names a variable or a reference
  the person fills (`--client-secret '${{VAR}}'`, `keyring://`, `--key '${{VAR}}'`), never the key, and nothing of a page keeps it in
  storage or in the address. `[CI: Server.Tests KeyWindowTests — the pages' text, not their behaviour]`
- **A cursor is the coordinator's: read when a run starts, kept only at the verdict of the whole run.** One cursor per branch that
  writes, keyed by (library pipeline, alias), kept by `CursorKeeper` in the store that holds its state. The child never reaches the
  coordinator or a file a brick names: the node gives each sink branch that sets `cursor` a state file of its own, reads it once the
  child exits with 0 and sends it in `Exited`; the coordinator hands the kept value to the fragment holding the one source upstream of
  the sink as `DTPIPE_CURSOR_OVERRIDE`. A failed, cancelled or interrupted run advances nothing. A cursor is tied to the versions of its
  sink brick (which holds the column) and its source brick: a change refuses the run by name. A launch beside a run of the same
  pipeline is refused, never queued, once a cursor exists. A value is substituted into the source's SQL, so `CursorValues` is the one
  rule the node, the coordinator and the route apply. `BrickRules` admits `cursor` on a sink, never `state`, never `cursor` on a source
  (a fragment that reads advances a cursor before the sink has written), and refuses a `cursor://` token that names a
  file. `[CI: Server.Tests CursorRunTests, CursorRoutesTests, CursorRulesTests; Hosting.Tests FragmentCursorsTests] [local: validate_doc_examples.sh]`
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
  write its keys into a volume that the coordinator and the runner read by `@path`. `[CI: Server.Tests KeyIssuerTests,
  PersonKeyTests, KeyOnlyCoordinatorTests; Hosting.Tests KeyTokenTests]` `[unchecked: a real
  TLS proxy; two instances that bootstrap at the same first start; deploy/compose.yaml, until a Docker host runs it]` Detail:
  `src/DtPipe.Coordinator.Server/CLAUDE.md`.
- **`DtPipe.PipelineNode.Hosting` is the only package of the family, and it bundles its three projects.** A consumer gets the host
  library, `DtPipe.PipelineNode`, `DtPipe.Coordinator.Contracts` and `DtPipe.Jobs` from one package, and a dependency on the packages
  they use (TransportR among them), never on a `DtPipe.*` package. The other projects of the family are not packable. A pack is a
  step of its own (`tests/scripts/validate_node_host_package.sh`): the family stays outside `DtPipe.sln`, so the solution's pack never
  reaches it, and `release.yml` packs and pushes it in a step after the solution's (`--release`), reading `<Version>` from
  `Directory.Build.props` like the solution's packages, gated by `verify-version` against the tag.
  `[CI: validate_distributed_boundary.sh — one packable project, its references private]`
  `[local: validate_node_host_package.sh — the package's content]` `[unchecked: the release's step, until a tag runs it]`
- **The CLI hosts a node only through one assembly, and never a hub.** `dtpipe` is the child a node drives, reached through
  `arrow:`; `dtpipe node` is a node whose bricks are the branches of the pipeline grammar (`src/DtPipe/CLAUDE.md`). The
  ordinary CLI references no TransportR assembly: only `DtPipe.PipelineNode.Launcher` carries it, and `Cli/Node/NodeHostEntry.cs`
  is the one file that names it, so the runtime loads it when `dtpipe node` runs and for no other command. The CLI references no
  other project of the family, maps no SignalR hub, and `dtpipe studio` is a client of the coordinator's `/api`, only when it is
  given `--coordinator`. A node started by the CLI takes no setting from a message and no secret from an argument: the client
  secret is a reference (`keyring://`, `${{NAME}}`, `@file`) read once at start, or the value in the environment.
  `[CI: validate_distributed_boundary.sh — names and references, one file naming the Launcher, no SignalR hub under src/DtPipe;
  NodeCommandTests in DtPipe.Tests — no TransportR loaded after --version, --help or node --help]`
  `[CI: DtPipe.PipelineNode.Tests NodeCommandTests — a node started by the command announces its bricks and shows its secret nowhere]`
  `[unchecked: a listener of the CLI that is no SignalR hub]`

`[CI: DtPipe.Coordinator.Tests, DtPipe.PipelineNode.Tests, DtPipe.Coordinator.Server.Tests] [local: the lab's
smoke.py]`
`[unchecked: Windows]` Detail: `src/DtPipe.Coordinator/CLAUDE.md`, `src/DtPipe.PipelineNode/CLAUDE.md`,
`src/DtPipe.PipelineNode.Hosting/CLAUDE.md`.
