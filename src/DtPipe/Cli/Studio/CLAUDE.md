# DtPipe (CLI) — studio

Mechanics for editing `dtpipe studio`. The rules themselves are in the root `CLAUDE.md`; the rest of
the CLI's mechanics are in `src/DtPipe/CLAUDE.md`.

## Studio

`dtpipe studio [folder]` (`StudioCommand`, `Cli/Studio/`) serves a local page over a folder's YAML
jobs and runs them through `IStudioEngine` (`StudioTools.Engine`, never null: a test double implements all of
it). `StudioEngine` samples and validates through `JobAnalysis`, the path the `dry-run` tool shares: never a
second sample path, and runs through `JobAnalysis.ResolveForRun` and `JobRunner`. `StudioServer` runs
one engine action at a time (409 otherwise), as the debug state and the quit require. Kestrel is referenced
for this server alone.

The page is the designer: `DesignService` (`DtPipe.Jobs`, the coordinator's own model) composes the
cards and takes a job apart, never the page. `ComponentEndpointRules` gives it dtpipe's readers and
writers through `JobCutter.OwnersOf`, so a source or sink card holds the blocks a cut would put on
that side, and `BranchSides` names the keys of each side for both.

The page runs dtpipe with the person's access, so the server is guarded like a shell:
- **No cookie.** The token rides in the URL fragment, the page keeps it in `sessionStorage` and sends
  it in `X-Dtpipe-Studio` on every `/api` call; cookies are shared by every port of a host.
- **Only its own address.** `Host` must be `127.0.0.1` or `localhost` at its port (DNS rebinding);
  another `Origin`, or `Sec-Fetch-Site` other than same-origin, is refused; no CORS header, ever.
- **No inline script or style**, and every value from a job or a source set as text in the page,
  never as HTML: a sampled value is untrusted. The page builds nodes (`el`, `fill`, `svg` in
  `dom.js`) and positions them through the CSSOM; `replaceChildren` takes nodes, never a list.
- **The folder only.** `StudioFolder` reads `.yaml`/`.yml` files under it: no segment starting with
  a dot, no symbolic link below it. The empty builder reads no `appsettings.json` from the folder.
- **The server writes, from cards.** `PUT /api/job` composes the page's cards and writes that text
  (`WriteJob`): whole and atomic, only where a job could be read, into a folder that exists, against
  the hash the page opened (409 otherwise), never over a file with a new job. What the studio keeps
  for a job (`StudioLocalFiles`) lives under `.dtpipe/studio/`, which `SessionStore.EnsureIgnored`
  keeps out of git; nothing there is reached through a link, and a link refuses the save before the
  job is written.
- **A variable's name is the contract.** `StudioVariables` reads a job's `${{…}}` with the run's own
  grammar (`VariableTokens` in `DtPipe.Jobs`), and an action arms each chosen value for
  its own call chain (`LaunchValues.Arm`, an `AsyncLocal` like `NonInteractiveGuard`) which
  `LaunchValueInterpolator` gives the real resolver ahead of the environment, in the one chain
  `CompositeStringContentResolver.Standard` builds for the DI and for `JobFileParser`. Nothing writes the process environment, so a request beside the action never
  resolves its values; the Variables tab reads the real environment only.
  `[CI: StudioServerTests, StudioSampleTests — the values reach every branch of a DAG]` The
  launch file holds an alias or a typed text, never a keyring value; the page sees names and
  aliases only. The keyring is opened only for a job that needs it, and its alias list is kept for the session
  (read again once when a name is missing): opening it can ask the person and wait.
- **The command line names the coordinator, never the page.** `--coordinator <url>` gives the studio
  a `CoordinatorLink` to that origin: the coordinator's metadata (RFC 9728) and contract number
  (`CoordinatorApi.Contract`, refused by name when it differs), its
  provider's discovery (issuer equal, no HTTPS downgrade), then authorization code with PKCE through
  a new tab, back to `/callback`, the one path reached without the session's token: a GET, the
  `Host` check kept, a single-use `state` that a wrong one leaves waiting, `iss` checked when sent.
  `--key` takes a reference (`${{NAME}}`, `keyring://`, `@file`) and refuses a value, as `--client-secret` does, and
  is read once at start (`StudioCommand.ReadKeyAsync`): the studio then opens no tab. The metadata's `dtpipe_auth` must name
  `key`, and the key is exchanged (client credentials) only at the token endpoint of the authorization server that is the
  coordinator's own address, its discovery's issuer equal and the endpoint on that origin; it is presented again to renew.
  The tokens and the key live in the process only, never in a response or the page; the access token is renewed
  two minutes ahead, a 401 renews once and sends once, a refused renewal signs out with its reason.
  The link's client sends to the coordinator's origin only, follows no redirect, and always carries
  the CSRF header. `[CI: StudioCoordinatorTests, StudioKeyTests]`
- **Without `--coordinator` the studio is whole, minus the bricks, the library and the publication.**
  `StudioTools.Coordinator` is null: `/api/coordinator` has no address, `/api/bricks` an empty catalogue,
  `GET /api/publish` no publication, and `/api/coordinator/login`, `/api/library`, `/api/library/open` and
  `POST /api/publish` answer 404 "opened without --coordinator". Jobs, cards, variables and samples never
  ask for the link; a route that needs it refuses the same way.
  `[CI: StudioServerTests — jobs, cards, variables, samples and the five refusals]`
  `[unchecked: import, debug and run without a coordinator; that a new route needing the link refuses]`
- **The coordinator decides what a brick is; the studio reads its answer.** A card that is a brick is written as the reference to
  it (`input: brick:<node>/<id>` for a source, `output: brick:<node>/<id>` with `from` for a sink): `DesignService` writes it from the
  catalog's node, id and kind through `BrickRef.Branch`, the function the coordinator's catalog uses, and is the one composer. Before taking a job apart, the studio sends its reference branches to
  `/api/bricks/recognize` and gives `DesignService` a `StudioBricks` that finds each answer again by the top-level branch's YAML, a
  key and never a matching rule (`Match` sees each top-level branch whole: `StudioBrickTests` pins it). Nobody signed
  in, or no answer: no reference is a brick, and each opens as a raw branch card, which composes back as written. `Find`/`BranchOf`
  read the catalog read last, kept after a sign-out, so a brick card still composes and saves. **Only a branch that is a reference
  is sent** (`ForRecognition`, by `BrickRef.Of`): a branch of this machine, a connection string included, never leaves it.
  A reference never reaches the engine: a sample, a debug run or a real run of a job in which a branch still references a brick that
  no card bound is refused by name (`RefuseReferences`). The catalog carries no schema, which depends on the rows: a brick card
  reads its node's first rows when a person asks (`POST /api/bricks/preview`, `CoordinatorLink.PreviewAsync`), kept nowhere.
  `[CI: StudioBrickTests, StudioRunTests]`
- **A brick samples here through the run's own binding.** `BrickSampling` turns a source brick's
  branch into `input: arrow:-` and a sink brick's into `output: arrow:-`; `SampleAsync` binds them as
  `--bind-input`/`--bind-output` do (`AliasBindingApplier`, before validation): never a second
  binding path. A source's rows come from its node through the coordinator, at most
  `CoordinatorApi.MaxSampleRows`; a sink's end in a throwaway file. Those files live under
  `.dtpipe/studio/samples/`, created owner-only, deleted when the sample ends and purged when the
  studio starts, never reached through a link. A brick's branch is out of the Variables tab: its node
  resolves those.
- **A publication sends no binding.** A reference names its node and its brick, so `/api/publish` sends the saved job alone
  (`LibrarySave.Bindings` stays absent): the coordinator keeps the mode of each reference and writes its pin. `[CI: StudioPublishTests]`
- **A publication sends the job saved in the folder, nothing beside it.** `/api/publish` reads the
  file again and refuses one whose hash is not the one the page saved; the layout and the launch
  file stay on this machine. The first publication of an id creates only (`If-None-Match: *`); a
  later one replaces only the commit published last from here (`<job>-publish.json`, under
  `.dtpipe/studio/`), and a 412 is replaced only when the person names the commit in place. A 422 is
  the coordinator's check refusing the job: the report is shown and nothing was published.
- **A command line imports through `--export-job`'s own path.** `POST /api/import` takes the
  arguments as the array the page split (never one string to cut here) and answers the text
  `CommandLineJob.Export` gives, which `Program.cs` shares for its refusal of `--bind-input`/
  `--bind-output`; a `${{…}}` stays as typed, and nothing is saved until the person saves the new
  job. `[CI: CommandLineJobTests, StudioServerTests]`
- **A library pipeline opens as a new file, never over one.** `POST /api/library/open` writes
  `<id>.yaml` through `WriteJob` (create only, 409 when the file exists) and keeps the version read in
  `<job>-publish.json`, so the next publication sends `If-Match` for it; a link under `.dtpipe/`
  refuses before the job is written. `[CI: StudioPublishTests]`
- **A run is the saved file, confirmed first, one engine action at a time.** `POST /api/run/prepare`
  re-reads the file (its hash is the page's), resolves every variable and lists what each branch writes:
  the output through `ConnectionStringSanitizer.Redact`, the writer's block as the file holds it (never
  interpreted), a sink brick as a throwaway file, and a source brick reading its node's rows through
  the coordinator. Its digest, an HMAC of the text and the launch values by name under the session's token,
  is what `POST /api/run` must carry: another text or another value is a 409. The run goes through
  `IStudioEngine.RunAsync`: `ResolveForRun` validates the job, then the SQL safety policy judges it, and
  only then `JobRunner` runs it. It holds the sample's semaphore (409 for a sample, a run or a quit beside
  it), and reports each branch's failure through `ConnectionStringSanitizer.Sanitize`.
  **A stop asked while the run was unfinished ends in 130**, whether the engine threw (a linear
  job) or answered 1 or 0 (a DAG); a run finished before the stop keeps its code and a later stop
  is a 409. `[CI: StudioRunTests]` `[local: studio_check.py]`
- **A run that reads a source brick is a sample.** It reads at most `CoordinatorApi.MaxSampleRows` rows, so
  `PlanRun` refuses (409) such a job when any output is not a sink brick's throwaway file, naming the source bricks,
  the bound and the refused outputs; the prepare result carries `sample` and `sampleRows`, and the page says nothing
  real is written. A staged run of it keys its levels as a sample's (the bound and `SampleSalt`), never as `real`, and
  its digest still binds the source bricks. A review binds no sink: the cut has no writer left. `[CI: StudioRunTests]`
- **A preview is the real sample of the job, cut only at the pinned cards, never the job's run.** A
  watcher (the page's pin) sits at a card's read (level 0) or at the end of its branch. `DebugCutter` cuts a branch
  there into mini-branches joined by `from:` (`__level` is reserved), each with a checkpoint in the
  studio's own session, through `JobCutter.Cut` and `BranchSides`: the head keeps the reader and what
  bounds it, the last piece keeps the branch's alias, output and writer side. With no watcher nothing
  is cut; a branch with a processor, no transformer or one `JobCutter` refuses runs whole and keeps
  an end checkpoint only. A run stops at a card (`upTo`) and drops what the stop does not need (`Dropped`),
  reading each branch it needs back from the highest level held (`Unread`: no input left to bind).
  **A stop is for its run only**: it is not added to the watchers, and the checkpoint it wrote is deleted
  unless a level held has the same key. It runs through `SampleCoreAsync` (bricks bound as a sample binds them),
  the bound applies to each source alone (at most `DebugMaxRows`, a thousand rows shown per stage) and a
  checkpoint read back is not bounded again. With no stop the run reaches the end of the job: only the branches
  that write or that nothing reads start it, so a branch whose reader reads a checkpoint back is left out.
- **The server says where a card's rows are; the page never rebuilds the cut's names.** The view lists
  `places`, one per branch read and end, with the digest of what its rows depend on and, after a run, the `alias` of
  the piece of the report that holds it (first stage for a read, last for an end) or null when the run did not
  produce it (read back from a level held, upstream of a stop). A null place keeps the rows it showed, while
  its digest is unchanged. `POST /api/debug/plan` answers the places of a text and nothing else: pure, no engine
  slot, no file of the job's. After an edit the page compares digests as strings and clears the rows they no longer
  support. `[CI: StudioDebugTests]`
- **A staged run is the job's own run, and nothing leaves the process before its last stage.**
  `POST /api/run/prepare` and `POST /api/run` take `debug` and `review`; the saved file only, and the
  digest also binds the mode, the watchers and the keys held. `DebugCutter.Cut(write: true)` makes the
  cut at the pinned cards. With `review` (`beforeWriters`) every branch that has an output ends at its
  end with a checkpoint and no writer (upstream branches included; one that runs whole keeps its processor and
  loses only the writer's keys and blocks, `WithoutWriter`; a job no branch of which writes is refused), because the engine neutralises nothing
  outside a sample. The cursor and its state file move with the piece that writes the branch
  (`JobCutter` puts them on the head, which would advance them for rows no target received). The
  engine reports the key of each committed checkpoint through `RunOutcome.Checkpoints`. A level of a real run is keyed to every row and to a salt (an HMAC
  under the session's token of the launch values and the state files a cursor reads), so it serves no sample and no
  other values; a write that reached the end of the job deletes the levels that fed it, one that stopped
  or failed keeps what it completed as `real` (`DebugState.Real`), and until it is continued or abandoned
  (`resetAll`) a sample, a change of watchers and any run but its continuation are a 409.
  **A review's holds are the run's, never pins**: they are watchers for the run only, leave `state.Watch`
  before it is saved, and `Judge` keeps a held end of a writing branch without a pin while `Real`. The
  continuation is refused (409) unless every writing branch holds its end, so no row is written unread.
  `view.queue` lists, while `real`, every writing branch of the saved file with its key and the rows
  counted once when the review ended (`HeldLevel.Rows`, a count in the clear file, never a row).
  `POST /api/debug/rows` reads the first rows of a held key, the only way a person sees them before the
  write. A failed last stage leaves the target as its writer does in a plain run, and the held rows for a
  retry. `[CI: StudioStagedRunTests]` `[unchecked: the page, a writer other than the local ones tried]`
- **A stop is a new run, never a paused process.** A review is a run that ends before its writers; the
  continuation reads the checkpoints back from the highest level held and writes. A watcher has a branch,
  a place and the card it sits on, nothing else.
- **A level's digest comes from the job as written, never from the cut.** It hashes the branches the
  level reads, the reader side, the transformers up to it and the rows asked, so pinning another card
  invalidates nothing. The launch values are in both, as a salt: a real run's under the session's token (it
  does not survive the studio), a sample's by name under a key derived from the debug session's own
  (`StudioVariables.Fingerprint`: a typed text and a process variable by value, a keyring alias by its name, never
  its value, so the keyring stays closed and a value changed under the same alias is not seen). A job with no
  variable has no salt. `/api/debug/plan` takes the job's `path` for the same reason, and the page asks it again once
  a launch value changed. A salt creates the session's key file, so that route is pure of the engine and the job, not of user state.
- **A checkpoint is valid or does not exist.** `POST /api/debug` and `POST /api/debug/state` settle the
  watchers and held levels against the text under the engine slot, delete from the session what the
  text, the rows or a reset no longer supports (a reset takes everything downstream with it), and
  answer what is held; the page never decides validity. `.dtpipe/studio/<job>-debug.json` keeps the
  watchers and each held level's key and digest (and, for the end of a branch a review holds, its row count), never the text, and does not exist for a job that
  holds none; a job not saved yet keeps it under
  `drafts/<id>`, adopted at its first save and purged when the studio starts. The cut is never saved
  or published. `[CI: StudioDebugTests]`
- **One state, one drawing, one counter per request; nothing the browser keeps is true.** The page
  keeps what it knows in `state` (`wwwroot/state.js`), one part per screen, and only the module of a
  screen writes its part. A change names the zones it touches (`invalidate`) and `render.js` draws
  them once, in a fixed order; a zone holding what the person is doing (the inspector's focused field,
  the drawer's scroll) is invalidated by what makes it untrue, never by its own events. Each kind of
  request has its own `sequence()`, so a stale answer of one never drops the answer of another, and an
  answer about a job is dropped once another job is open. The page keeps nothing in `localStorage`:
  how it was left (tab, rows, selected card, pane sizes) goes through `PUT /api/view` to
  `.dtpipe/studio/view.json`, whose values the server checks and rewrites, and which is read only as a
  preference. A new script needs its route in `StudioServer.Assets`.
  `[CI: StudioServerTests (every module is served, every import resolves; the view's bounds)]`
  `[local: tests/studio/page_smoke.mjs - Node 22+, Chrome and a built dtpipe; types are checked by the
  editor alone (// @ts-check), no tool runs them]`
- Quit from the page exits 0, Ctrl+C 130. A quit is refused while a run goes on; Ctrl+C cancels
  the run and the studio then exits.

`[CI: StudioServerTests (no script writes HTML or an inline style), StudioSampleTests,
StudioCoordinatorTests, StudioBrickTests, StudioPublishTests, StudioRunTests, StudioDebugTests, DesignServiceTests, LocalEndpointCardTests]` `[unchecked: the page's behaviour beyond page_smoke.mjs - no CI test drives a
browser]`
