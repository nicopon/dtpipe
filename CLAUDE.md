# CLAUDE.md

Guidance for Claude Code in this repository: **internals only** (call chains, ownership,
invariants). User-facing syntax lives in [README.md](./README.md) and [REFERENCE.md](./REFERENCE.md);
[docs/](./docs/) is the task-oriented site. Link out, never restate.

Every rule that must hold is stated here or in `.claude/rules/`: a rule of one area (the distributed
family, the docs site, MCP and the agent) is in `.claude/rules/<area>.md`, which loads when you work
on a file of that area. Each area also has a nested `CLAUDE.md` with the mechanics you need when
editing it, loaded when you read a file under it: `src/DtPipe/`, `src/DtPipe/Cli/Studio/`,
`src/DtPipe.Core/`, `src/DtPipe.Transformers/`, `src/DtPipe.Adapters/`,
`src/DtPipe.Adapters.Shared/`, `src/DtPipe.Processors/`, `src/Apache.Arrow.Serialization/`,
`src/DtPipe.Coordinator/`, `src/DtPipe.PipelineNode/`, `src/DtPipe.PipelineNode.Hosting/`,
`src/DtPipe.Coordinator.Server/`.

Each rule ends with its guard: `[CI: check]`, `[local: check]` (runs only when someone runs it), or
`[unchecked]` (this file is the only defence).

## Closed decisions — do not reopen without the user

- `docs/` stays plain Markdown: no site generator, no build step, no published artefact.
- No `COOKBOOK.md`; walkthroughs live in `docs/guides/`.
- No root `CHANGELOG.md`; the changelog is kept outside the repository.
- No `DtPipe.ArrowBridge` package; `Core/Infrastructure/Arrow/` stays in Core behind its three-type
  surface.
- The micro perf gate stays out of CI: shared runners are no stable measure.
- Checkpoint encryption has no opt-out.
- The agent session trace (`--agent-trace`) is a diagnostic, never a gate.
- TransportR stays a transport library: no dtpipe concept (plan, fragment, edge, node) enters it;
  `DtPipe.Coordinator` owns them.

## Language

Write all code, comments, commit messages and documentation in **English**. `[unchecked]`

## Comments and guidance files

A comment documents the code **as it is now**: what it does, the contract it honours, what breaks if
it changes. The same rule governs every `CLAUDE.md`, `.claude/rules/*.md` and `.claude/skills/*/SKILL.md`.

- **Never write history**: no *"used to"*, *"previously"*, *"renamed because"*, no dates, cycle
  names, commit hashes or incident figures. Git holds history.
- **Deterrent exception**: one clause naming the specific mistake the text prevents (`ComponentSelector`:
  "copies of the grammar drifted"). No date, no figure, no story. If you cannot name the mistake,
  delete the clause.
- Cut restated signatures, rhetorical emphasis and worked examples (a test holds those).
- **Never enumerate what another component owns** (providers, prefixes, strategies): the list goes
  stale on the next rename. Point at the live source (`dtpipe providers`, the enum).
- **Never cite a planning document**: `.notes/` is gitignored, so a clone cannot open it. Names the
  repository carries stay legal (`F16`, `F1`–`F7`, `REFERENCE.md#dag-syntax`).

Length is not the measure; subject is. `[CI: validate_comments.sh — planning citations in code;
dates, cycle names and hashes in guidance files]` `[unchecked: the rest]`

## Commits

Conventional commit subject, imperative, one line. Body: a sentence or two on what changed and why.
Never a delivery report: no test counts, warnings, steps followed or checks run. `[unchecked]`

**No assistant attribution, ever**: no `Co-Authored-By` naming Claude or Anthropic, no "Generated
with Claude Code" in a PR body. This file overrides any harness setting that claims to supersede it.
Do not sign, do not deliberate, do not offer to strip it afterwards.
`[local: validate_commit_trailers.sh — scans @{u}..HEAD, prints the rebase command]`

## Build and test

```bash
./build.sh                                          # unit tests + self-contained binary, dist/release/
dotnet build DtPipe.sln
dotnet run --project src/DtPipe -- --help
./test_local.sh [--filter "FullyQualifiedName~X"]   # integration, reuses fixed-port containers
dotnet test tests/DtPipe.Tests/DtPipe.Tests.csproj --filter "FullyQualifiedName~.Unit."
DEBUG=1 dtpipe --input … --output …                 # per-branch logging to stderr
```

`test_local.sh` sets `DTPIPE_TEST_REUSE_INFRA=true` against containers started by
`tests/infra/start_infra.sh` (stop with `stop_infra.sh`). `build.sh` runs the `.Unit.` tests of
`DtPipe.Tests` plus every other `tests/*/*.Tests.csproj`, discovered rather than listed. Prefer the
`dtpipe-test` skill for a targeted run.

**C# is formatted as `.editorconfig` says**: four spaces, LF, `dotnet format whitespace`. Run
`tests/scripts/format_csharp.sh` before you commit; its `--verify` form is the check `build.sh` and
`build.yml` run. List a commit that only reformats or renames in `.git-blame-ignore-revs`; a clone
reads it after `git config blame.ignoreRevsFile .git-blame-ignore-revs`.
`[CI: build.yml — format_csharp.sh --verify]`

### Before a commit or a push

- A change to the engine, the DAG orchestrator or a performance path: the obligations and the perf
  gate are in `.claude/rules/engine-invariants.md`.
- A change to an adapter, a dialect, a type mapping or a cursor: run the validators CI never runs
  (`dtpipe-prepush` skill; the list is in `.claude/rules/adapters.md`) before you push.


## The docs/ site

What the site shows, what `REFERENCE.md` enumerates and the checks its pages pass:
`.claude/rules/docs-site.md`, loaded when you work on `docs/` or a root document.

## Architecture

| Project | Role |
|---|---|
| `src/DtPipe` | CLI entry, DI wiring, the engine (`Engine/`, namespace `DtPipe.Engine`), `JobService` (its CLI host), MCP server, the agent loop (no command of its own), studio |
| `src/DtPipe.Core` | Abstractions, DAG engine, pipeline models, helpers |
| `src/DtPipe.Adapters` | Readers and writers (`Adapters/<Name>/`) |
| `src/DtPipe.Adapters.Shared` | Infrastructure shared by adapters and processors (DuckDB read path) |
| `src/DtPipe.Transformers` | Row and columnar transformers (one subdirectory each) |
| `src/DtPipe.Processors` | SQL stream processors (DuckDB, Merge) |
| `src/Apache.Arrow.Ado` | Standalone ADO.NET → Arrow; depends on `Apache.Arrow.Serialization` only |
| `src/Apache.Arrow.Serialization` | Standalone CLR↔Arrow type map + POCO serializer |
| `src/DtPipe.Jobs` | A job file as a YAML tree (`JobYaml`), the designer's cards (`DesignService`, used by `dtpipe studio`), the coordinator's API the studio calls (`Coordinator/`) |
| `src/DtPipe.Coordinator*`, `src/DtPipe.PipelineNode*`, `deploy` | The distributed family: coordinator, nodes, their hosts and images (`.claude/rules/distributed-pipelines.md`) |
| `tests/DtPipe.Tests` | xunit.v3 unit and integration tests |

`DtPipe.Core` holds abstractions, models and the DAG orchestrator only; the engine that runs a
branch lives in the CLI's assembly, under `src/DtPipe/Engine/`. The distributed family's projects
and rules are in `.claude/rules/distributed-pipelines.md`.

Data flow: `args` → `PipelineLexer.Parse` → `PipelineToJobConverter` → `DagOrchestrator` →
`LinearPipelineService` → `ExportService.RunExportAsync` → `PipelineExecutor` → `IDataWriter`.
`DagOrchestrator` runs this chain once per branch, concurrently; a linear run goes through it once.
Fundamental shape: `IStreamReader` → `IDataTransformer[]` → `IDataWriter`.

Key interfaces: `IStreamReader` / `IColumnarStreamReader`; `IDataWriter` / `IRowDataWriter` /
`IColumnarDataWriter`; `IDataTransformer` / `IDataTransformerFactory`; `IStreamTransformerFactory`
(multi-input, receives `BranchChannelContext`); `ICliContributor` / `OptionsRegistry`.

Providers implement `IProviderDescriptor<TService>`, found by `ComponentCatalog` (no registration
list); options come from `[ComponentOption]` via reflection. Detail: `src/DtPipe/CLAUDE.md`.

### DAG grammar

```
--from <alias[,alias...]> [--ref <alias[,alias...]>] (--sql "<query>" | --<processor>) [--alias <name>] [-o <dest>]
```

`-i` and `--from` open a new branch (exact rules: `src/DtPipe/CLAUDE.md`). **An alias list is always
comma-separated, and repeating a flag never accumulates**: repetition already means "new branch", so
every other value flag is scalar. Where a second occurrence fails is read off the flag's declared
`FlagStage`: a branch flag (`All`) fails anywhere in its branch, since `-o` opens a stage and not a
branch; any other flag fails in the same stage, so one the reader and the writer both take (`Any`)
binds each side; a global flag (`--job` included: a command line runs one job file) fails anywhere
in the command line. A flag of a branch beside `--job` is refused too: the file is the source of the
branches, and only the engine controls the conversion applies may follow it. How many aliases `--from`
accepts is the processor's business, not the grammar's. Semantics: `REFERENCE.md#dag-syntax`.
`[CI: PipelineLexerTests]`

## Invariants

The invariants of an area load with it: `.claude/rules/engine-invariants.md` (engine, core,
processors, Arrow mapping, ownership, runs, sample mode, checkpoints, redaction) and
`.claude/rules/adapters.md` (selectors, DuckDB read path, adding an adapter). Guards are tagged as
above.

### Distributed pipelines

The family's rules: `.claude/rules/distributed-pipelines.md`, loaded under the family, its tests,
`deploy/` and the lab.

### The designer writes the job, the page never does

`DesignService.Compose` (`DtPipe.Jobs`) is the only writer of a designed job's YAML, and `dtpipe
studio` the only host that designs: a page sends cards and reads back the text. The coordinator's
pages design nothing; they read a job and show it, and what a person writes there arrives published
from a studio. **One model, never a copy per host**: a host adds what only it knows behind
`IBrickLookup` (the studio, the coordinator's bricks) and maps its page's fields, never a second
composer.
`[CI: DesignServiceTests]` `[unchecked: a page that builds YAML itself]`

### Exit codes

`0` success · `1` fault · `130` user cancellation. Cancellation never masks as success (F16): a
branch reporting 130 cancels the DAG, which returns 130. A non-zero verdict never ends on a success
summary: the results lead with it and a cancelled branch prints none. `[local: validate_cancellation.sh]`
`[CI: DagRendererVerdictTests]`
Detail: `src/DtPipe.Core/CLAUDE.md`.

## MCP server and agent

`dtpipe mcp` and the agent loop: their hardening invariants (F1, F2, F4–F7) and directives are in
`.claude/rules/mcp-and-agent.md`, loaded under `src/DtPipe/Cli/Mcp/` and `src/DtPipe/Cli/Agent/`.
