---
name: dtpipe-test
description: Run a targeted subset of the dtpipe xunit tests and report only build errors, failed tests (message plus first in-repo frames) and the summary. Use instead of a raw `dotnet test` whenever verifying a change in the dtpipe repository.
---

# dtpipe targeted tests

A raw `dotnet test` prints restore, build and one line per passing test. The bundled script sends
all of it to `tests/scripts/artifacts/dtpipe_test.log` and prints only what needs acting on.

## Run

```bash
.claude/skills/dtpipe-test/run_tests.sh --changed                                   # derive from the diff
.claude/skills/dtpipe-test/run_tests.sh "FullyQualifiedName~PipelineLexerTests"
.claude/skills/dtpipe-test/run_tests.sh "FullyQualifiedName~A|FullyQualifiedName~B"
.claude/skills/dtpipe-test/run_tests.sh --integration "FullyQualifiedName~PostgreSql"  # via test_local.sh
.claude/skills/dtpipe-test/run_tests.sh --project tests/Apache.Arrow.Serialization.Tests/Apache.Arrow.Serialization.Tests.csproj "FullyQualifiedName~ArrowSerializer"
```

`--changed` maps each changed or untracked `Foo.cs` to `FooTests` when such a test file exists, and
keeps changed `*Tests.cs` files as they are. It prints the filter it used. When it finds nothing,
pick the filter yourself.

## Choosing the filter

Start narrow, widen once green:
1. The test class of each changed class (`--changed`).
2. The obligations the root `CLAUDE.md` attaches to the area touched:
   - `DagOrchestrator` → `DagOrchestratorTests`, plus the golden-shape suites
     `ChannelInjectionTests|EngineInvariantsTests|JobDagDefinition_JsonTests`;
   - `LinearPipelineService` → `OrderedPipelineTests`;
   - lexer or converter → `PipelineLexerTests|PipelineToJobConverterTests`;
   - a columnar transformer or anything that disposes a `RecordBatch` → `ArrowOwnershipTests|CDataOwnershipTests`;
   - sample mode → `SampleModeEquivalenceTests|SampleModeSafetyGateTests`;
   - redaction → `ConnectionStringSanitizerTests`;
   - adapter help attributes → `McpAdapterHelpTests`.
3. The whole unit tier before declaring done: `"FullyQualifiedName~.Unit."`.

Integration tests (`.Integration.`) need containers: use `--integration`, which goes through
`./test_local.sh` and reuses the persistent infrastructure.

## Read the result

Report the summary line and, for each failure, the assertion message and the frame in this
repository. Open the full log only for a failure the extract does not explain, and then only
around the failing test (`grep -n` for its name, bounded `Read`).
