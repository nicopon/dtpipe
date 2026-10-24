---
name: dtpipe-test
description: Run a targeted subset of the dtpipe xunit tests and report only build errors, failed tests (message plus first in-repo frames) and the summary. Use instead of a raw `dotnet test` whenever verifying a change in the dtpipe repository.
model: haiku
---

# dtpipe targeted tests

The script sends restore, build and per-test output to `tests/scripts/artifacts/dtpipe_test.log` and
prints only the filter, build errors, each failure and the summary.

## Run one command

```bash
.claude/skills/dtpipe-test/run_tests.sh --changed                                     # the test classes of the diff
.claude/skills/dtpipe-test/run_tests.sh "FullyQualifiedName~A|FullyQualifiedName~B"
.claude/skills/dtpipe-test/run_tests.sh --integration "FullyQualifiedName~PostgreSql"  # via test_local.sh
.claude/skills/dtpipe-test/run_tests.sh --project tests/<Name>.Tests/<Name>.Tests.csproj "FullyQualifiedName~X"
```

Put every class you need in one filter, joined with `|`, so the project builds once. `--changed`
prints the filter it derived; when it finds nothing, name the classes yourself. The tests an area
owes are the guard tags and "Engine-change obligations" of the root `CLAUDE.md`. Widen to the whole
unit tier only when asked: `build.sh` runs it.

## Report

The summary line and, for each failure, its assertion message and first frame in this repository,
as printed. Do not fix anything. Open the log only for a failure the extract does not explain:
`grep -n` the test's name, then a bounded read around it.
