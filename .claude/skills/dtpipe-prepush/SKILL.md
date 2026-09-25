---
name: dtpipe-prepush
description: Run the dtpipe validators that CI never runs (database-backed validate_*.sh, validate_xml, optionally the full vitals battery) plus the commit-trailer check, reporting only verdicts and failure tails. Use before any git push on dtpipe, and whenever a change touches an adapter, a SQL dialect, a type mapping, a cursor or a temporal conversion.
---

# dtpipe pre-push

CI skips every `tests/scripts/validate_*.sh` that sources `lib/test_connections.sh` (it needs a
database), plus `validate_vitals` and `validate_xml`. Nothing runs them unless you do.

## Run

Launch the bundled script **in the background** (`run_in_background: true`) — a clean
`build.sh` plus the database validators takes longer than the Bash timeout:

```bash
.claude/skills/dtpipe-prepush/prepush.sh              # build + infra + DB validators + validate_xml
.claude/skills/dtpipe-prepush/prepush.sh --skip-xml   # without the 2.7 GB XML run
.claude/skills/dtpipe-prepush/prepush.sh --all        # the whole vitals battery instead
```

`--skip-build` reuses `dist/release/dtpipe`. Use it only when that binary was built from the
current tree: the validators run the binary, not the sources, so a stale one validates old code.

The script derives the validator list with the same rule `build.yml` uses, so a new database-backed
validator is covered without editing anything. Full logs land in `tests/scripts/artifacts/`
(`prepush_*.log`, `vitals_<name>.log`); stdout carries one line per step and the tail of each
failure.

## Read the result

- `PREPUSH: PASS` — report that in one line. Do not paste the table.
- A failure — open the named `vitals_<name>.log` only around the failing assertion (`grep -n`,
  then a bounded `Read`), never whole. Diagnose before re-running: a validator that fails on
  infrastructure (connection refused, container unhealthy) is not a code regression — restart
  with `tests/infra/stop_infra.sh && tests/infra/start_infra.sh` and say so.
- `commit trailers` failing means an unpushed commit carries assistant attribution. The log prints
  the `git rebase -i` command; show it and ask before rewriting anything.

Never push as part of this skill. It reports; the user decides.
