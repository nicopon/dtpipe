---
name: dtpipe-perf-gate
description: Measure or compare dtpipe performance — the micro gate (BenchmarkDotNet on hot conversion paths) and the macro bench in experiments/dtpipe-sandbox — and interpret the numbers without publishing noise. Use before and after any engine or conversion-path change, when a benchmark looks slower or faster, before recording a baseline, and before stating any performance figure in a commit or changelog.
---

# dtpipe performance gate

Three tiers, all local — none run in CI:

| Tier | What | Where | Threshold |
|---|---|---|---|
| Micro | BenchmarkDotNet in-process on the hot conversion paths, no infra | `tests/scripts/micro_perf_gate.sh`, reference machine | Wide (detects a ×2) |
| Macro complete | Full scenario set incl. Oracle / SQL Server | `experiments/dtpipe-sandbox` | 15 % |
| Macro light | file↔file + PostgreSQL subset | optional, only if micro proves insufficient | Wide |

## Micro

Run through the wrapper, **in the background**: BenchmarkDotNet takes minutes and prints thousands
of lines, which the wrapper keeps in `tests/scripts/artifacts/micro_perf_gate.log`.

```bash
.claude/skills/dtpipe-perf-gate/micro_gate.sh                     # compare with the baseline
.claude/skills/dtpipe-perf-gate/micro_gate.sh --report-only       # numbers only, no verdict
.claude/skills/dtpipe-perf-gate/micro_gate.sh --filter "*Extract*" # fast iteration only
.claude/skills/dtpipe-perf-gate/micro_gate.sh --update            # record a new baseline
```

Exit codes: `0` pass, `1` regression over the threshold, `2` refused (foreign host), `3` error.

- **Never report a figure from a filtered run.** The baseline holds every benchmark measured in one
  process; a subset runs under different memory pressure and GC behaviour, and can move a benchmark
  by tens of percent with no code change. Use `--filter` to iterate on an order of magnitude only.
- **Replay a deviation before diagnosing it.** Do not dismiss a gap as a bad machine day until a
  second full run says so.
- **The baseline records its machine.** A different fingerprint is refused (exit 2, no verdict).
  `--allow-foreign-host` exists for a deliberate cross-machine check and clamps the threshold wide;
  machine identity alone moves every benchmark far past it.
- **Run `--update` only on the reference machine, from a full run, for a deliberate and understood
  shift.** State in the commit what shifted and why.

## Macro

The sandbox is a separate repository with its own `CLAUDE.md` (runner, `--scope`, `--tool`,
`--repetitions`, baseline comparison). Work from the dtpipe root, run the bench as a background
command, and never push the sandbox without asking. Build a sibling repository through its real path,
never through the `experiments/` link: MSBuild would import dtpipe's `Directory.Build.props`.

- **Below ~10 % between two macro runs there is no result to report.** Run-to-run dispersion is far
  wider than within-run σ.
- **Interleave an A/B, never run it in blocks.** Drift hits whichever block runs last. Each round
  runs every binary once, in the same order, for at least 5 rounds; report min, median and
  dispersion.
- **Never re-run memory-heavy scenarios back to back.** The database containers and the bench share
  the container VM's memory, so an isolated re-run degrades. Trust the coordinated run's log, or
  restart the containers between measurements.
- **Read per-scenario totals, not the derived transformation figures.** `NullDataWriter` implements
  the columnar contract, so `B18 − B16` measures `--compute` **plus** an Arrow round trip, and
  `(B19 − B17) − (B18 − B16)` cancels to about zero.
- **Generate bench data with `generate:` + `--fake`** (columnar), never `--compute "Math.random()"`
  (slow, leaks) nor `duck:`. Seed `--fake` when the control and the trial must read the same values.

## Reporting

A figure goes into a commit or changelog only from a full micro run or an interleaved macro series,
with its dispersion. Report a gap inside the noise band as "no measurable difference", never as a
number.
