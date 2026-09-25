---
name: dtpipe-perf-gate
description: Measure or compare dtpipe performance — the micro gate (BenchmarkDotNet on hot conversion paths) and the macro bench in experiments/dtpipe-sandbox — and interpret the numbers without publishing noise. Use before and after any engine or conversion-path change, when a benchmark looks slower or faster, before recording a baseline, and before stating any performance figure in a commit or changelog.
---

# dtpipe performance gate

Three tiers, all local — none run in CI:

| Tier | What | Where | Threshold |
|---|---|---|---|
| Micro | BenchmarkDotNet in-process on the hot conversion paths, no infra | `tests/scripts/micro_perf_gate.sh`, reference machine | Wide (detects a ×2) |
| Macro complete | 15 scenarios incl. Oracle / SQL Server | `experiments/dtpipe-sandbox` | 15 % |
| Macro light | file↔file + PostgreSQL subset | optional, nightly, only if micro proves insufficient | Wide |

## Micro

Run through the wrapper, **in the background** — BenchmarkDotNet takes minutes and prints
thousands of lines the wrapper keeps in `tests/scripts/artifacts/micro_perf_gate.log`:

```bash
.claude/skills/dtpipe-perf-gate/micro_gate.sh                     # compare with the baseline
.claude/skills/dtpipe-perf-gate/micro_gate.sh --report-only       # numbers only, no verdict
.claude/skills/dtpipe-perf-gate/micro_gate.sh --filter "*Extract*" # fast iteration only
.claude/skills/dtpipe-perf-gate/micro_gate.sh --update            # record a new baseline
```

Exit codes: `0` pass, `1` regression over the threshold, `2` refused (foreign host), `3` error.

- **A filtered run is not comparable with the baseline.** The baseline holds every benchmark
  measured in one process; a subset runs under different memory pressure and GC behaviour. A filtered
  run once gave `B6_Extract_String` −47 % and `B7_Extract_Int32` +83 % together, with no code change
  behind either; the next full run put both back within ±2 %. Use `--filter` to iterate on an order
  of magnitude — a ×5 stays a ×5 — never for a figure you report.
- **Replay a deviation before diagnosing it.** Two of four gaps suspected to be "a bad machine day"
  reproduced within 3 %; they were real.
- **The baseline records its machine.** A different fingerprint is refused (exit 2, no verdict).
  `--allow-foreign-host` exists for a deliberate cross-machine check and clamps the threshold wide.
  On a GitHub runner, 30 of 31 benchmarks came back +111 % to +201 % from machine identity alone.
- **`--update` only on the reference machine, from a full run, for a deliberate and understood
  shift** — and say in the commit what shifted and why.

## Macro

The sandbox is a separate repository with its own `CLAUDE.md` (runner, `--scope`, `--tool`,
`--repetitions`, baseline comparison). Work from the dtpipe root, run the bench as a background
command, never push the sandbox without asking. Build any sibling repository through its real path,
never through the `experiments/` link: MSBuild would import dtpipe's `Directory.Build.props`.

- **Below ~10 % between two macro runs there is no result to report.** Run-to-run dispersion is far
  wider than within-run: the same binary gave `B18` 17 969 ms then 19 627 ms the same day (+9 %)
  while σ inside each run was ~1.9 %.
- **Interleave an A/B, never run it in blocks.** A×3 then B×3 produced an 8 % phantom gap (13 864 vs
  15 021 ms, same binary): drift hits whichever block runs last. Each round runs every binary once,
  in the same order, for at least 5 rounds; report min, median and dispersion.
- **Re-running memory-heavy scenarios back to back degrades them.** The OrbStack VM caps memory at
  ~15.7 GiB and the three database containers take ~6 GB; `B18`/`B19` measured slower on each
  isolated re-run with nothing changed. Trust the coordinated run's log over later standalone
  re-runs, or restart the containers between measurements.
- **The derived transformation figures are mis-calibrated.** Since `NullDataWriter` gained the
  columnar contract, `B18 − B16` measures `--compute` **plus** an Arrow round trip, and
  `(B19 − B17) − (B18 − B16)` cancels to ≈0 or below. Per-scenario totals are unaffected.
- **Generate bench data with `generate:` + `--fake`** (columnar), never `--compute "Math.random()"`
  (slow, leaks) nor `duck:`. Seed `--fake` when the control and the trial must read the same values.

## Reporting

A figure goes into a commit or changelog only from a full micro run or an interleaved macro series,
with its dispersion. A gap inside the noise band is reported as "no measurable difference", not as a
number.
