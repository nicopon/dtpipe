# The agent, and what it is allowed to do

[← Documentation](../README.md)

`dtpipe agent` runs a loop where a language model describes a pipeline, validates it against your
real sources, and — if you let it — runs it. `dtpipe mcp` exposes the same tools to an assistant
you already use.

Both are built the same way: **what the model can do is decided by which tools it is offered**, not
by asking it nicely.

## A mission

```bash
dtpipe agent
```

It discovers the local Ollama models and asks for a mission:

> *Inspect csv:invoices.csv, compute gross_total = subtotal * (1 + tax), keep the rows above 100,
> and write jsonl:high_invoices.jsonl*

The loop inspects the source schema, builds a YAML job, validates its topology, and shows you the
DAG. The plan can be exported as a standalone job file, which is the artefact worth keeping: from
there the pipeline runs without a model in the loop.

## The default is plan, and plan cannot write

| Mode | What the model is handed |
|:---|:---|
| `--mode plan` **(default)** | Every tool except the ones that write. They are not refused at call time — they are **absent from the catalogue the model sees** |
| `--mode execute` | The writing tools too, still behind `--apply` and an approval |
| `--mode autonomous` | The same, with the approval loop driven automatically |

The hidden set is reflected off an attribute on the tools themselves, so a tool added tomorrow is
covered without anyone remembering to update a list.

**`dry-run` is deliberately offered in plan mode.** It runs the real pipeline over a handful of
rows with the writer switched off, and returns the rows leaving each stage — so the model can check
its own work against your data instead of guessing. See
[Preview and checkpoints](preview-and-checkpoints.md).

## Unlocking a write, one step at a time

```bash
# Plan only. Nothing can write, whatever the model decides.
dtpipe agent "inspect csv:sales.csv and summarise totals"

# A real write: execute mode, --apply, and an approval for each one.
dtpipe agent "load csv:orders.csv into pg" --mode execute --apply

# Only when you know the SQL is destructive, or reaches the network.
dtpipe agent "..." --apply --allow-destructive --allow-network
```

Without `--apply`, the execution tool runs as a dry run — the pipeline happens, the target does
not change. Destructive verbs (`DROP`, `DELETE`, `TRUNCATE`, `UPDATE`, `ALTER`, `INSERT`, `ATTACH`)
and network access (`LOAD httpfs`, a remote `read_parquet`) are refused unless the matching flag is
set. The refusal is the default in both cases: a policy that has to be unlocked, not one that has
to be remembered.

## Determinism is available, not the default

> [!IMPORTANT]
> **The shipped default is `--temperature 1`.** Greedy decoding degenerates weaker quantized
> models, so the default favours a loop that works over a loop that repeats. Determinism is
> something you ask for.

```bash
dtpipe agent "..." --temperature 0 --seed 42 --repeat 3
```

`--repeat N` replicates the validated plan N times from a fresh conversation each time and reports
the variance — the number of distinct YAML jobs produced, minus one. **Zero means the mission is
stable**; anything else means the wording, the model or the source is leaving room for
interpretation. `--seed` makes a run replayable at any temperature.

`--sequential` forces one tool call at a time; by default the independent calls of a turn run
together.

## One mission, one session

Everything the loop materialises — checkpoints, inspected schemas — belongs to a session, and a
session can be thrown away whole:

```bash
export DTPIPE_SESSION=mission-7
dtpipe agent --mode plan
...
dtpipe session purge mission-7
```

The environment variable follows the `ssh-agent` pattern, so the flag does not have to be repeated
on every command. `dtpipe session list` shows what a store holds.

## Reading what actually happened

```bash
dtpipe agent --trace session.jsonl "..."
```

The trace records the session as JSON lines: the role prompt the mode selected, the tool catalogue
as it was offered, every step, each turn's verdict. The first two matter most — a wrong tool call
cannot be told apart from a tool that was never offered without them. Connection strings are
sanitised on the way in.

It is a diagnostic for a person to read, not a gate: nothing in dtpipe reads its verdict.

---

See also: [Running in production](production.md) · [YAML jobs](yaml-jobs.md) ·
[Preview and checkpoints](preview-and-checkpoints.md) ·
[REFERENCE.md](../../REFERENCE.md#agent-guardrails)
