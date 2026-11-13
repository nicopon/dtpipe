# The agent

[← Documentation](../README.md)

`dtpipe agent` lets a language model design a pipeline, check it against your real sources and, if
you allow it, run it. `dtpipe mcp` offers the same tools to an AI assistant you already use.

```bash
dtpipe agent
```

It finds the local Ollama models (or uses OpenAI with `--provider`) and asks for a mission:

> *Inspect csv:invoices.csv, compute gross_total = subtotal * (1 + tax), keep the rows above 100,
> and write jsonl:high_invoices.jsonl*

The agent inspects the source, writes a YAML job, validates it, and shows the DAG. Save the job:
from then on it runs without a model.

## What the model may do

| Mode | The model gets |
|:---|:---|
| `--mode plan` (default) | Every tool except those that write. They are not offered at all, and a call to one anyway is refused |
| `--mode execute` | The writing tools too, behind `--apply` and your approval |
| `--mode autonomous` | The same, with approvals given automatically |

The `dry-run` tool is available in plan mode: the model can check its pipeline on a few real rows
without writing anything.

```bash
# Plan only: nothing can be written
dtpipe agent "inspect csv:sales.csv and summarise totals"

# A real write: execute mode, --apply, and your approval for each run
dtpipe agent "load csv:orders.csv into pg" --mode execute --apply

# Only if you know the SQL is destructive, or needs the network
dtpipe agent "..." --apply --allow-destructive --allow-network
```

Without `--apply`, runs are previews. SQL with `DROP`, `DELETE`, `TRUNCATE`, `UPDATE`, `ALTER`,
`INSERT` or `ATTACH` is refused unless `--allow-destructive` is set; network access (`LOAD httpfs`,
a remote file) is refused unless `--allow-network` is set.

## Repeatable results

The default temperature is 1, because weaker models loop at 0. For a repeatable run:

```bash
dtpipe agent "..." --temperature 0 --seed 42 --repeat 3
```

`--repeat N` runs the mission N times from scratch and reports how many different jobs came out,
minus one. `0` means the mission is stable.

## Sessions and traces

Everything a mission saves (checkpoints, schemas) belongs to a session, which you can delete whole:

```bash
export DTPIPE_SESSION=mission-7
dtpipe agent --mode plan
dtpipe session purge mission-7
```

`--trace session.jsonl` records the session as JSON lines: the prompt, the tools offered, every
step and each verdict, with connection strings masked. It is for reading, not for automation.

---

See also: [Preview and checkpoints](preview-and-checkpoints.md) · Every option and the MCP tools:
[REFERENCE.md](../../REFERENCE.md#model-context-protocol-mcp-server)
