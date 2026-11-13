# The agent

[← Documentation](../README.md)

The agent is a panel of `dtpipe studio` in which a language model designs a job with you, checks it
against your real sources, saves it and runs it, each write after you confirm it. `dtpipe mcp`
offers the same tools to an AI assistant you already use.

```bash
dtpipe studio ./jobs --llm-provider ollama --llm-model qwen2.5-coder:7b
```

Open the address it prints. The panel on the right takes a mission:

> *sales.csv has columns id, name, amount. Write a job that keeps the rows whose amount is above 15
> and writes them to big.csv. Check it with a dry-run, save it as big.yaml, then run it.*

The agent inspects the source, writes the job, previews it on a few rows, then asks to save it. Once
saved, the job runs without a model: you run it with **Write targets…**.

## What the agent may do

| The agent… | You confirm |
|:---|:---|
| reads sources, adapter and transformer help, the folder's jobs | no |
| validates a job and previews it (`dry-run`): nothing is written | no |
| gives a job to check or preview: it lands on the open job's cards, unsaved, as soon as the call returns | no, nothing saved |
| saves a job into the folder | **yes**: the card shows the text the studio will write |
| tells you to run a saved job with **Write targets…** | no: it writes no target itself |

Nothing waits on a timer: without your answer, nothing is saved. **Refuse** tells the agent you
declined; it is not an error. The agent never writes a target: when the job is ready, save it and
run it yourself. A turn has no step limit; **Stop** ends it. The panel shows the turn, its step
and the context the model last read. Drag its left edge to resize it. Publishing, the library and
the coordinator stay your gestures.

## The open job

The open job always goes with your message, as the studio composes it, so you can ask "fix the
filter of this job"; the agent works on what you see, and what it gives lands on the same cards,
unsaved, until you save. With no job open, what it gives opens as a new job (`agent-job.yaml`). If
you change the cards while it works, your changes stay: its job waits behind **Apply to the open
job**. A job it saves under the open job's path is read back into the cards, and a run it starts
shows in the Run tab.

When the job holds a credential written as a value, such as `Password=secret` in a connection
string, a card names where it is, never the value, and asks what to do: send anyway, or cancel the
turn. Reading a job of the folder asks the same, and you may keep it here. Write credentials as
`${{NAME}}` or `keyring://alias` and the question never comes up: see [Secrets](secrets.md).

## Where your data goes

What the tools read (schemas, sample rows) and the open job go to the model's address. With a local
Ollama it stays on your machine; the panel warns when the address is elsewhere.

```bash
# A remote model: the key is a reference, never the key itself
dtpipe studio ./jobs --llm-provider openai --llm-model gpt-4o --llm-key keyring://openai
```

To use a studio on a server from your machine, forward its port with the same number on both sides:
`dtpipe studio ./jobs --port 8765` there, `ssh -L 8765:127.0.0.1:8765 server` here, then open the
printed address.

## Traces

```bash
dtpipe studio ./jobs --llm-provider ollama --agent-trace ./traces/
```

The trace records the session as JSON lines: the role prompt, the tools offered, every step and each
verdict, with connection strings masked. Under each answer, a thumb up, a thumb down and a comment
icon add your verdict to it; they work only with `--agent-trace`. It is for reading, not for
automation.

## Pitfalls

- A credential under a key dtpipe does not know is not flagged; a column named like one may be.
- A job that reads or writes a brick runs from the page, not from the agent.
- Weaker models loop or invent YAML keys: the agent's `dry-run` catches most of it, read its plan.

---

See also: [Preview and checkpoints](preview-and-checkpoints.md) · Every option of the studio and the
MCP tools: [REFERENCE.md](../../REFERENCE.md#the-agent)
