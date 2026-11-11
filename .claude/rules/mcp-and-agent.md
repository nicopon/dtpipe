---
paths:
  - "src/DtPipe/Cli/Mcp/**"
  - "src/DtPipe/Cli/Agent/**"
---

# MCP server and agent

`dtpipe mcp` (STDIO) and `dtpipe agent`. Never enumerate tool names here; see
`REFERENCE.md#model-context-protocol-mcp-server`. Hardening invariants, fail-closed:
**F1** plan mode hides every `[WritesToTarget]` tool (reflected, never listed) ·
**F2** `ISqlSafetyPolicy` + `IApprovalGate`; a write needs `apply` + approval ·
**F3** determinism available, not default (`--temperature 0 --seed N --repeat N`) ·
**F4** fact cache + `ConversationWindowManager.Compact` ·
**F5** every tool call of a turn runs; those that run a pipeline (`[RunsPipeline]`, reflected) one
after the other ·
**F6** `yamlContent` is the sole plan source ·
**F7** `analyze-traces.sh --gate`; never record a placeholder variance.

MCP directives: reflect help from `[Description]` / `[ComponentHelp]`, never hardcode it; execute
in memory via `JobFileParser` + `JobRunner`, no temp files; discover tables
on `inspect`; default `apply=false`. `[CI: ExecuteYamlJobGuardrailTests — the last one]`
`[unchecked: the rest]` Detail: `src/DtPipe/CLAUDE.md`.
