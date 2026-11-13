---
paths:
  - "src/DtPipe/Cli/Mcp/**"
  - "src/DtPipe/Cli/Agent/**"
---

# MCP server and agent

`dtpipe mcp` (STDIO) and the agent loop (`Cli/Agent/`), which has no command of its own. Never
enumerate tool names here; see `REFERENCE.md#model-context-protocol-mcp-server`. Hardening invariants, fail-closed:
**F1** the model is never offered a `[WritesToTarget]` tool (reflected, never listed), and the model
loop refuses a call to any tool it did not offer, `_` and `-` spellings alike; the refusal lives in
the loop, not the provider `[CI: AgentCatalogueTests, AgentExecutorCatalogueTests]` ·
**F2** `ISqlSafetyPolicy` + `IApprovalGate`; a write by `execute-yaml-job` needs `apply` + approval ·
**F4** fact cache + `ConversationWindowManager.Compact` ·
**F5** every tool call of a turn runs; those that run a pipeline (`[RunsPipeline]`, reflected) one
after the other ·
**F6** `yamlContent` is the sole plan source ·
**F7** `analyze-traces.sh --gate`; never record a placeholder variance.

MCP directives: reflect help from `[Description]` / `[ComponentHelp]`, never hardcode it; execute
in memory via `JobFileParser` + `JobRunner`, no temp files; discover tables
on `inspect`; default `apply=false`. `[CI: ExecuteYamlJobGuardrailTests — the last one]`
`[unchecked: the rest]` Detail: `src/DtPipe/CLAUDE.md`.
