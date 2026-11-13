---
paths:
  - "src/DtPipe/Cli/Mcp/**"
  - "src/DtPipe/Cli/Agent/**"
  - "src/DtPipe/Cli/Studio/StudioAgent*.cs"
  - "src/DtPipe/Cli/Studio/StudioServer.Agent.cs"
---

# MCP server and agent

`dtpipe mcp` (STDIO) and the agent loop (`Cli/Agent/`), whose one host is `dtpipe studio`. Never
enumerate tool names here; see `REFERENCE.md#model-context-protocol-mcp-server`. Hardening invariants, fail-closed:
**F1** the model is never offered a `[WritesToTarget]` tool (reflected, never listed), and the model
loop refuses a call to any tool it did not offer, `_` and `-` spellings alike; the refusal lives in
the loop, not the provider `[CI: AgentCatalogueTests, AgentExecutorCatalogueTests]` ·
**F2** `ISqlSafetyPolicy` + `IApprovalGate`; a write by `execute-yaml-job` needs `apply` + approval;
the studio's agent writes a job only through `save-job`, waiting in its call for the person's answer
to a card, which nothing of the command line, the environment or the model skips, and offers no tool
that writes a target: a real run is the page's `Write targets…`
`[CI: StudioAgentTests]` ·
**F4** fact cache + `ConversationWindowManager.Compact` ·
**F5** every tool call of a turn runs; those that run a pipeline (`[RunsPipeline]`, reflected, compared
through `McpToolReflector.Normalise`) one after the other ·
**F6** `yamlContent` is the sole plan source ·
**F7** `analyze-traces.sh --gate` fails on unhandled MCP errors or a failed mission.

MCP directives: reflect help from `[Description]` / `[ComponentHelp]`, never hardcode it; execute
in memory via `JobFileParser` + `JobRunner`, no temp files; discover tables
on `inspect`; default `apply=false`. `[CI: ExecuteYamlJobGuardrailTests — the last one]`
`[unchecked: the rest]` Detail: `src/DtPipe/CLAUDE.md`.
