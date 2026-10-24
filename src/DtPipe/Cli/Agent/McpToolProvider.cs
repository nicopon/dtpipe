using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using DtPipe.Cli.Mcp;

namespace DtPipe.Cli.Agent;

public class McpToolProvider : IAgentToolProvider
{
    private readonly object _toolsInstance;
    private readonly Type _toolsType;
    private readonly List<ToolDefinition> _definitions;
    private readonly IReadOnlySet<string> _writingTools;
    private readonly IReadOnlySet<string> _runningTools;

    /// <summary>
    /// A tool the model can call to stop and ask the user a question. It is defined here, not on
    /// the MCP tool surface: outside an interactive agent session there is no user to answer, so
    /// an external MCP client should never see it. The planning loop intercepts the call and never
    /// dispatches it — the user's next message is the answer.
    /// </summary>
    /// <summary>
    /// True when <paramref name="toolName"/> is the ask-user terminator, tolerating the underscore
    /// spelling a model may emit (<c>ask_user</c>) as well as the canonical <c>ask-user</c>.
    /// </summary>
    internal static bool IsAskUser(string? toolName) =>
        toolName is not null
        && (toolName.Equals("ask-user", StringComparison.OrdinalIgnoreCase)
            || toolName.Equals("ask_user", StringComparison.OrdinalIgnoreCase));

    internal static ToolDefinition AskUserTool { get; } = new(
        "ask-user",
        "Stop and ask the user a question when a decision is needed that only they can make — a "
        + "target filename, an ambiguous column, a business rule. Ask only what blocks you: one "
        + "focused question, never a closing \"anything else?\".",
        JsonDocument.Parse("""
        {
          "type": "object",
          "properties": {
            "question": { "type": "string", "description": "The single question to put to the user." },
            "options": {
              "type": "array", "items": { "type": "string" },
              "description": "Optional: a short list of choices, when the answer is a pick."
            }
          },
          "required": ["question"]
        }
        """).RootElement.Clone());

    public McpToolProvider(object toolsInstance)
    {
        _toolsInstance = toolsInstance;
        _toolsType = toolsInstance.GetType();
        _definitions = McpToolReflector.BuildToolDefinitions(_toolsType);
        _definitions.Add(AskUserTool);
        _writingTools = ToolModePolicy.WritingTools(_toolsType);
        _runningTools = ToolModePolicy.RunningTools(_toolsType);
    }

    /// <inheritdoc />
    /// <remarks>Stated rather than inherited: these tools are dtpipe's own engine.</remarks>
    public bool CanRunDtPipePlans => true;

    /// <inheritdoc />
    public bool RunsPipeline(string toolName) => _runningTools.Contains(toolName);

    public List<ToolDefinition> GetToolDefinitions() => _definitions;

    public List<ToolDefinition> GetToolDefinitions(AgentMode mode)
    {
        // In Plan mode the agent only plans & validates: it must never be able to trigger
        // a real write. Every tool marked as writing to a target is excluded from the allow-list.
        if (mode == AgentMode.Plan)
        {
            return _definitions
                  .Where(d => !_writingTools.Contains(d.Name))
                  .ToList();
        }

        // Execute / Autonomous: the full tool set is available, but execution remains
        // gated by the guardrails (dry-run by default, approval gate, SQL safety policy).
        return _definitions;
    }

    public async Task<ToolResult> InvokeToolAsync(string toolName, JsonElement args, CancellationToken ct)
    {
        // 'ask-user' is a turn terminator handled by the planning loop and never dispatched.
        // A call reaching here means the loop's interception was bypassed — fail loud rather
        // than hand the model a reflection "tool not found".
        if (IsAskUser(toolName))
            throw new InvalidOperationException("'ask-user' is a turn terminator; it must not be dispatched as a tool.");

        // The LLM drove this call — nobody is watching for a keypress, even though the process
        // shares a real interactive console with the agent's own TUI (NonInteractiveGuard).
        using var _ = NonInteractiveGuard.Suppress();
        var rawResult = await McpToolReflector.InvokeToolAsync(_toolsInstance, toolName, args, ct);
        return ToolResult.FromJson(rawResult);
    }
}

/// <summary>
/// Central definition of which tools are available in each <see cref="AgentMode"/>.
/// Fail-closed: a tool that writes to a target is blocked in <see cref="AgentMode.Plan"/>.
///
/// <para>
/// Which tools those are is read off <see cref="WritesToTargetAttribute"/>, never listed here.
/// A list in this file is a second place to edit when a tool is added, and the first symptom of
/// forgetting it is a planner that can write.
/// </para>
/// </summary>
public static class ToolModePolicy
{
    /// <summary>The tools on <paramref name="toolsType"/> that declare they write to a target.</summary>
    public static IReadOnlySet<string> WritingTools(Type toolsType) =>
        McpToolReflector.ToolsMarked<WritesToTargetAttribute>(toolsType);

    /// <summary>
    /// The tools on <paramref name="toolsType"/> that declare they execute a run, read off
    /// <see cref="RunsPipelineAttribute"/> like the writing set.
    /// </summary>
    public static IReadOnlySet<string> RunningTools(Type toolsType) =>
        McpToolReflector.ToolsMarked<RunsPipelineAttribute>(toolsType);

    private static readonly IReadOnlySet<string> DtPipeWritingTools = WritingTools(typeof(DtPipeMcpTools));

    /// <summary>True when <paramref name="toolName"/> must not be offered in Plan mode.</summary>
    public static bool IsBlockedInPlanMode(string toolName) => IsExecutionTool(toolName);

    /// <summary>True when <paramref name="toolName"/> performs a real write / execution.</summary>
    public static bool IsExecutionTool(string toolName) => DtPipeWritingTools.Contains(toolName);
}
