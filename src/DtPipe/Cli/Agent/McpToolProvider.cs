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
    private readonly List<ToolDefinition> _offered;
    private readonly IReadOnlySet<string> _runningTools;

    /// <summary>
    /// True when <paramref name="toolName"/> is the ask-user terminator, tolerating the underscore
    /// spelling a model may emit (<c>ask_user</c>) as well as the canonical <c>ask-user</c>.
    /// </summary>
    internal static bool IsAskUser(string? toolName) =>
        toolName is not null
        && (toolName.Equals("ask-user", StringComparison.OrdinalIgnoreCase)
            || toolName.Equals("ask_user", StringComparison.OrdinalIgnoreCase));

    /// <summary>
    /// A tool the model can call to stop and ask the user a question. It is defined here, not on
    /// the MCP tool surface: outside an agent session there is no user to answer, so an MCP client
    /// should never see it. The planning loop intercepts the call and never dispatches it — the
    /// user's next message is the answer.
    /// </summary>
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
        var toolsType = toolsInstance.GetType();
        var writing = ToolModePolicy.WritingTools(toolsType);

        // The agent only plans and validates: it must never be able to trigger a real write.
        // Every tool marked as writing to a target is left out of what the model is offered.
        _offered = McpToolReflector.BuildToolDefinitions(toolsType)
            .Where(d => !writing.Contains(d.Name))
            .ToList();
        _offered.Add(AskUserTool);
        _runningTools = ToolModePolicy.RunningTools(toolsType);
    }

    /// <inheritdoc />
    public bool RunsPipeline(string toolName) => _runningTools.Contains(toolName);

    public List<ToolDefinition> GetToolDefinitions() => _offered;

    public async Task<ToolResult> InvokeToolAsync(string toolName, JsonElement args, CancellationToken ct)
    {
        // 'ask-user' is a turn terminator handled by the planning loop and never dispatched.
        // A call reaching here means the loop's interception was bypassed — fail loud rather
        // than hand the model a reflection "tool not found".
        if (IsAskUser(toolName))
            throw new InvalidOperationException("'ask-user' is a turn terminator; it must not be dispatched as a tool.");

        // The LLM drove this call — nobody is watching for a keypress, even though the process
        // may share a real interactive console with the person who started it (NonInteractiveGuard).
        using var _ = NonInteractiveGuard.Suppress();
        var rawResult = await McpToolReflector.InvokeToolAsync(_toolsInstance, toolName, args, ct);
        return ToolResult.FromJson(rawResult);
    }
}

/// <summary>
/// Which tools carry the marks the agent reads. Fail-closed: a tool that writes to a target is
/// never offered to the model.
///
/// <para>
/// Which tools those are is read off <see cref="WritesToTargetAttribute"/>, never listed here.
/// A list in this file is a second place to edit when a tool is added, and the first symptom of
/// forgetting it is a model that can write.
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
}
