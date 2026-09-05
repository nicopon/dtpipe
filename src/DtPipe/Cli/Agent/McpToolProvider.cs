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

    public McpToolProvider(object toolsInstance)
       {
         _toolsInstance = toolsInstance;
         _toolsType = toolsInstance.GetType();
         _definitions = McpToolReflector.BuildToolDefinitions(_toolsType);
       }

    public List<ToolDefinition> GetToolDefinitions() => _definitions;

    public List<ToolDefinition> GetToolDefinitions(AgentMode mode)
       {
         // In Plan mode the agent only plans & validates: it must never be able to trigger
          // a real write. The execution tool is excluded from the allow-list.
        if (mode == AgentMode.Plan)
          {
            return _definitions
                  .Where(d => !ToolModePolicy.IsBlockedInPlanMode(d.Name))
                  .ToList();
          }

          // Execute / Autonomous: the full tool set is available, but execution remains
          // gated by the guardrails (dry-run by default, approval gate, SQL safety policy).
        return _definitions;
       }

    public async Task<ToolResult> InvokeToolAsync(string toolName, JsonElement args, CancellationToken ct)
       {
           // The LLM drove this call — nobody is watching for a keypress, even though the process
           // shares a real interactive console with the agent's own TUI (NonInteractiveGuard).
        using var _ = NonInteractiveGuard.Suppress();
        var rawResult = await McpToolReflector.InvokeToolAsync(_toolsInstance, toolName, args, ct);
        return ToolResult.FromJson(rawResult);
       }
 }

 /// <summary>
 /// Central definition of which tools are available in each <see cref="AgentMode"/>.
 /// KISS + fail-closed: <c>execute-yaml-job</c> is the only execution tool and is blocked
 /// in <see cref="AgentMode.Plan"/>.
 /// </summary>
public static class ToolModePolicy
 {
    private static readonly IReadOnlyList<string> ExecutionToolNames = new[]
       {
        "execute-yaml-job"
       };

        /// <summary>True when <paramref name="toolName"/> must not be offered in Plan mode.</summary>
    public static bool IsBlockedInPlanMode(string toolName)
       {
        return ExecutionToolNames.Any(n => string.Equals(n, toolName, StringComparison.OrdinalIgnoreCase));
       }

        /// <summary>True when <paramref name="toolName"/> performs a real write / execution.</summary>
    public static bool IsExecutionTool(string toolName)
       {
        return ExecutionToolNames.Any(n => string.Equals(n, toolName, StringComparison.OrdinalIgnoreCase));
       }
 }