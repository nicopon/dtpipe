using System.Collections.Generic;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace DtPipe.Cli.Agent;

/// <summary>
/// Abstraction for providing tools to the agent.
/// Allows decoupling the agent execution loop from DtPipeMcpTools.
/// </summary>
public interface IAgentToolProvider
{
    /// <summary>
    /// The tool definitions the model is offered. A tool that declares it writes to a target is
    /// never among them (F1); the model only plans and validates.
    /// </summary>
    List<ToolDefinition> GetToolDefinitions();

    Task<ToolResult> InvokeToolAsync(string toolName, JsonElement args, CancellationToken ct);

    /// <summary>
    /// True when <paramref name="toolName"/> executes a run in this process. The agent runs such
    /// calls one at a time, in the order of the turn (F5). The default is false: a fake has no
    /// engine behind it.
    /// </summary>
    bool RunsPipeline(string toolName) => false;
}
