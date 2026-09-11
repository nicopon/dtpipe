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
    /// <summary>Return all available tool definitions.</summary>
    List<ToolDefinition> GetToolDefinitions();

        /// <summary>
        /// Return the tool definitions allowed in a given <see cref="AgentMode"/>.
        /// In <see cref="AgentMode.Plan"/> the destructive/execution tool
        /// <c>execute-yaml-job</c> is excluded; the LLM only plans and validates.
        /// </summary>
    List<ToolDefinition> GetToolDefinitions(AgentMode mode);

    Task<ToolResult> InvokeToolAsync(string toolName, JsonElement args, CancellationToken ct);

    /// <summary>
    /// True when the tools behind this provider are dtpipe's own, so a validated plan can be handed
    /// to the execution tool. False for a foreign MCP server: there is no dtpipe engine behind its
    /// catalogue, and a tool that happens to share the name is a coincidence, not the engine. The
    /// default is true so a provider says nothing unless it is not dtpipe.
    /// </summary>
    bool CanRunDtPipePlans => true;
}