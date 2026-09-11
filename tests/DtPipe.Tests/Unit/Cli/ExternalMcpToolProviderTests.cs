using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using DtPipe.Cli.Agent;
using Spectre.Console;
using Xunit;

namespace DtPipe.Tests.Unit.Cli;

/// <summary>
/// The invariants of driving someone else's MCP server. Connecting to a real child process belongs
/// to a shell test; what is decided here is what the loop does with a catalogue it did not write.
/// </summary>
public class ExternalMcpToolProviderTests
{
    private sealed class DeadLlmClient : ILlmClient
    {
        public string ProviderName => "dead";
        public Task<List<string>> ListModelsAsync(string baseUrl, CancellationToken ct = default)
            => Task.FromResult(new List<string>());
        public Task<LlmResponse> ChatAsync(string baseUrl, string model, List<ChatMessage> messages,
            List<ToolDefinition> tools, int numCtx = 16384, double temperature = 0.7, int? seed = null,
            CancellationToken ct = default)
            => Task.FromResult(new LlmResponse(new ChatMessage("assistant", "x"), true, null));
    }

    private static AgentExecutor Build(IAgentToolProvider tools)
    {
        IAnsiConsole console = AnsiConsole.Create(new AnsiConsoleSettings());
        return new AgentExecutor(tools, new DeadLlmClient(), new AgentTui(console), console);
    }

    /// <summary>A foreign catalogue, including a tool that shares dtpipe's execution name.</summary>
    private sealed class ForeignProvider : IAgentToolProvider
    {
        public bool CanRunDtPipePlans => false;

        public List<ToolDefinition> GetToolDefinitions() =>
        [
            new("connection", "Parent tool with an action discriminant.", Schema()),
            new("execute-yaml-job", "A coincidence of naming, not dtpipe's engine.", Schema()),
        ];

        public List<ToolDefinition> GetToolDefinitions(AgentMode mode) => GetToolDefinitions();

        public Task<ToolResult> InvokeToolAsync(string toolName, JsonElement args, CancellationToken ct)
            => Task.FromResult(ToolResult.Success($"called {toolName}"));

        private static JsonElement Schema()
        {
            using var d = JsonDocument.Parse("""{"type":"object","properties":{}}""");
            return d.RootElement.Clone();
        }
    }

    /// <summary>
    /// The guard that matters: a foreign server carrying a tool called 'execute-yaml-job' must not
    /// become a way to run something through dtpipe's plan path. The refusal is on the provider's
    /// own answer, not on the tool name, because the name is exactly what cannot be trusted.
    /// </summary>
    [Fact]
    public async Task A_Foreign_Catalogue_Cannot_Run_A_Plan_Even_When_It_Names_The_Execution_Tool()
    {
        var provider = new ForeignProvider();
        Assert.Contains(provider.GetToolDefinitions(), t => t.Name == "execute-yaml-job");

        var executor = Build(provider);
        executor.Trajectory.LastGeneratedYaml = "main:\n  input: in.csv\n  output: out.csv\n";

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => executor.ExecuteValidatedPlanAsync(CancellationToken.None));

        Assert.Contains("external MCP server", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>dtpipe's own provider keeps the escape hatch the plan contract promises.</summary>
    [Fact]
    public void DtPipes_Own_Provider_Still_Runs_Plans()
        => Assert.True(new McpToolProvider(new object()).CanRunDtPipePlans);

    /// <summary>
    /// A dtpipe role prompt names dtpipe tools by hand. Sent to a foreign catalogue it briefs the
    /// model about tools it was never offered, which is the one thing that makes a measurement of
    /// that catalogue unreadable.
    /// </summary>
    [Fact]
    public void A_Foreign_Server_Is_Framed_By_Its_Own_Instructions()
    {
        const string theirs = "Call 'connection' with an action before anything else.";

        Assert.Equal(theirs, AgentSystemPrompt.ForExternalServer(theirs));
        Assert.DoesNotContain("dtpipe", AgentSystemPrompt.ForExternalServer(theirs), StringComparison.OrdinalIgnoreCase);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void A_Server_That_Said_Nothing_Gets_The_Neutral_Frame(string? instructions)
    {
        var frame = AgentSystemPrompt.ForExternalServer(instructions);

        Assert.Equal(AgentSystemPrompt.ExternalServerFallbackPrompt, frame);
        // It must not name a dtpipe tool, nor promise a workflow the foreign catalogue cannot serve.
        Assert.DoesNotContain("validate-yaml-job", frame, StringComparison.Ordinal);
        Assert.DoesNotContain("yamlContent", frame, StringComparison.Ordinal);
    }

    /// <summary>
    /// Plan mode hides a tool by the name dtpipe gave it. Applying that filter to a foreign
    /// catalogue would either do nothing or hide an unrelated tool that happens to match — a filter
    /// right by accident. The whole catalogue is offered instead, and the plan path refuses above.
    /// </summary>
    [Fact]
    public void A_Foreign_Catalogue_Is_Not_Filtered_By_Mode()
    {
        var provider = new ForeignProvider();

        Assert.Equal(
            provider.GetToolDefinitions().Select(t => t.Name),
            provider.GetToolDefinitions(AgentMode.Plan).Select(t => t.Name));
    }
}
