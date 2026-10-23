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
/// F6 tests: the single YAML path. The <c>yamlContent</c> tool-call argument is the sole source of
/// the plan YAML; free-text content is never parsed.
/// </summary>
public class AgentYamlExtractionTests
{
    private sealed class CountingToolProvider : IAgentToolProvider
    {
        public string? LastYamlContentArg { get; private set; }
        public int CallCount { get; private set; }

        public List<ToolDefinition> GetToolDefinitions() => new();
        public List<ToolDefinition> GetToolDefinitions(AgentMode mode) => new();

        public Task<ToolResult> InvokeToolAsync(string toolName, JsonElement args, CancellationToken ct)
        {
            CallCount++;
            if (args.ValueKind == JsonValueKind.Object &&
                args.TryGetProperty("yamlContent", out var yamlProp) &&
                yamlProp.ValueKind == JsonValueKind.String)
            {
                LastYamlContentArg = yamlProp.GetString();
            }

            return Task.FromResult(ToolResult.Success("{}"));
        }
    }

    private sealed class QueuedLlmClient : ILlmClient
    {
        private readonly IReadOnlyList<LlmResponse> _responses;

        public QueuedLlmClient(IEnumerable<LlmResponse> responses)
        {
            _responses = responses.ToList();
        }

        public string ProviderName => "queued";
        public Task<List<string>> ListModelsAsync(string baseUrl, CancellationToken ct = default) => Task.FromResult(new List<string>());

        public Task<LlmResponse> ChatAsync(string baseUrl, string model, List<ChatMessage> messages, List<ToolDefinition> tools, int maxTokens = 16384, double temperature = 0.7, int? seed = null, CancellationToken ct = default)
        {
            int i = Math.Min(_callIndex, _responses.Count - 1);
            _callIndex++;
            return Task.FromResult(_responses[i]);
        }

        private int _callIndex;
    }

    private static AgentExecutor BuildExecutor(IAgentToolProvider tools, ILlmClient llm)
    {
        IAnsiConsole console = Spectre.Console.AnsiConsole.Create(new Spectre.Console.AnsiConsoleSettings());
        var tui = new AgentTui(console);
        return new AgentExecutor(tools, llm, tui, console);
    }

    private static LlmResponse WithToolCall(string content, string toolName, string argsJson)
    {
        var tcs = new List<ToolCall>
           {
             new ("call-1", toolName, JsonDocument.Parse(argsJson).RootElement.Clone())
           };
        return new LlmResponse(new ChatMessage("assistant", content, null, tcs), true, null);
    }

    private static LlmResponse TextOnly(string content)
       => new(new ChatMessage("assistant", content), true, null);

    [Fact]
    public async Task YamlContent_Argument_Is_The_Sole_Source()
    {
        string argYaml = "input: from-arg.csv\noutput: out-arg.csv\n";
        string contentYaml = "input: from-content.csv\noutput: out-content.csv\n";
        string content = "here is a skeleton:\n" + "```yaml\n" + contentYaml + "```\n";

        var provider = new CountingToolProvider();
        var llm = new QueuedLlmClient(new[]
           {
            // A tool call whose argument carries the plan, while the same message content also
            // contains a (different) fenced block that must be ignored entirely.
           WithToolCall(content, "validate-yaml-job",
              $"{{\"yamlContent\": {System.Text.Json.JsonSerializer.Serialize(argYaml)}}}"),
           TextOnly("done")
           });

        var executor = BuildExecutor(provider, llm);
        await executor.RunTurnAsync("mission", "model", "http://localhost:11434", maxIterations: 5);

        Assert.Equal(argYaml, executor.Trajectory.LastGeneratedYaml);
    }

    [Fact]
    public async Task FreeTextYamlBlock_Is_Ignored()
    {
        string contentYaml = "input: from-content.csv\noutput: out-content.csv\n";
        string content = "skeleton:\n" + "```yaml\n" + contentYaml + "```\n";

        var provider = new CountingToolProvider();
        // No tool call: a fenced YAML block in free text must not become a plan anymore.
        var llm = new QueuedLlmClient(new[] { TextOnly(content) });

        var executor = BuildExecutor(provider, llm);
        await executor.RunTurnAsync("mission", "model", "http://localhost:11434", maxIterations: 5);

        Assert.Null(executor.Trajectory.LastGeneratedYaml);
    }

    [Fact]
    public async Task No_Yaml_Produces_No_Plan()
    {
        var provider = new CountingToolProvider();
        var llm = new QueuedLlmClient(new[] { TextOnly("planning done, no yaml") });

        var executor = BuildExecutor(provider, llm);
        await executor.RunTurnAsync("mission", "model", "http://localhost:11434", maxIterations: 5);

        Assert.Null(executor.Trajectory.LastGeneratedYaml);
    }
}
