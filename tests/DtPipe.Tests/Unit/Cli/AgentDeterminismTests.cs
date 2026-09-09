using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using DtPipe.Cli.Agent;
using Moq;
using Spectre.Console;
using Xunit;

namespace DtPipe.Tests.Unit.Cli;

public class AgentDeterminismTests
{
     /// <summary>An empty, non-executing tool provider for planning-only runs.</summary>
    private sealed class EmptyToolProvider : IAgentToolProvider
    {
        public List<ToolDefinition> GetToolDefinitions() => new();
        public List<ToolDefinition> GetToolDefinitions(AgentMode mode) => new();
        public Task<ToolResult> InvokeToolAsync(string toolName, System.Text.Json.JsonElement args, CancellationToken ct)
            => Task.FromResult(ToolResult.Success("{}"));
    }

      /// <summary>
      /// A scripting fake that records the temperature/seed it was called with and returns a
      /// fixed response. When <c>toolYaml</c> is set, the response carries a 'validate-yaml-job'
      /// tool call whose <c>yamlContent</c> argument is the plan (the F6 single path).
      /// </summary>
      private sealed class ScriptedLlmClient : ILlmClient
       {
        public double? LastTemperature { get; private set; }
        public int? LastSeed { get; private set; }
        public int CallCount { get; private set; }

        private readonly string? _content;
        private readonly string? _toolYaml;

        public ScriptedLlmClient(string? content = null, string? toolYaml = null)
          {
            _content = content;
            _toolYaml = toolYaml;
          }

        public string ProviderName => "scripted";

        public Task<List<string>> ListModelsAsync(string baseUrl, CancellationToken ct = default)
              => Task.FromResult(new List<string>());

        public Task<LlmResponse> ChatAsync(
            string baseUrl,
            string model,
            List<ChatMessage> messages,
            List<ToolDefinition> tools,
            int maxTokens = 16384,
            double temperature = 0.7,
            int? seed = null,
            CancellationToken ct = default)
          {
            LastTemperature = temperature;
            LastSeed = seed;
            CallCount++;

            if (_toolYaml == null)
            {
               var response = new LlmResponse(new ChatMessage("assistant", _content ?? "done"), true, null);
               return Task.FromResult(response);
            }

            var args = System.Text.Json.JsonDocument.Parse(
                $"{{\"yamlContent\": {System.Text.Json.JsonSerializer.Serialize(_toolYaml)}}}").RootElement.Clone();
            var toolCalls = new List<ToolCall> { new ("call-1", "validate-yaml-job", args) };
            var toolResponse = new LlmResponse(new ChatMessage("assistant", _content ?? "done", null, toolCalls), true, null);
            return Task.FromResult(toolResponse);
           }
       }

    private static AgentExecutor BuildExecutor(ScriptedLlmClient llm)
       {
           // A non-interactive Spectre console keeps the run headless under `dotnet test`.
        IAnsiConsole console = Spectre.Console.AnsiConsole.Create(new Spectre.Console.AnsiConsoleSettings());
        var tui = new AgentTui(console);
        return new AgentExecutor(new EmptyToolProvider(), llm, tui, console);
       }

    /// <summary>
    /// The shipped default is 1, and F3's determinism is a capability rather than a default.
    /// Greedy decoding is where a weak quantized model degenerates — dtpipe's own repetition guard
    /// tells the user to raise the temperature — and a measurement run on a 12B local model
    /// returned empty responses at 0 and complete plans at 1. Replay stays available through the
    /// seed, which fixes the sampling at any temperature.
    /// </summary>
    [Fact]
    public void The_Default_Temperature_Is_Not_Greedy_Decoding()
        => Assert.Equal(1.0, new AgentOptions().Temperature);

    [Fact]
    public async Task Temperature_And_Seed_Are_Propagated_To_LlmClient()
    {
        var llm = new ScriptedLlmClient(content: "done");
        var executor = BuildExecutor(llm);

        var options = new AgentOptions { Temperature = 0.0, Seed = 42 };
        await executor.RunTurnAsync("mission", "model", "http://localhost:11434", options, maxIterations: 1);

        Assert.Equal(0.0, llm.LastTemperature);
        Assert.Equal(42, llm.LastSeed);
    }

    [Fact]
    public async Task Repeat_Replicates_The_Planning_Loop()
    {
        var llm = new ScriptedLlmClient(content: "done");
        var executor = BuildExecutor(llm);

        var options = new AgentOptions { Repeat = 3 };
        await executor.RunTurnAsync("mission", "model", "http://localhost:11434", options, maxIterations: 1);

        // 1 primary run + 2 replications = 3 LLM invocations.
        Assert.Equal(3, llm.CallCount);
        Assert.NotNull(executor.Trajectory.Determinism);
        Assert.Equal(3, executor.Trajectory.Determinism!.Repetitions);
    }

    [Fact]
    public async Task Repeat_Without_Yaml_Reports_Zero_Variance()
    {
        var llm = new ScriptedLlmClient(content: "planning complete, no yaml");
        var executor = BuildExecutor(llm);

        var options = new AgentOptions { Repeat = 3 };
        await executor.RunTurnAsync("mission", "model", "http://localhost:11434", options, maxIterations: 1);

        var report = executor.Trajectory.Determinism!;
        Assert.Equal(3, report.Repetitions);
        // No YAML observed in any run => still a single (empty) distinct payload => deterministic.
        Assert.Equal(0, report.Variance);
        Assert.True(report.IsDeterministic);
    }

       [Fact]
    public async Task Repeat_With_Identical_Yaml_Blocks_Reports_Zero_Variance()
        {
        string yaml = "input: csv:a.csv\noutput: csv:b.csv\n";
             // With the single YAML path (F6), the plan arrives through the yamlContent tool
             // argument, not free-text content.
        var llm = new ScriptedLlmClient(toolYaml: yaml);
        var executor = BuildExecutor(llm);

        var options = new AgentOptions { Repeat = 3 };
        await executor.RunTurnAsync("mission", "model", "http://localhost:11434", options, maxIterations: 1);

        var report = executor.Trajectory.Determinism!;
        Assert.Equal(3, report.Repetitions);
        Assert.Single(report.DistinctYaml);
        Assert.Equal(0, report.Variance);
        Assert.True(report.IsDeterministic);
        }
}
