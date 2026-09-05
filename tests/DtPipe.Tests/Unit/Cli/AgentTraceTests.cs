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
/// §6 feedback UX: token usage and the model's chain of thought reach the trajectory so the step
/// inspector and the compact trace can show them.
/// </summary>
public class AgentTraceTests
{
    private sealed class OneShotLlmClient : ILlmClient
    {
        private readonly LlmResponse _response;
        public OneShotLlmClient(LlmResponse response) => _response = response;
        public string ProviderName => "oneshot";
        public Task<List<string>> ListModelsAsync(string baseUrl, CancellationToken ct = default) => Task.FromResult(new List<string>());
        public Task<LlmResponse> ChatAsync(string baseUrl, string model, List<ChatMessage> messages, List<ToolDefinition> tools,
            int numCtx = 16384, double temperature = 0.7, int? seed = null, CancellationToken ct = default)
            => Task.FromResult(_response);
    }

    private sealed class NoopToolProvider : IAgentToolProvider
    {
        public List<ToolDefinition> GetToolDefinitions() => new();
        public List<ToolDefinition> GetToolDefinitions(AgentMode mode) => new();
        public Task<ToolResult> InvokeToolAsync(string toolName, JsonElement args, CancellationToken ct)
            => Task.FromResult(ToolResult.Success("{}"));
    }

    private static AgentExecutor Build(LlmResponse response)
    {
        IAnsiConsole console = AnsiConsole.Create(new AnsiConsoleSettings());
        return new AgentExecutor(new NoopToolProvider(), new OneShotLlmClient(response), new AgentTui(console), console);
    }

    [Fact]
    public async Task Thinking_And_Usage_Are_Recorded_On_The_Trajectory_Step()
    {
        var usage = new LlmUsage(120, 340, TimeSpan.FromSeconds(2), TimeSpan.FromSeconds(5));
        var response = new LlmResponse(new ChatMessage("assistant", "here is the plan"), true, null, usage, "first I will inspect the source");

        var executor = Build(response);
        await executor.RunTurnAsync("mission", "m", "http://x", new AgentOptions(), maxIterations: 3);

        var step = executor.Trajectory.Steps.Single();
        Assert.Equal("first I will inspect the source", step.Thinking);
        Assert.NotNull(step.Usage);
        Assert.Equal(340, step.Usage!.CompletionTokens);
        Assert.Equal(68.0, step.Usage.TokensPerSecond!.Value, 3);
    }

    [Fact]
    public async Task A_Response_Without_Usage_Leaves_The_Step_Usage_Null()
    {
        var executor = Build(new LlmResponse(new ChatMessage("assistant", "done"), true, null));
        await executor.RunTurnAsync("mission", "m", "http://x", new AgentOptions(), maxIterations: 3);

        Assert.Null(executor.Trajectory.Steps.Single().Usage);
    }
}
