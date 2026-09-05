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
/// §6 feedback UX: a turn always ends with a stated reason, and the summary's verdict matches what
/// actually happened. In particular an empty model response is a failure, not a "COMPLETED".
/// </summary>
public class AgentTurnOutcomeTests
{
    /// <summary>Returns a fixed scripted sequence; repeats the last entry once exhausted.</summary>
    private sealed class QueuedLlmClient : ILlmClient
    {
        private readonly IReadOnlyList<LlmResponse> _responses;
        private int _i;

        public QueuedLlmClient(params LlmResponse[] responses) => _responses = responses;

        public string ProviderName => "queued";
        public Task<List<string>> ListModelsAsync(string baseUrl, CancellationToken ct = default) => Task.FromResult(new List<string>());

        public Task<LlmResponse> ChatAsync(string baseUrl, string model, List<ChatMessage> messages, List<ToolDefinition> tools,
            int maxTokens = 16384, double temperature = 0.7, int? seed = null, CancellationToken ct = default)
        {
            var r = _responses[Math.Min(_i, _responses.Count - 1)];
            _i++;
            return Task.FromResult(r);
        }
    }

    private sealed class NoopToolProvider : IAgentToolProvider
    {
        public List<ToolDefinition> GetToolDefinitions() => new();
        public List<ToolDefinition> GetToolDefinitions(AgentMode mode) => new();
        public Task<ToolResult> InvokeToolAsync(string toolName, JsonElement args, CancellationToken ct)
            => Task.FromResult(ToolResult.Success("{}"));
    }

    private static AgentExecutor BuildExecutor(ILlmClient llm)
    {
        IAnsiConsole console = AnsiConsole.Create(new AnsiConsoleSettings());
        return new AgentExecutor(new NoopToolProvider(), llm, new AgentTui(console), console);
    }

    private static LlmResponse Text(string content) => new(new ChatMessage("assistant", content), true, null);

    private static LlmResponse ToolCall(string name)
    {
        var tcs = new List<ToolCall> { new("c1", name, JsonDocument.Parse("{}").RootElement.Clone()) };
        return new LlmResponse(new ChatMessage("assistant", "reasoning", null, tcs), true, null);
    }

    [Fact]
    public async Task Substantive_Text_Response_Succeeds()
    {
        var executor = BuildExecutor(new QueuedLlmClient(Text("here is the plan")));

        int code = await executor.RunTurnAsync("mission", "m", "http://x", new AgentOptions(), maxIterations: 5);

        Assert.Equal(0, code);
        Assert.Equal(TurnOutcome.Succeeded, executor.LastTurnOutcome);
    }

    [Theory]
    [InlineData("")]
    [InlineData("   \n  ")]
    public async Task Empty_Response_Is_Not_Reported_As_Success(string emptyContent)
    {
        var executor = BuildExecutor(new QueuedLlmClient(Text(emptyContent)));

        int code = await executor.RunTurnAsync("mission", "m", "http://x", new AgentOptions(), maxIterations: 5);

        Assert.Equal(1, code);
        Assert.Equal(TurnOutcome.EmptyResponse, executor.LastTurnOutcome);
        Assert.Contains(executor.Trajectory.Steps, s => s.IsError);
    }

    [Fact]
    public async Task Running_Out_Of_Iterations_Is_Not_Success()
    {
        // The model never stops calling a tool, so the loop exhausts its budget.
        var executor = BuildExecutor(new QueuedLlmClient(ToolCall("inspect")));

        int code = await executor.RunTurnAsync("mission", "m", "http://x", new AgentOptions(), maxIterations: 3);

        Assert.Equal(1, code);
        Assert.Equal(TurnOutcome.MaxIterationsReached, executor.LastTurnOutcome);
    }

    [Fact]
    public async Task Llm_Error_Response_Is_Classified_As_LlmError()
    {
        var errored = new LlmResponse(new ChatMessage("assistant", null), true,
            "Ollama at http://x did not respond within 300s.");
        var executor = BuildExecutor(new QueuedLlmClient(errored));

        int code = await executor.RunTurnAsync("mission", "m", "http://x", new AgentOptions(), maxIterations: 5);

        Assert.Equal(1, code);
        Assert.Equal(TurnOutcome.LlmError, executor.LastTurnOutcome);
        Assert.Contains(executor.Trajectory.Steps, s => s.IsError && s.Reasoning.Contains("did not respond"));
    }

    [Fact]
    public async Task A_Detected_Repetition_Loop_Is_Classified_Distinctly_From_A_Plain_LlmError()
    {
        // A client-level repetition guard (OllamaClient/OpenAiClient) reports the loop as an error
        // with this exact, shared message — the executor must tell it apart from a generic
        // connection/timeout failure so the user gets guidance about the model, not the endpoint.
        var errored = new LlmResponse(new ChatMessage("assistant", null), true, RepetitionGuard.DetectedMessage);
        var executor = BuildExecutor(new QueuedLlmClient(errored));

        int code = await executor.RunTurnAsync("mission", "m", "http://x", new AgentOptions(), maxIterations: 5);

        Assert.Equal(1, code);
        Assert.Equal(TurnOutcome.RepetitionDetected, executor.LastTurnOutcome);
    }
}
