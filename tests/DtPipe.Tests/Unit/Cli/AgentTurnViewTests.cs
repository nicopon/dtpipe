using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using DtPipe.Cli.Agent;
using DtPipe.Tests.Helpers;
using Xunit;

namespace DtPipe.Tests.Unit.Cli;

/// <summary>
/// A turn needs a view and nothing else: no console, no terminal. The loop tells the view what is in
/// flight, and which kind of model call it makes follows what the view and the client can do.
/// </summary>
public class AgentTurnViewTests
{
    private sealed class Tools : IAgentToolProvider
    {
        public List<ToolDefinition> GetToolDefinitions()
            => new[] { "validate-yaml-job" }.Select(n => new ToolDefinition(n, "", default)).ToList();

        public Task<ToolResult> InvokeToolAsync(string toolName, JsonElement args, CancellationToken ct)
            => Task.FromResult(ToolResult.Success("{\"valid\":true}"));
    }

    private class Client : ILlmClient
    {
        private int _calls;
        public string ProviderName => "scripted";
        public Task<List<string>> ListModelsAsync(string baseUrl, CancellationToken ct = default) => Task.FromResult(new List<string>());

        public Task<LlmResponse> ChatAsync(string baseUrl, string model, List<ChatMessage> messages, List<ToolDefinition> tools,
            int maxTokens = 16384, double temperature = 0.7, int? seed = null, CancellationToken ct = default)
            => Task.FromResult(Next());

        protected LlmResponse Next()
        {
            if (_calls++ > 0)
                return new LlmResponse(new ChatMessage("assistant", "done"), true, null);

            var args = JsonSerializer.SerializeToElement(new { yamlContent = "main:\n  input: csv:a.csv\n" });
            var calls = new List<ToolCall> { new("c1", "validate-yaml-job", args) };
            return new LlmResponse(new ChatMessage("assistant", "checking", null, calls), true, null);
        }
    }

    private sealed class StreamingClient : Client, IStreamingLlmClient
    {
        public Task<LlmResponse> ChatStreamAsync(string baseUrl, string model, List<ChatMessage> messages,
            List<ToolDefinition> tools, ILlmStreamObserver observer, int numCtx = 16384,
            double temperature = 0.7, int? seed = null, CancellationToken ct = default)
            => Task.FromResult(Next());
    }

    [Fact]
    public async Task A_Turn_Tells_The_View_What_Is_In_Flight_With_No_Console_Behind_It()
    {
        var view = new RecordingTurnView();
        var executor = new AgentExecutor(new Tools(), new Client());

        var summary = await executor.TurnAsync("mission", view: view, maxIterations: 5);

        Assert.Equal(TurnOutcome.Succeeded, summary.Outcome);
        Assert.Equal(new[] { "block 1", "plan", "result validate-yaml-job", "block 2" }, view.Events);
    }

    [Fact]
    public async Task A_View_That_Can_Stream_Gets_The_Streaming_Call_From_A_Client_That_Can()
    {
        var view = new RecordingTurnView { CanStream = true };
        var executor = new AgentExecutor(new Tools(), new StreamingClient());

        await executor.TurnAsync("mission", view: view, maxIterations: 5);

        Assert.Equal(new[] { "stream 1", "plan", "result validate-yaml-job", "stream 2" }, view.Events);
    }

    [Fact]
    public async Task A_View_That_Cannot_Stream_Gets_The_Blocking_Call_Even_From_A_Client_That_Can()
    {
        var view = new RecordingTurnView { CanStream = false };
        var executor = new AgentExecutor(new Tools(), new StreamingClient());

        await executor.TurnAsync("mission", view: view, maxIterations: 5);

        Assert.All(view.Events.Where(e => e.StartsWith("stream", StringComparison.Ordinal) || e.StartsWith("block", StringComparison.Ordinal)),
            e => Assert.StartsWith("block", e));
    }

    [Fact]
    public async Task A_Client_That_Cannot_Stream_Gets_The_Blocking_Call_From_A_View_That_Can()
    {
        var view = new RecordingTurnView { CanStream = true };
        var executor = new AgentExecutor(new Tools(), new Client());

        await executor.TurnAsync("mission", view: view, maxIterations: 5);

        Assert.Contains("block 1", view.Events);
        Assert.DoesNotContain(view.Events, e => e.StartsWith("stream", StringComparison.Ordinal));
    }
}
