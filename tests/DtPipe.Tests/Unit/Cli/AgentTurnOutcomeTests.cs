using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using DtPipe.Cli.Agent;
using DtPipe.Cli.Agent.Tui;
using Spectre.Console;
using Xunit;

namespace DtPipe.Tests.Unit.Cli;

/// <summary>
/// A turn always ends with a stated reason, and the summary's verdict matches what
/// actually happened. In particular an empty model response is a failure, not a "COMPLETED".
///
/// <para>
/// The soft cancel extends this: Esc on the full-screen surface
/// stops the model call and nothing else. The session lives, the partial trajectory stays, and the
/// run does <em>not</em> report the 130 reserved for a real interrupt (F16) — the two cancellation
/// sources must stay distinguishable, exactly as <c>LinearPipelineService</c> keeps its own apart.
/// </para>
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

    /// <summary>Runs a caller-supplied script; each call sees its 1-based index and the token.</summary>
    private sealed class ScriptedLlmClient : ILlmClient
    {
        private readonly Func<int, CancellationToken, Task<LlmResponse>> _script;
        private int _calls;

        public ScriptedLlmClient(Func<int, CancellationToken, Task<LlmResponse>> script) => _script = script;

        public string ProviderName => "scripted";
        public Task<List<string>> ListModelsAsync(string baseUrl, CancellationToken ct = default) => Task.FromResult(new List<string>());

        public Task<LlmResponse> ChatAsync(string baseUrl, string model, List<ChatMessage> messages, List<ToolDefinition> tools,
            int maxTokens = 16384, double temperature = 0.7, int? seed = null, CancellationToken ct = default)
            => _script(++_calls, ct);
    }

    /// <summary>Answers every call with a fixed result, after running a hook that can cancel.</summary>
    private sealed class HookedToolProvider : IAgentToolProvider
    {
        private readonly Func<CancellationToken, Task> _hook;

        public HookedToolProvider(Func<CancellationToken, Task> hook) => _hook = hook;

        public List<ToolDefinition> GetToolDefinitions() => new();
        public List<ToolDefinition> GetToolDefinitions(AgentMode mode) => new();

        public async Task<ToolResult> InvokeToolAsync(string toolName, JsonElement args, CancellationToken ct)
        {
            await _hook(ct);
            return ToolResult.Success("{\"rows\":5}");
        }
    }

    [Fact]
    public async Task Esc_During_A_Model_Call_Interrupts_The_Turn_And_Keeps_What_It_Had()
    {
        // The first call asks for a tool, the second is the one the user stops. The soft token is
        // tripped from inside that call, which is exactly what the key handler does from the UI
        // thread while the worker sits in the request.
        using var soft = new CancellationTokenSource();
        var llm = new ScriptedLlmClient(async (call, ct) =>
        {
            if (call == 1) return ToolCall("inspect");
            await soft.CancelAsync();
            await Task.Delay(Timeout.Infinite, ct);
            return Text("unreachable");
        });

        IAnsiConsole console = AnsiConsole.Create(new AnsiConsoleSettings());
        var executor = new AgentExecutor(new NoopToolProvider(), llm, new AgentTui(console), console);
        var log = new TranscriptLog();

        // No exception escapes: a soft stop is a stated outcome, not the cancellation the CLI
        // turns into exit 130.
        var summary = await executor.RunTurnOnSurfaceAsync("mission", "m", "http://x", new AgentOptions(),
            maxIterations: 5, executor.CreateSurfaceView(log), soft.Token, CancellationToken.None);

        Assert.Equal(TurnOutcome.UserInterrupted, summary.Outcome);
        Assert.Equal(TurnOutcome.UserInterrupted, executor.LastTurnOutcome);
        Assert.Equal(1, summary.ExitCode);
        Assert.NotEqual(130, summary.ExitCode);

        // The work done before the interrupt is kept — the tool call from the first iteration.
        Assert.Contains(executor.Trajectory.Steps, s => s.ToolName == "inspect");
        Assert.Contains(executor.Trajectory.Steps, s => s.Reasoning.Contains("Interrupted"));
    }

    [Fact]
    public async Task Esc_Never_Cancels_A_Tool_Call_Already_In_Flight()
    {
        // A call already running against a database is not something a keystroke may abandon
        // halfway: the soft token reaches the model call only, tools run on the session token.
        using var soft = new CancellationTokenSource();
        bool toolFinished = false;

        var tools = new HookedToolProvider(async ct =>
        {
            await soft.CancelAsync();          // the user hits Esc while the tool is running
            await Task.Delay(20, ct);          // would throw here if the soft token reached tools
            toolFinished = true;
        });

        var llm = new ScriptedLlmClient(async (call, ct) =>
        {
            if (call == 1) return ToolCall("inspect");
            await Task.Delay(Timeout.Infinite, ct);
            return Text("unreachable");
        });

        IAnsiConsole console = AnsiConsole.Create(new AnsiConsoleSettings());
        var executor = new AgentExecutor(tools, llm, new AgentTui(console), console);
        var log = new TranscriptLog();

        var summary = await executor.RunTurnOnSurfaceAsync("mission", "m", "http://x", new AgentOptions(),
            maxIterations: 5, executor.CreateSurfaceView(log), soft.Token, CancellationToken.None);

        Assert.True(toolFinished, "the tool call was cut short by a soft cancel");
        Assert.Contains(executor.Trajectory.Steps, s => s.ToolName == "inspect" && s.ToolResult!.Contains("rows"));
        Assert.Equal(TurnOutcome.UserInterrupted, summary.Outcome);
    }

    [Fact]
    public async Task Ctrl_C_Still_Escapes_As_A_Cancellation_So_The_Run_Reports_130()
    {
        // The other half of the split: the session token is the process one, and cancelling it
        // must still surface as an OperationCanceledException for AgentCommand to map to 130.
        using var session = new CancellationTokenSource();
        var llm = new ScriptedLlmClient(async (_, ct) =>
        {
            await session.CancelAsync();
            await Task.Delay(Timeout.Infinite, ct);
            return Text("unreachable");
        });

        IAnsiConsole console = AnsiConsole.Create(new AnsiConsoleSettings());
        var executor = new AgentExecutor(new NoopToolProvider(), llm, new AgentTui(console), console);
        var log = new TranscriptLog();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            executor.RunTurnOnSurfaceAsync("mission", "m", "http://x", new AgentOptions(),
                maxIterations: 5, executor.CreateSurfaceView(log), CancellationToken.None, session.Token));
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
