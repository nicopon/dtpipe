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
/// The role prompt tells the model that repeating a call unchanged spends a turn for nothing, and
/// nothing applied it. A recorded session emitted the same 2 250-character <c>validate-yaml-job</c>
/// call twice in a row — no reasoning, no thinking between them — and was handed the same error
/// back as if it were news.
///
/// <para>
/// The repeat is judged on the pair: the call runs every time, and the advice is added only when
/// the answer that came back is the one that came back before.
/// </para>
/// </summary>
public class AgentRepeatedCallTests
{
    private const string Marker = "[repeated call]";

    /// <summary>Answers from a script, so a test can make the second answer differ from the first.</summary>
    private sealed class ScriptedToolProvider : IAgentToolProvider
    {
        private readonly Queue<ToolResult> _answers;
        private readonly ToolResult _default;
        public readonly List<string> Invoked = new();

        public ScriptedToolProvider(ToolResult fallback, params ToolResult[] answers)
        {
            _default = fallback;
            _answers = new Queue<ToolResult>(answers);
        }

        public List<ToolDefinition> GetToolDefinitions() => new();
        public List<ToolDefinition> GetToolDefinitions(AgentMode mode) => new();

        public Task<ToolResult> InvokeToolAsync(string toolName, JsonElement args, CancellationToken ct)
        {
            lock (Invoked)
            {
                Invoked.Add(toolName);
                return Task.FromResult(_answers.Count > 0 ? _answers.Dequeue() : _default);
            }
        }
    }

    private sealed class QueuedLlmClient : ILlmClient
    {
        private readonly IReadOnlyList<LlmResponse> _responses;
        private int _n;

        public QueuedLlmClient(params LlmResponse[] responses) => _responses = responses;

        public string ProviderName => "queued";
        public Task<List<string>> ListModelsAsync(string baseUrl, CancellationToken ct = default) => Task.FromResult(new List<string>());

        public Task<LlmResponse> ChatAsync(string baseUrl, string model, List<ChatMessage> messages, List<ToolDefinition> tools,
            int maxTokens = 16384, double temperature = 0.7, int? seed = null, CancellationToken ct = default)
            => Task.FromResult(_responses[Math.Min(_n++, _responses.Count - 1)]);
    }

    private static LlmResponse Calls(params (string Id, string Name, string Args)[] calls)
        => new(new ChatMessage("assistant", "reasoning", null,
            calls.Select(c => new ToolCall(c.Id, c.Name, JsonDocument.Parse(c.Args).RootElement)).ToList()), true, null);

    private static readonly LlmResponse Done = new(new ChatMessage("assistant", "done"), true, null);

    private const string Job = "{\"yamlContent\":\"main:\\n  input: a.csv\"}";
    private static readonly ToolResult SameError = ToolResult.Error("{\"error\":\"line 18, column 41\"}");

    private static AgentExecutor Build(IAgentToolProvider tools, ILlmClient llm)
    {
        var console = AnsiConsole.Create(new AnsiConsoleSettings());
        return new AgentExecutor(tools, llm, new AgentTui(console), console);
    }

    private static async Task<(ScriptedToolProvider Tools, AgentExecutor Executor)> RunTwice(
        bool sequential, params ToolResult[] answers)
    {
        var tools = new ScriptedToolProvider(SameError, answers);
        var executor = Build(tools, new QueuedLlmClient(
            Calls(("1", "validate-yaml-job", Job)),
            Calls(("2", "validate-yaml-job", Job)),
            Done));

        await executor.RunTurnAsync("mission", "model", "http://localhost:11434",
            new AgentOptions { Sequential = sequential }, maxIterations: 6);

        return (tools, executor);
    }

    private static List<string> ToolReplies(AgentExecutor e)
        => e.Messages.Where(m => m.Role == "tool").Select(m => m.Content ?? string.Empty).ToList();

    /// <summary>
    /// The call is dispatched every time. Answering the second one from a cache would have dtpipe
    /// claim nothing can have changed about a source another process is free to write to.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task A_Repeated_Call_Is_Still_Dispatched(bool sequential)
    {
        var (tools, _) = await RunTwice(sequential);

        Assert.Equal(2, tools.Invoked.Count);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task An_Answer_That_Came_Back_The_Same_Is_Named_As_A_Repeat(bool sequential)
    {
        var (_, executor) = await RunTwice(sequential);

        var replies = ToolReplies(executor);
        Assert.Equal(2, replies.Count);
        Assert.DoesNotContain(Marker, replies[0]);
        Assert.Contains(Marker, replies[1]);
        Assert.Contains("step 1", replies[1]);
        // The answer travels whole: the advice wraps it, never replaces it.
        Assert.Contains("line 18, column 41", replies[1]);
    }

    /// <summary>
    /// The repeat is the pair, not the question. A source that moved between two identical calls
    /// answers differently, and that is not a repeat — it is the new answer the model asked for.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task An_Answer_That_Changed_Is_Not_A_Repeat(bool sequential)
    {
        var (_, executor) = await RunTwice(sequential,
            ToolResult.Error("{\"error\":\"line 18, column 41\"}"),
            ToolResult.Success("{\"success\":true}"));

        Assert.All(ToolReplies(executor), r => Assert.DoesNotContain(Marker, r));
    }

    /// <summary>A repeat of a failing call still reads as a failure, so there is still something
    /// to correct.</summary>
    [Fact]
    public async Task The_Advice_Keeps_The_Error_Flag_Of_The_Answer_It_Wraps()
    {
        var (_, executor) = await RunTwice(sequential: true);

        var steps = executor.Trajectory.Steps.Where(s => s.ToolName != null).ToList();
        Assert.Equal(2, steps.Count);
        Assert.Contains(Marker, steps[1].ToolResult);
        Assert.All(steps, s => Assert.True(s.IsError));
    }

    /// <summary>F5 is about one message: every call in it is dispatched.</summary>
    [Fact]
    public async Task Two_Identical_Calls_In_One_Message_Both_Run()
    {
        var tools = new ScriptedToolProvider(ToolResult.Success("{\"ok\":1}"));
        var executor = Build(tools, new QueuedLlmClient(
            Calls(("1", "inspect", "{\"input\":\"a.csv\"}"), ("2", "inspect", "{\"input\":\"a.csv\"}")),
            Done));

        await executor.RunTurnAsync("mission", "model", "http://localhost:11434", maxIterations: 5);

        Assert.Equal(2, tools.Invoked.Count);
    }

    /// <summary>Different arguments are a different question, whatever they answer.</summary>
    [Fact]
    public async Task A_Call_With_Changed_Arguments_Is_Not_A_Repeat()
    {
        var tools = new ScriptedToolProvider(ToolResult.Success("{\"ok\":1}"));
        var executor = Build(tools, new QueuedLlmClient(
            Calls(("1", "inspect", "{\"input\":\"a.csv\"}")),
            Calls(("2", "inspect", "{\"input\":\"b.csv\"}")),
            Done));

        await executor.RunTurnAsync("mission", "model", "http://localhost:11434", maxIterations: 5);

        Assert.Equal(2, tools.Invoked.Count);
        Assert.All(ToolReplies(executor), r => Assert.DoesNotContain(Marker, r));
    }
}
