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
/// F5/F6 tests: parallel tool execution and the single YAML path (yamlContent priority +
/// logged regex fallback).
/// </summary>
public class AgentParallelToolsTests
{
    /// <summary>
    /// A tool provider that records every invocation and can block all but a configurable number
    /// of concurrent calls, allowing the test to assert that independent calls ran in parallel.
    /// </summary>
    private sealed class RecordingToolProvider : IAgentToolProvider
    {
        public readonly List<string> InvokedNames = new();
        public int MaxConcurrentObserved;

        private readonly object _lock = new();
        private int _inflight;

        public int CallCount => InvokedNames.Count;

        public List<ToolDefinition> GetToolDefinitions() => new();
        public List<ToolDefinition> GetToolDefinitions(AgentMode mode) => new();

        public async Task<ToolResult> InvokeToolAsync(string toolName, JsonElement args, CancellationToken ct)
        {
            lock (_lock)
            {
                _inflight++;
                MaxConcurrentObserved = System.Math.Max(MaxConcurrentObserved, _inflight);
                InvokedNames.Add(toolName);
            }

            // Small yield so that, under parallel execution, multiple calls overlap; under
            // sequential execution only one call is ever in flight at a time.
            await Task.Delay(80, ct);

            lock (_lock)
            {
                _inflight--;
            }

            return ToolResult.Success("{}");
        }
    }

    /// <summary>
    /// An LLM client that returns a scripted sequence of responses (first N with tool calls, a
    /// final text-only response) so the executor loop terminates deterministically.
    /// </summary>
    private sealed class QueuedLlmClient : ILlmClient
    {
        private readonly IReadOnlyList<LlmResponse> _responses;
        public int CallCount { get; private set; }

        public QueuedLlmClient(IEnumerable<LlmResponse> responses)
        {
            _responses = responses.ToList();
        }

        public string ProviderName => "queued";
        public Task<List<string>> ListModelsAsync(string baseUrl, CancellationToken ct = default) => Task.FromResult(new List<string>());

        public Task<LlmResponse> ChatAsync(string baseUrl, string model, List<ChatMessage> messages, List<ToolDefinition> tools, int maxTokens = 16384, double temperature = 0.7, int? seed = null, CancellationToken ct = default)
        {
            int i = Math.Min(CallCount, _responses.Count - 1);
            CallCount++;
            return Task.FromResult(_responses[i]);
        }
    }

    private static AgentExecutor BuildExecutor(IAgentToolProvider tools, ILlmClient llm, out IAnsiConsole console)
    {
        console = Spectre.Console.AnsiConsole.Create(new Spectre.Console.AnsiConsoleSettings());
        var tui = new AgentTui(console);
        return new AgentExecutor(tools, llm, tui, console);
    }

    private static LlmResponse WithToolCalls(string content, params (string id, string name)[] calls)
    {
        var tcs = new List<ToolCall>();
        foreach (var c in calls)
        {
            tcs.Add(new ToolCall(c.id, c.name, JsonDocument.Parse("{}").RootElement));
        }

        return new LlmResponse(new ChatMessage("assistant", content, null, tcs), true, null);
    }

    [Fact]
    public async Task Three_Independent_Tool_Calls_Are_All_Executed()
    {
        var provider = new RecordingToolProvider();
        var llm = new QueuedLlmClient(new[]
          {
            WithToolCalls("reasoning", ("1", "inspect"), ("2", "inspect"), ("3", "preview-data")),
            new LlmResponse(new ChatMessage("assistant", "done"), true, null)
          });

        var executor = BuildExecutor(provider, llm, out _);
        await executor.RunTurnAsync("mission", "model", "http://localhost:11434", maxIterations: 5);

        Assert.Equal(3, provider.CallCount);
        // One "tool" message per call is appended to the conversation.
        Assert.Equal(3, executor.Messages.Count(m => m.Role == "tool"));
        // Per-call trajectory steps recorded.
        Assert.Equal(3, executor.Trajectory.Steps.Count(s => s.ToolName != null));
    }

    [Fact]
    public async Task Parallel_By_Default_Overlaps_Independent_Calls()
    {
        var provider = new RecordingToolProvider();
        var llm = new QueuedLlmClient(new[]
          {
            WithToolCalls("reasoning", ("1", "inspect"), ("2", "inspect"), ("3", "inspect")),
            new LlmResponse(new ChatMessage("assistant", "done"), true, null)
          });

        var executor = BuildExecutor(provider, llm, out _);
        // Default options => Sequential = false => parallel.
        await executor.RunTurnAsync("mission", "model", "http://localhost:11434", new AgentOptions { Sequential = false }, maxIterations: 5);

        Assert.Equal(3, provider.CallCount);
        // At least two calls overlapped in time, proving parallel execution.
        Assert.True(provider.MaxConcurrentObserved >= 2, $"expected overlap, observed {provider.MaxConcurrentObserved}");
    }

    [Fact]
    public async Task Sequential_Does_Not_Overlap_Calls()
    {
        var provider = new RecordingToolProvider();
        var llm = new QueuedLlmClient(new[]
          {
            WithToolCalls("reasoning", ("1", "inspect"), ("2", "inspect"), ("3", "inspect")),
            new LlmResponse(new ChatMessage("assistant", "done"), true, null)
          });

        var executor = BuildExecutor(provider, llm, out _);
        await executor.RunTurnAsync("mission", "model", "http://localhost:11434", new AgentOptions { Sequential = true }, maxIterations: 5);

        Assert.Equal(3, provider.CallCount);
        Assert.Equal(1, provider.MaxConcurrentObserved);
    }

    [Fact]
    public async Task Tool_Messages_Stay_Correlated_With_Their_Call_Ids()
    {
        var provider = new RecordingToolProvider();
        var llm = new QueuedLlmClient(new[]
          {
            WithToolCalls("reasoning", ("a-1", "inspect"), ("b-2", "preview-data")),
            new LlmResponse(new ChatMessage("assistant", "done"), true, null)
          });

        var executor = BuildExecutor(provider, llm, out _);
        await executor.RunTurnAsync("mission", "model", "http://localhost:11434", maxIterations: 5);

        var toolMessages = executor.Messages.Where(m => m.Role == "tool").ToList();
        Assert.Equal(2, toolMessages.Count);
        // Each tool message carries the id of the call it answers (stable regardless of order).
        var ids = toolMessages.Select(m => m.ToolCallId).OrderBy(id => id).ToList();
        Assert.Equal(new[] { "a-1", "b-2" }, ids);
    }
}
