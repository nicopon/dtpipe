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
/// F4 — non-destructive context: inspected schemas / sample rows / recent errors must survive
/// conversation compaction and remain available to the planner.
/// </summary>
public class AgentContextStoreTests
{
    private sealed class InspectToolProvider : IAgentToolProvider
    {
        /// <summary>Schema rows returned by the inspect tool.</summary>
        public string InspectResult { get; }
        public int CallCount { get; private set; }

        public InspectToolProvider(string inspectResult)
             => InspectResult = inspectResult;

        public List<ToolDefinition> GetToolDefinitions() => new();
        public List<ToolDefinition> GetToolDefinitions(AgentMode mode) => new();

        public Task<ToolResult> InvokeToolAsync(string toolName, JsonElement args, CancellationToken ct)
        {
            CallCount++;
            switch (toolName)
            {
                case "inspect":
                    return Task.FromResult(ToolResult.Success(InspectResult));
                case "validate-yaml-job":
                    return Task.FromResult(ToolResult.Success("ok"));
                default:
                    return Task.FromResult(ToolResult.Success("{}"));
            }
        }
    }

    private sealed class QueuedLlmClient : ILlmClient
    {
        private readonly IReadOnlyList<LlmResponse> _responses;
        private int _i;

        public QueuedLlmClient(IEnumerable<LlmResponse> responses)
             => _responses = responses.ToList();

        public string ProviderName => "queued";
        public Task<List<string>> ListModelsAsync(string baseUrl, CancellationToken ct = default) => Task.FromResult(new List<string>());

        public Task<LlmResponse> ChatAsync(string baseUrl, string model, List<ChatMessage> messages, List<ToolDefinition> tools, int maxTokens = 16384, double temperature = 0.7, int? seed = null, CancellationToken ct = default)
        {
            int idx = Math.Min(_i, _responses.Count - 1);
            _i++;
            return Task.FromResult(_responses[idx]);
        }
    }

    private static AgentExecutor BuildExecutor(IAgentToolProvider tools, ILlmClient llm, AgentContextStore? store = null)
    {
        IAnsiConsole console = Spectre.Console.AnsiConsole.Create(new Spectre.Console.AnsiConsoleSettings());
        var tui = new AgentTui(console);
        return new AgentExecutor(tools, llm, tui, console, store);
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
    public async Task Inspected_Schema_Survives_Compaction_And_Appears_In_Facts()
    {
        var schemas = "{\"tables\":[{\"name\":\"orders\",\"columns\":[\"id\",\"amount\",\"region\"]}]}";
        var store = new AgentContextStore();
        var provider = new InspectToolProvider(schemas);

        // Two turns: first inspects, second validates. The conversation grows so that compaction
        // would otherwise drop the inspected schema.
        var llm = new QueuedLlmClient(new[]
           {
            WithToolCall("discover the schema", "inspect", $"{{\"input\": \"csv:orders.csv\"}}"),
            WithToolCall("now plan the pipeline", "validate-yaml-job", "{}"),
           TextOnly("done")
           });

        var executor = BuildExecutor(provider, llm, store);
        await executor.RunTurnAsync("inspects then plans", "m", "http://localhost:11434", maxIterations: 5);

        // The fact must be cached in the store.
        var facts = store.GetFacts().ToList();
        Assert.NotEmpty(facts);
        Assert.Contains(facts, f => f.ToolName == "inspect");

        // And the FACTS block must surface the inspected schema.
        var factsBlock = store.BuildFactsBlock();
        Assert.Contains("orders", factsBlock);
    }

    [Fact]
    public async Task After_Compaction_The_Inspect_Schema_Remains_Available()
    {
        var schemas = "orders: id, amount, region";
        var store = new AgentContextStore();

        // Simulate compaction with the context store: the FACTS block is emitted in place of
        // the lossy one-line summary, and it must contain the schema.
        var manager = new ConversationWindowManager(maxMessages: 5, keepSystemMessages: 1, keepRecentMessages: 2);
        store.RecordFact("inspect @ csv:orders.csv", "inspect", schemas, isError: false);

        var messages = new List<ChatMessage>
           {
            new("system", "sys"),
            new("user", "u1"),
            new("assistant", "a1", null, new List<ToolCall> { new("1", "inspect", default) }),
             new("tool", schemas, "inspect", ToolCallId: "1"),
            new("user", "u2"),
            new("assistant", "a2")
           };

        var compacted = manager.Compact(messages, store);

        // The compacted window must carry the schema via a FACTS block (not the lossy summary).
        var assistantMsgs = compacted.Where(m => m.Role == "assistant").Select(m => m.Content ?? "").ToList();
        var joined = string.Join("\n", assistantMsgs);
        Assert.Contains("orders", joined);
        // The full journal stays in the trajectory (compaction does not mutate the message list).
        Assert.Equal(6, messages.Count);
    }

    [Fact]
    public async Task Inspect_Fact_Key_Opens_On_Repeat_Inspection()
    {
        var store = new AgentContextStore();

        // Two inspections of the same source share the same key => one fact.
        store.RecordFact("inspect @ csv:orders.csv", "inspect", "old", false);
        store.RecordFact("inspect @ csv:orders.csv", "inspect", "new", false);

        var facts = store.GetFacts().ToList();
        Assert.Single(facts);
        Assert.Equal("new", facts[0].Content);
    }

    [Fact]
    public async Task Recent_Errors_Are_Preserved_As_Facts()
    {
        var store = new AgentContextStore();
        store.RecordFact("preview-data @ pg:secret", "preview-data", "ERROR: relation does not exist", true);

        var block = store.BuildFactsBlock();
        Assert.Contains("ERROR", block);
    }
}
