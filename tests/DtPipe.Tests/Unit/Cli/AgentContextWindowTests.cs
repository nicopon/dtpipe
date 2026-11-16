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
/// The window's use, as the provider reports it, folds the conversation and tells the person: a prompt
/// past the window loses its start without a word, and the count of messages does not see it coming.
/// </summary>
public class AgentContextWindowTests
{
    private sealed class OneToolProvider : IAgentToolProvider
    {
        public List<ToolDefinition> GetToolDefinitions() => [new ToolDefinition("inspect", "", default)];
        public Task<ToolResult> InvokeToolAsync(string toolName, JsonElement args, CancellationToken ct)
            => Task.FromResult(ToolResult.Success("{}"));
    }

    /// <summary>Answers with a tool call that reports <paramref name="promptTokens"/>, <paramref name="calls"/> times, then ends.</summary>
    private sealed class UsageClient(string provider, int promptTokens, int calls) : ILlmClient
    {
        private int _n;
        public List<List<ChatMessage>> Sent { get; } = [];
        public string ProviderName => provider;
        public Task<List<string>> ListModelsAsync(string baseUrl, CancellationToken ct = default) => Task.FromResult(new List<string>());

        public Task<LlmResponse> ChatAsync(string baseUrl, string model, List<ChatMessage> messages, List<ToolDefinition> tools,
            int maxTokens = 16384, double temperature = 0.7, int? seed = null, CancellationToken ct = default)
        {
            Sent.Add(messages.ToList());
            int n = _n++;
            if (n >= calls) return Task.FromResult(new LlmResponse(new ChatMessage("assistant", "done"), true, null));
            var call = new ToolCall($"c{n}", "inspect", JsonDocument.Parse($"{{\"n\":{n}}}").RootElement);
            return Task.FromResult(new LlmResponse(new ChatMessage("assistant", "", ToolCalls: [call]), true, null,
                new LlmUsage(promptTokens, 100)));
        }
    }

    private static async Task<(List<string> Notices, UsageClient Client, AgentExecutor Executor)> Run(string provider, int promptTokens, int calls)
    {
        var client = new UsageClient(provider, promptTokens, calls);
        var executor = new AgentExecutor(new OneToolProvider(), client);
        var notices = new List<string>();
        executor.ContextNotice += notices.Add;
        await executor.TurnAsync("ask the question", options: new AgentOptions { NumCtx = 32768 });
        return (notices, client, executor);
    }

    [Fact]
    public async Task A_Window_Seventy_Percent_Used_Folds_The_Older_Messages_And_Says_So_Once()
    {
        var (notices, client, executor) = await Run("ollama", promptTokens: 24_000, calls: 8);

        Assert.Single(notices);
        Assert.Contains("24,100 of 32,768 tokens", notices[0].Replace(' ', ',').Replace(' ', ','));
        Assert.Contains("folded", notices[0]);
        var last = client.Sent[^1];
        Assert.True(last.Count < executor.Messages.Count);
        Assert.Contains(last, m => m.Role == "user" && m.Content!.Contains("ask the question"));
    }

    [Fact]
    public async Task A_Window_Nearly_Full_Is_Announced_Even_When_Nothing_Can_Be_Folded()
    {
        var (notices, _, _) = await Run("ollama", promptTokens: 30_000, calls: 1);

        Assert.Single(notices);
        Assert.Contains("nearly full", notices[0]);
    }

    [Fact]
    public async Task A_Provider_With_No_Window_Is_Never_Told_It_Is_Full()
    {
        var (notices, _, _) = await Run("openai", promptTokens: 30_000, calls: 8);

        Assert.Empty(notices);
    }
}
