using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using DtPipe.Cli.Agent;
using Xunit;

namespace DtPipe.Tests.Unit.Cli;

/// <summary>
/// A single model call is bounded. Nothing bounded one before: --llm-timeout measures silence and
/// is reset by every line, so a model that keeps producing ran for as long as it liked — twenty
/// minutes, twice, with no verdict.
/// </summary>
public class TurnLimitsTests
{
    private sealed class StubHandler : HttpMessageHandler
    {
        private readonly Func<HttpResponseMessage> _make;
        public StubHandler(Func<HttpResponseMessage> make) => _make = make;
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
        {
            Request = request.Content!.ReadAsStringAsync(ct).GetAwaiter().GetResult();
            return Task.FromResult(_make());
        }
        public string? Request { get; private set; }
    }

    private static readonly List<ChatMessage> Msgs = new() { new ChatMessage("user", "hi") };
    private static readonly List<ToolDefinition> NoTools = new();

    private static HttpResponseMessage Ndjson(params string[] lines) => new(HttpStatusCode.OK)
    {
        Content = new StringContent(string.Join("\n", lines) + "\n", Encoding.UTF8, "application/x-ndjson")
    };

    [Fact]
    public async Task The_Output_Ceiling_Is_Sent_To_The_Provider()
    {
        var handler = new StubHandler(() => Ndjson("{\"message\":{\"role\":\"assistant\",\"content\":\"ok\"},\"done\":true}"));
        var client = new OllamaClient(handler, TimeSpan.FromSeconds(5), maxOutputTokens: 1234);

        await client.ChatAsync("http://x", "m", Msgs, NoTools);

        using var sent = JsonDocument.Parse(handler.Request!);
        Assert.Equal(1234, sent.RootElement.GetProperty("options").GetProperty("num_predict").GetInt32());
    }

    /// <summary>
    /// Truncated text with no tool call must not reach the loop as an answer: the turn would end on
    /// a plan the model never finished writing, and report it as delivered.
    /// </summary>
    [Fact]
    public async Task A_Truncated_Answer_Is_Reported_Rather_Than_Returned_As_One()
    {
        var handler = new StubHandler(() => Ndjson(
            "{\"message\":{\"role\":\"assistant\",\"content\":\"main:\\n  input: \"},\"done\":false}",
            "{\"message\":{\"role\":\"assistant\",\"content\":\"\"},\"done\":true,\"done_reason\":\"length\"}"));
        var client = new OllamaClient(handler, TimeSpan.FromSeconds(5));

        var resp = await client.ChatAsync("http://x", "m", Msgs, NoTools);

        Assert.Equal(TurnLimits.OutputCeilingMessage, resp.Error);
        Assert.Contains("main:", resp.Message.Content);
    }

    /// <summary>A parsed tool call is usable whatever followed it, so the ceiling does not void it.</summary>
    [Fact]
    public async Task A_Truncated_Response_Carrying_A_Tool_Call_Is_Kept()
    {
        var handler = new StubHandler(() => Ndjson(
            "{\"message\":{\"role\":\"assistant\",\"tool_calls\":[{\"function\":{\"name\":\"help\",\"arguments\":{}}}]},\"done\":false}",
            "{\"message\":{\"role\":\"assistant\",\"content\":\"\"},\"done\":true,\"done_reason\":\"length\"}"));
        var client = new OllamaClient(handler, TimeSpan.FromSeconds(5));

        var resp = await client.ChatAsync("http://x", "m", Msgs, NoTools);

        Assert.Null(resp.Error);
        Assert.Equal("help", Assert.Single(resp.Message.ToolCalls!).Name);
    }

    [Fact]
    public void The_Call_Deadline_Is_A_Multiple_Of_The_Idle_Ceiling()
    {
        var deadline = TurnLimits.CallDeadline(TimeSpan.FromSeconds(300));

        Assert.Equal(TimeSpan.FromSeconds(900), deadline);
        Assert.True(TurnLimits.IsCallDeadline(TurnLimits.CallDeadlineMessage(deadline)));
        Assert.False(TurnLimits.IsCallDeadline(TurnLimits.OutputCeilingMessage));
    }

    /// <summary>
    /// Every stated way a turn can end has a clause; a bare enum name in the summary is what the
    /// fallback produces, and it reads as a leak rather than a verdict.
    /// </summary>
    [Theory]
    [InlineData(TurnOutcome.OutputCeilingReached)]
    [InlineData(TurnOutcome.CallTookTooLong)]
    public void Each_New_Outcome_Reads_As_A_Sentence(TurnOutcome outcome)
    {
        var described = TurnSummaryModel.Describe(outcome);

        Assert.NotEqual(outcome.ToString(), described);
        Assert.Contains(' ', described);
    }
}
