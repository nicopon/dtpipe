using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using DtPipe.Cli.Agent;
using Xunit;

namespace DtPipe.Tests.Unit.Cli;

/// <summary>
/// A slow or dead LLM endpoint must surface as a stated error, never as a
/// bare cancellation that the CLI mistakes for a user Ctrl-C and turns into a silent exit 130.
/// Only a genuine caller cancellation is allowed to propagate.
/// </summary>
public class OllamaClientResilienceTests
{
    private sealed class StubHandler : HttpMessageHandler
    {
        private readonly Func<HttpRequestMessage, CancellationToken, Task<HttpResponseMessage>> _handler;
        public StubHandler(Func<HttpRequestMessage, CancellationToken, Task<HttpResponseMessage>> handler) => _handler = handler;
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
            => _handler(request, cancellationToken);
    }

    private static readonly List<ChatMessage> Msgs = new() { new ChatMessage("user", "hi") };
    private static readonly List<ToolDefinition> NoTools = new();

    [Fact]
    public async Task Endpoint_Timeout_Returns_An_Error_Response_Not_An_Exception()
    {
        var handler = new StubHandler(async (_, ct) =>
        {
            await Task.Delay(TimeSpan.FromSeconds(30), ct); // outlives the client's timeout
            return new HttpResponseMessage(HttpStatusCode.OK);
        });
        var client = new OllamaClient(handler, TimeSpan.FromMilliseconds(100));

        var resp = await client.ChatAsync("http://localhost:11434", "m", Msgs, NoTools);

        Assert.True(resp.Done);
        Assert.NotNull(resp.Error);
        Assert.Contains("did not respond within", resp.Error);
    }

    /// <summary>
    /// The chat timeout is the only deadline. HttpClient's own default is 100 s and whichever
    /// expires first wins, so '--llm-timeout' did nothing above 100 s while the failure still
    /// reported the flag's value: a measurement run recorded turns dying after 122 s and being
    /// told they had waited 420. Asserting the field is the only check that does not take 100 s
    /// to run.
    /// </summary>
    [Fact]
    public void The_Client_Imposes_No_Deadline_Of_Its_Own()
    {
        var client = new DtPipe.Cli.Agent.OllamaClient(TimeSpan.FromSeconds(420));

        var http = (System.Net.Http.HttpClient)typeof(DtPipe.Cli.Agent.OllamaClient)
            .GetField("_http", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!
            .GetValue(client)!;

        Assert.Equal(System.Threading.Timeout.InfiniteTimeSpan, http.Timeout);
    }

    [Fact]
    public async Task Genuine_Caller_Cancellation_Propagates()
    {
        var handler = new StubHandler(async (_, ct) =>
        {
            await Task.Delay(TimeSpan.FromSeconds(30), ct);
            return new HttpResponseMessage(HttpStatusCode.OK);
        });
        var client = new OllamaClient(handler, TimeSpan.FromSeconds(30));
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => client.ChatAsync("http://localhost:11434", "m", Msgs, NoTools, ct: cts.Token));
    }

    [Fact]
    public async Task Connection_Failure_Returns_An_Error_Response_Naming_The_Endpoint()
    {
        var handler = new StubHandler((_, _) => throw new HttpRequestException("Connection refused"));
        var client = new OllamaClient(handler, TimeSpan.FromSeconds(5));

        var resp = await client.ChatAsync("http://localhost:9", "m", Msgs, NoTools);

        Assert.True(resp.Done);
        Assert.NotNull(resp.Error);
        Assert.Contains("http://localhost:9", resp.Error);
        Assert.Contains("failed", resp.Error);
    }

    [Fact]
    public async Task Successful_Response_Is_Parsed()
    {
        var handler = new StubHandler((_, _) => Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK)
        {
            Content = new StringContent(
                "{\"model\":\"m\",\"message\":{\"role\":\"assistant\",\"content\":\"hello\"},\"done\":true}",
                Encoding.UTF8, "application/json")
        }));
        var client = new OllamaClient(handler, TimeSpan.FromSeconds(5));

        var resp = await client.ChatAsync("http://localhost:11434", "m", Msgs, NoTools);

        Assert.Null(resp.Error);
        Assert.Equal("hello", resp.Message.Content);
    }
}
