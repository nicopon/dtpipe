using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using System.ClientModel;
using OpenAI;
using OpenAI.Chat;

namespace DtPipe.Cli.Agent;

// The OpenAI .NET SDK flags ChatCompletionOptions.Seed with OPENAI001 ("for evaluation
// purposes only"). We use it deliberately to make agent runs deterministic and replicable
// (F3), so the diagnostic is suppressed for this file.
#pragma warning disable OPENAI001

public class OpenAiClient : ILlmClient, IStreamingLlmClient
{
    private readonly string _apiKey;
    private readonly TimeSpan _chatTimeout;

    public string ProviderName => "openai";

    public OpenAiClient(string? apiKey = null, TimeSpan? chatTimeout = null)
    {
        _apiKey = apiKey ?? Environment.GetEnvironmentVariable("DTPIPE_LLM_API_KEY") ?? "";
        _chatTimeout = chatTimeout ?? AgentOptions.DefaultLlmTimeout;
    }

    public async Task<List<string>> ListModelsAsync(string baseUrl, CancellationToken ct = default)
    {
        try
        {
            using var client = new HttpClient();
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            cts.CancelAfter(TimeSpan.FromSeconds(10));

            var url = baseUrl.TrimEnd('/') + "/v1/models";
            if (!string.IsNullOrEmpty(_apiKey))
            {
                client.DefaultRequestHeaders.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", _apiKey);
            }

            var response = await client.GetAsync(url, cts.Token);
            if (!response.IsSuccessStatusCode)
                return new List<string>();

            var json = await response.Content.ReadAsStringAsync(ct);
            using var doc = JsonDocument.Parse(json);
            var list = new List<string>();

            if (doc.RootElement.TryGetProperty("data", out var dataArray) && dataArray.ValueKind == JsonValueKind.Array)
            {
                foreach (var el in dataArray.EnumerateArray())
                {
                    if (el.TryGetProperty("id", out var idEl) && idEl.ValueKind == JsonValueKind.String)
                    {
                        list.Add(idEl.GetString()!);
                    }
                }
            }

            return list;
        }
        catch
        {
            return new List<string>();
        }
    }

    /// <summary>
    /// Pipeline options for one endpoint, separated from the client so the deadline is assertable.
    ///
    /// <para>
    /// The SDK pipeline carries its own <c>NetworkTimeout</c> (100 s) and whichever deadline
    /// expires first wins, so leaving it unset makes <c>--llm-timeout</c> inert above 100 s while
    /// the failure still reports the flag's value — the defect HttpClient's default caused on the
    /// Ollama client.
    /// </para>
    /// </summary>
    internal static OpenAIClientOptions BuildClientOptions(string baseUrl)
    {
        var options = new OpenAIClientOptions { NetworkTimeout = System.Threading.Timeout.InfiniteTimeSpan };
        if (!string.IsNullOrEmpty(baseUrl))
            options.Endpoint = new Uri(baseUrl.TrimEnd('/') + "/v1");
        return options;
    }

    private ChatClient BuildClient(string baseUrl, string model)
    {
        return new ChatClient(model, new ApiKeyCredential(_apiKey), BuildClientOptions(baseUrl));
    }

    private static List<OpenAI.Chat.ChatMessage> MapMessages(List<ChatMessage> messages)
    {
        var sdkMessages = new List<OpenAI.Chat.ChatMessage>();
        foreach (var msg in messages)
        {
            if (msg.Role.Equals("system", StringComparison.OrdinalIgnoreCase))
                sdkMessages.Add(new SystemChatMessage(msg.Content));
            else if (msg.Role.Equals("user", StringComparison.OrdinalIgnoreCase))
                sdkMessages.Add(new UserChatMessage(msg.Content));
            else if (msg.Role.Equals("assistant", StringComparison.OrdinalIgnoreCase))
            {
                if (msg.ToolCalls != null && msg.ToolCalls.Count > 0)
                {
                    var assistantToolCalls = msg.ToolCalls.Select(tc => ChatToolCall.CreateFunctionToolCall(tc.Id, tc.Name, BinaryData.FromString(tc.Arguments.GetRawText()))).ToList();
                    var assistantMsg = new AssistantChatMessage(assistantToolCalls);
                    if (!string.IsNullOrEmpty(msg.Content))
                        assistantMsg.Content.Add(ChatMessageContentPart.CreateTextPart(msg.Content));
                    sdkMessages.Add(assistantMsg);
                }
                else
                {
                    sdkMessages.Add(new AssistantChatMessage(msg.Content));
                }
            }
            else if (msg.Role.Equals("tool", StringComparison.OrdinalIgnoreCase))
                sdkMessages.Add(new ToolChatMessage(msg.ToolCallId, msg.Content));
        }
        return sdkMessages;
    }

    private static ChatCompletionOptions BuildOptions(List<ToolDefinition> tools, double temperature, int? seed)
    {
        var options = new ChatCompletionOptions { Temperature = (float)temperature };
        if (seed.HasValue) options.Seed = seed.Value;
        foreach (var t in tools)
            options.Tools.Add(ChatTool.CreateFunctionTool(t.Name, t.Description, BinaryData.FromString(t.ParametersSchema.GetRawText())));
        return options;
    }

    private static JsonElement ParseArgs(string raw)
    {
        try
        {
            using var parsedDoc = JsonDocument.Parse(string.IsNullOrEmpty(raw) ? "{}" : raw);
            return parsedDoc.RootElement.Clone();
        }
        catch
        {
            return default;
        }
    }

    public Task<LlmResponse> ChatAsync(
        string baseUrl,
        string model,
        List<ChatMessage> messages,
        List<ToolDefinition> tools,
        int maxTokens = 16384,
        double temperature = 0.7,
        int? seed = null,
        CancellationToken ct = default)
        // One transport, one meaning for --llm-timeout, as on the Ollama client: the blocking call
        // reads the same streamed completion and simply has nobody watching it, so the flag is an
        // IDLE ceiling on both paths. It was a total call deadline here — a model whose answer took
        // longer than the flag was cut off and told it had been silent.
        => ChatStreamAsync(baseUrl, model, messages, tools, NullLlmStreamObserver.Instance,
                           maxTokens, temperature, seed, ct);

    public async Task<LlmResponse> ChatStreamAsync(
        string baseUrl,
        string model,
        List<ChatMessage> messages,
        List<ToolDefinition> tools,
        ILlmStreamObserver observer,
        int numCtx = 16384,
        double temperature = 0.7,
        int? seed = null,
        CancellationToken ct = default)
    {
        // When streaming, _chatTimeout is an IDLE ceiling — reset on every update received.
        using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        timeoutCts.CancelAfter(_chatTimeout);

        var acc = new AccumulatingLlmStreamObserver(observer);
        var splitter = new ThinkTagSplitter();
        var toolAcc = new SortedDictionary<int, (string id, string name, StringBuilder args)>();
        LlmUsage? usage = null;

        try
        {
            var chatClient = BuildClient(baseUrl, model);
            var sdkMessages = MapMessages(messages);
            var options = BuildOptions(tools, temperature, seed);

            await foreach (var update in chatClient.CompleteChatStreamingAsync(sdkMessages, options, timeoutCts.Token))
            {
                timeoutCts.CancelAfter(_chatTimeout);

                foreach (var part in update.ContentUpdate)
                {
                    if (!string.IsNullOrEmpty(part.Text)) splitter.Push(part.Text, acc);
                }

                foreach (var tc in update.ToolCallUpdates)
                {
                    if (!toolAcc.TryGetValue(tc.Index, out var entry))
                    {
                        entry = (tc.ToolCallId ?? $"call_{Guid.NewGuid():N}", tc.FunctionName ?? "", new StringBuilder());
                        toolAcc[tc.Index] = entry;
                        if (!string.IsNullOrEmpty(tc.FunctionName)) acc.OnToolCall(tc.FunctionName);
                    }
                    else if (string.IsNullOrEmpty(entry.name) && !string.IsNullOrEmpty(tc.FunctionName))
                    {
                        entry.name = tc.FunctionName;
                        toolAcc[tc.Index] = entry;
                        acc.OnToolCall(tc.FunctionName);
                    }

                    if (tc.FunctionArgumentsUpdate is { } argsChunk)
                        entry.args.Append(argsChunk.ToString());
                }

                if (update.Usage is { } u)
                    usage = new LlmUsage(u.InputTokenCount, u.OutputTokenCount);

                // The text it looped on is the whole evidence: a trace that records only the
                // verdict says a run died of repetition without saying on what.
                if (acc.RepetitionDetected)
                    return new LlmResponse(new ChatMessage("assistant", acc.ContentOrNull), true,
                        RepetitionGuard.DetectedMessage, null, acc.ThinkingOrNull);
            }

            splitter.Flush(acc);

            List<ToolCall>? toolCalls = toolAcc.Count > 0
                ? toolAcc.Values.Select(e => new ToolCall(e.id, e.name, ParseArgs(e.args.ToString()))).ToList()
                : null;

            return new LlmResponse(new ChatMessage("assistant", acc.ContentOrNull, null, toolCalls), true, null, usage, acc.ThinkingOrNull);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch (OperationCanceledException)
        {
            return new LlmResponse(new ChatMessage("assistant", null), true,
                $"The endpoint at {baseUrl} sent no output for {_chatTimeout.TotalSeconds:F0}s.");
        }
        catch (Exception ex)
        {
            return new LlmResponse(new ChatMessage("assistant", null), true, ex.Message);
        }
    }
}
