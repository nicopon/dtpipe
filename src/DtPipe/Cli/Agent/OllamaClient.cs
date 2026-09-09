using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Tasks;

namespace DtPipe.Cli.Agent;

public class OllamaClient : ILlmClient, IStreamingLlmClient
{
    private readonly HttpClient _http;
    private readonly TimeSpan _chatTimeout;

    public OllamaClient(TimeSpan? chatTimeout = null)
    {
        _chatTimeout = chatTimeout ?? AgentOptions.DefaultLlmTimeout;
        _http = Unbounded(new HttpClient());
    }

    /// <summary>Test seam: inject a handler to exercise the timeout / connection-failure paths.</summary>
    internal OllamaClient(HttpMessageHandler handler, TimeSpan chatTimeout)
    {
        _chatTimeout = chatTimeout;
        _http = Unbounded(new HttpClient(handler));
    }

    /// <summary>
    /// Hands the deadline to <see cref="_chatTimeout"/> alone.
    ///
    /// <para>
    /// HttpClient's own default is 100 s and whichever expires first wins, so without this line
    /// <c>--llm-timeout</c> silently stops mattering above 100 s while the failure still reports
    /// the flag's value. A measurement run recorded turns dying after 122 s and being told they
    /// had waited 420.
    /// </para>
    /// </summary>
    private static HttpClient Unbounded(HttpClient http)
    {
        http.Timeout = System.Threading.Timeout.InfiniteTimeSpan;
        return http;
    }

    public string ProviderName => "ollama";

    public record OllamaModelInfo(string Name, long Size, DateTime ModifiedAt);

    public async Task<List<string>> ListModelsAsync(string baseUrl, CancellationToken ct = default)
    {
        var models = await GetAvailableModelsAsync(baseUrl, ct);
        return models.Select(m => m.Name).ToList();
    }

    public async Task<List<OllamaModelInfo>> GetAvailableModelsAsync(string baseUrl, CancellationToken ct = default)
    {
        try
        {
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            cts.CancelAfter(TimeSpan.FromSeconds(10));

            var url = baseUrl.TrimEnd('/') + "/api/tags";
            var response = await _http.GetAsync(url, cts.Token);
            if (!response.IsSuccessStatusCode)
                return new List<OllamaModelInfo>();

            var json = await response.Content.ReadAsStringAsync(ct);
            using var doc = JsonDocument.Parse(json);
            var models = new List<OllamaModelInfo>();

            if (doc.RootElement.TryGetProperty("models", out var modelsArray) && modelsArray.ValueKind == JsonValueKind.Array)
            {
                foreach (var el in modelsArray.EnumerateArray())
                {
                    var name = el.GetProperty("name").GetString() ?? "";
                    var size = el.TryGetProperty("size", out var s) ? s.GetInt64() : 0;
                    var modifiedAt = el.TryGetProperty("modified_at", out var m) && DateTime.TryParse(m.GetString(), out var dt) ? dt : DateTime.MinValue;
                    if (!string.IsNullOrEmpty(name))
                    {
                        models.Add(new OllamaModelInfo(name, size, modifiedAt));
                    }
                }
            }

            return models;
        }
        catch
        {
            return new List<OllamaModelInfo>();
        }
    }

    private record ToolFunction(string Name, string Description, JsonElement Parameters);
    private record OllamaToolDefinition(string Type, ToolFunction Function);

    private record OllamaChatMessage(
        [property: JsonPropertyName("role")] string Role,
        [property: JsonPropertyName("content")] string? Content,
        [property: JsonPropertyName("name")] string? Name = null,
        [property: JsonPropertyName("tool_call_id")] string? ToolCallId = null,
        [property: JsonPropertyName("tool_calls")] List<OllamaToolCall>? ToolCalls = null
    );

    private record OllamaToolCall(
        [property: JsonPropertyName("id")] string? Id,
        [property: JsonPropertyName("function")] OllamaFunctionCall Function
    );

    private record OllamaFunctionCall(
        [property: JsonPropertyName("name")] string Name,
        [property: JsonPropertyName("arguments")] JsonElement Arguments
    );

    private static readonly JsonSerializerOptions JsonOpts = new()
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase
    };

    private static List<OllamaChatMessage> MapMessages(List<ChatMessage> messages) => messages.Select(m => new OllamaChatMessage(
        m.Role,
        m.Content,
        m.Name,
        m.ToolCallId,
        m.ToolCalls?.Select(tc => new OllamaToolCall(tc.Id, new OllamaFunctionCall(tc.Name, tc.Arguments))).ToList()
    )).ToList();

    private static List<OllamaToolDefinition> MapTools(List<ToolDefinition> tools) => tools.Select(t => new OllamaToolDefinition(
        "function",
        new ToolFunction(t.Name, t.Description, t.ParametersSchema)
    )).ToList();

    private static string BuildRequestJson(string model, List<OllamaChatMessage> messages,
        List<OllamaToolDefinition> tools, int numCtx, double temperature, int? seed, bool stream)
    {
        // temperature is always sent so a run can be made fully deterministic (temperature = 0).
        // seed is only sent when explicitly provided (null => omit, provider picks its own).
        var options = new Dictionary<string, object>
        {
            ["num_ctx"] = numCtx,
            ["temperature"] = temperature
        };
        if (seed.HasValue) options["seed"] = seed.Value;

        return JsonSerializer.Serialize(new { model, messages, tools, options, stream }, JsonOpts);
    }

    public Task<LlmResponse> ChatAsync(
        string baseUrl,
        string model,
        List<ChatMessage> messages,
        List<ToolDefinition> tools,
        int numCtx = 16384,
        double temperature = 0.7,
        int? seed = null,
        CancellationToken ct = default)
        // One transport, one meaning for --llm-timeout. The blocking call reads the same streamed
        // response and simply has nobody watching it: the flag is an IDLE ceiling on both paths,
        // reset by every line the model sends, rather than a total call deadline here and an idle
        // one there. It was the second: a 12B model generating a long YAML was cut off mid-answer
        // and told it had been silent, which is a claim about the model that was not true.
        => ChatStreamAsync(baseUrl, model, messages, tools, NullLlmStreamObserver.Instance,
                           numCtx, temperature, seed, ct);

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
        var url = baseUrl.TrimEnd('/') + "/api/chat";
        var requestJson = BuildRequestJson(model, MapMessages(messages), MapTools(tools), numCtx, temperature, seed, stream: true);

        // When streaming, _chatTimeout is an IDLE ceiling: it is reset on every line received, so a
        // model that keeps producing tokens is never cut off, and only a genuine stall trips it.
        using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        timeoutCts.CancelAfter(_chatTimeout);

        var acc = new AccumulatingLlmStreamObserver(observer);
        var splitter = new ThinkTagSplitter();
        List<OllamaToolCall>? toolCalls = null;
        LlmUsage? usage = null;
        string role = "assistant";

        try
        {
            using var req = new HttpRequestMessage(HttpMethod.Post, url)
            {
                Content = new StringContent(requestJson, Encoding.UTF8, "application/json")
            };
            using var response = await _http.SendAsync(req, HttpCompletionOption.ResponseHeadersRead, timeoutCts.Token);

            if (!response.IsSuccessStatusCode)
            {
                var body = await response.Content.ReadAsStringAsync(timeoutCts.Token);
                return new LlmResponse(new ChatMessage("assistant", null), true, ExtractError(body, (int)response.StatusCode));
            }

            using var stream = await response.Content.ReadAsStreamAsync(timeoutCts.Token);
            using var reader = new StreamReader(stream, Encoding.UTF8);

            while (await reader.ReadLineAsync(timeoutCts.Token) is { } line)
            {
                timeoutCts.CancelAfter(_chatTimeout);   // a line arrived — a live stream is not a stall
                if (line.Length == 0) continue;

                using var doc = JsonDocument.Parse(line);
                var root = doc.RootElement;

                if (root.TryGetProperty("error", out var errEl) && errEl.ValueKind == JsonValueKind.String)
                    return new LlmResponse(new ChatMessage("assistant", null), true, errEl.GetString());

                if (root.TryGetProperty("message", out var msgEl) && msgEl.ValueKind == JsonValueKind.Object)
                {
                    if (msgEl.TryGetProperty("role", out var rEl) && rEl.ValueKind == JsonValueKind.String)
                        role = rEl.GetString() ?? role;

                    if (msgEl.TryGetProperty("thinking", out var thEl) && thEl.ValueKind == JsonValueKind.String)
                    {
                        var t = thEl.GetString();
                        if (!string.IsNullOrEmpty(t)) acc.OnThinking(t);
                    }

                    if (msgEl.TryGetProperty("content", out var cEl) && cEl.ValueKind == JsonValueKind.String)
                    {
                        var c = cEl.GetString();
                        if (!string.IsNullOrEmpty(c)) splitter.Push(c, acc);
                    }

                    var chunkCalls = ParseToolCalls(msgEl);
                    if (chunkCalls is { Count: > 0 } && (toolCalls == null || toolCalls.Count == 0))
                    {
                        toolCalls = chunkCalls;
                        foreach (var tc in toolCalls) acc.OnToolCall(tc.Function.Name);
                    }
                }

                if (root.TryGetProperty("done", out var dEl) && dEl.ValueKind == JsonValueKind.True)
                    usage = ParseUsage(root);

                // The text it looped on is the whole evidence: a trace that records only the
                // verdict says a run died of repetition without saying on what.
                if (acc.RepetitionDetected)
                    return new LlmResponse(new ChatMessage("assistant", acc.ContentOrNull), true,
                        RepetitionGuard.DetectedMessage, null, acc.ThinkingOrNull);
            }

            splitter.Flush(acc);

            var assistant = new ChatMessage(role, acc.ContentOrNull, null,
                toolCalls?.Select(tc => new ToolCall(tc.Id ?? $"call_{Guid.NewGuid():N}", tc.Function.Name, tc.Function.Arguments)).ToList());

            return new LlmResponse(assistant, true, null, usage, acc.ThinkingOrNull);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch (OperationCanceledException)
        {
            return new LlmResponse(new ChatMessage("assistant", null), true,
                $"Ollama at {baseUrl} sent no output for {_chatTimeout.TotalSeconds:F0}s. " +
                "The model may be stuck — retry, pick a smaller model, or raise --llm-timeout.");
        }
        catch (Exception ex)
        {
            return new LlmResponse(new ChatMessage("assistant", null), true,
                $"Ollama request to {baseUrl} failed: {ex.Message}");
        }
    }

    private static string ExtractError(string body, int statusCode)
    {
        try
        {
            using var doc = JsonDocument.Parse(body);
            if (doc.RootElement.TryGetProperty("error", out var e) && e.ValueKind == JsonValueKind.String)
                return e.GetString() ?? $"HTTP {statusCode}";
        }
        catch { }
        return $"HTTP {statusCode}: {body}";
    }

    /// <summary>Routes any <c>&lt;think&gt;…&lt;/think&gt;</c> found in <paramref name="content"/> into
    /// the thinking channel, and merges it with a dedicated <paramref name="thinkingField"/> if the
    /// provider supplied one.</summary>
    internal static (string content, string? thinking) SplitThinking(string? content, string? thinkingField)
    {
        var acc = new AccumulatingLlmStreamObserver(NullLlmStreamObserver.Instance);
        if (!string.IsNullOrEmpty(thinkingField)) acc.OnThinking(thinkingField);
        if (!string.IsNullOrEmpty(content))
        {
            var splitter = new ThinkTagSplitter();
            splitter.Push(content, acc);
            splitter.Flush(acc);
        }
        return (acc.Content.ToString(), acc.ThinkingOrNull);
    }

    private static List<OllamaToolCall>? ParseToolCalls(JsonElement messageEl)
    {
        if (!messageEl.TryGetProperty("tool_calls", out var tcArray) || tcArray.ValueKind != JsonValueKind.Array)
            return null;

        var toolCalls = new List<OllamaToolCall>();
        foreach (var tc in tcArray.EnumerateArray())
        {
            var id = tc.TryGetProperty("id", out var idEl) ? idEl.GetString() : null;
            var fnEl = tc.GetProperty("function");
            var fnName = fnEl.GetProperty("name").GetString() ?? "";
            var fnArgs = fnEl.GetProperty("arguments");

            // If fnArgs is a string representation of JSON, re-parse it as JsonElement.
            if (fnArgs.ValueKind == JsonValueKind.String)
            {
                try
                {
                    using var parsedArgsDoc = JsonDocument.Parse(fnArgs.GetString()!);
                    fnArgs = parsedArgsDoc.RootElement.Clone();
                }
                catch { }
            }

            toolCalls.Add(new OllamaToolCall(id, new OllamaFunctionCall(fnName, fnArgs.Clone())));
        }
        return toolCalls;
    }

    private static LlmUsage? ParseUsage(JsonElement root)
    {
        int Count(string k) => root.TryGetProperty(k, out var e) && e.ValueKind == JsonValueKind.Number ? e.GetInt32() : 0;
        TimeSpan? Nanos(string k) => root.TryGetProperty(k, out var e) && e.ValueKind == JsonValueKind.Number
            ? TimeSpan.FromTicks(e.GetInt64() / 100)   // Ollama reports nanoseconds; a tick is 100 ns
            : null;

        int prompt = Count("prompt_eval_count");
        int completion = Count("eval_count");
        if (prompt == 0 && completion == 0) return null;
        return new LlmUsage(prompt, completion, Nanos("prompt_eval_duration"), Nanos("eval_duration"));
    }
}
