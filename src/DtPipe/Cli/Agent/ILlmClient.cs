using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace DtPipe.Cli.Agent;

public interface ILlmClient
{
    /// <summary>Provider name (e.g., "ollama", "openai")</summary>
    string ProviderName { get; }

    /// <summary>Lists available models on this backend; an <see cref="HttpRequestException"/> says the server did not answer or refused the list, an empty list that it has no model.</summary>
    Task<List<string>> ListModelsAsync(string baseUrl, CancellationToken ct = default);

    /// <summary>Sends a chat completion request with support for tool calls.</summary>
    /// <remarks>
    /// <paramref name="temperature"/> and <paramref name="seed"/> are propagated to the underlying
    /// provider so that agent runs can be made deterministic (e.g. temperature 0 + a fixed seed)
    /// and replicated. Defaults preserve the previous non-deterministic behavior.
    /// </remarks>
    Task<LlmResponse> ChatAsync(
        string baseUrl,
        string model,
        List<ChatMessage> messages,
        List<ToolDefinition> tools,
        int maxTokens = 16384,
        double temperature = 0.7,
        int? seed = null,
        CancellationToken ct = default);
}
