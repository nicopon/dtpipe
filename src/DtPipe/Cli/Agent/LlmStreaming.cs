using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace DtPipe.Cli.Agent;

/// <summary>
/// Receives incremental output while a model generates. All methods are called on the thread that
/// drives the stream; an implementation must return quickly and must not throw.
/// </summary>
public interface ILlmStreamObserver
{
    /// <summary>A chunk of model reasoning — Ollama's <c>message.thinking</c> or the text inside a
    /// <c>&lt;think&gt;…&lt;/think&gt;</c> block. Called zero or many times.</summary>
    void OnThinking(string delta);

    /// <summary>A chunk of the model's answer text.</summary>
    void OnContent(string delta);

    /// <summary>The model has committed to a tool call (its name is known; arguments may still be
    /// streaming). Called once per tool call.</summary>
    void OnToolCall(string toolName);
}

/// <summary>
/// Implemented by an <see cref="ILlmClient"/> that can stream a response token by token. The agent
/// prefers this when rendering a TUI; a client that does not implement it falls back to the
/// blocking <see cref="ILlmClient.ChatAsync"/>.
/// </summary>
public interface IStreamingLlmClient
{
    /// <summary>
    /// Same contract as <see cref="ILlmClient.ChatAsync"/>, but <paramref name="observer"/> is
    /// notified as output arrives. The returned <see cref="LlmResponse"/> is the fully assembled
    /// result (content, thinking, tool calls, usage) — identical to what the blocking call returns.
    /// </summary>
    Task<LlmResponse> ChatStreamAsync(
        string baseUrl,
        string model,
        List<ChatMessage> messages,
        List<ToolDefinition> tools,
        ILlmStreamObserver observer,
        int numCtx = 16384,
        double temperature = 0.7,
        int? seed = null,
        CancellationToken ct = default);
}

/// <summary>A sink that ignores every event — used for replication runs and headless contexts.</summary>
public sealed class NullLlmStreamObserver : ILlmStreamObserver
{
    public static readonly NullLlmStreamObserver Instance = new();
    public void OnThinking(string delta) { }
    public void OnContent(string delta) { }
    public void OnToolCall(string toolName) { }
}

/// <summary>
/// Wraps another observer, forwarding every event while also accumulating the full thinking and
/// content text so the client can assemble the final <see cref="LlmResponse"/>, and watching the
/// combined stream for a model stuck repeating itself (<see cref="RepetitionGuard"/>) — the loop can
/// live in either channel, so both feed the same guard.
/// </summary>
internal sealed class AccumulatingLlmStreamObserver : ILlmStreamObserver
{
    private readonly ILlmStreamObserver _inner;
    private readonly RepetitionGuard _repetition = new();
    public readonly StringBuilder Thinking = new();
    public readonly StringBuilder Content = new();

    public AccumulatingLlmStreamObserver(ILlmStreamObserver inner) => _inner = inner;

    /// <summary>True once the stream has been seen repeating the same text — the reading loop
    /// should stop consuming further output and report this as a failed call.</summary>
    public bool RepetitionDetected { get; private set; }

    public void OnThinking(string delta)
    {
        Thinking.Append(delta);
        if (_repetition.Feed(delta)) RepetitionDetected = true;
        _inner.OnThinking(delta);
    }

    public void OnContent(string delta)
    {
        Content.Append(delta);
        if (_repetition.Feed(delta)) RepetitionDetected = true;
        _inner.OnContent(delta);
    }

    public void OnToolCall(string toolName) => _inner.OnToolCall(toolName);

    public string? ThinkingOrNull => Thinking.Length > 0 ? Thinking.ToString() : null;
    public string? ContentOrNull => Content.Length > 0 ? Content.ToString() : null;
}
