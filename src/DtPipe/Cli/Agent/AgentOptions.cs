namespace DtPipe.Cli.Agent;

/// <summary>
/// The settings of the model calls an agent turn makes. Nothing here touches a write gate: what the
/// model may do is decided by the tools it is offered.
/// </summary>
public sealed class AgentOptions
{
    /// <summary>Ceiling for a single LLM call. Past this the call is reported as a failed call,
    /// not left to stall until the HTTP stack's own default fires (which surfaces as a bare
    /// cancellation and is easily mistaken for a user Ctrl-C). Threaded to the LLM client at
    /// construction.</summary>
    public static readonly System.TimeSpan DefaultLlmTimeout = System.TimeSpan.FromSeconds(300);

    /// <summary>Default model context window requested from the provider (Ollama <c>num_ctx</c>).
    /// The agent prompt carries the tool catalogue plus a growing transcript, so the provider's
    /// own small default (4k) is not enough; going much higher mainly costs prefill time and
    /// VRAM.</summary>
    public const int DefaultNumCtx = 32768;

    /// <summary>Share of the window, as the provider reported its use, from which the older messages
    /// are folded into the facts block for the rest of the conversation. A tool answer of a few
    /// thousand tokens still fits above it.</summary>
    public const double CompactFrom = 0.70;

    /// <summary>Share of the window from which the person is told it is nearly full. The provider
    /// drops the start of a prompt that overflows without saying so.</summary>
    public const double WarnFrom = 0.90;

    /// <summary>
    /// Sampling temperature. 0 is greedy decoding, which is where a weak or heavily quantized
    /// model degenerates: a measurement run on a 12B local model returned empty responses at 0 and
    /// complete plans at 1.
    /// </summary>
    public double Temperature { get; init; } = 1.0;

    /// <summary>Model context window to request from the provider.</summary>
    public int NumCtx { get; init; } = DefaultNumCtx;

    /// <summary>Fixed seed for sampling. Null leaves the choice to the provider, so two sessions
    /// over the same mission are two draws, not one draw twice.</summary>
    public int? Seed { get; init; }
}
