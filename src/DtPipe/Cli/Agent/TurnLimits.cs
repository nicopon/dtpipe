using System;

namespace DtPipe.Cli.Agent;

/// <summary>
/// What bounds a single model call, and what it says when a bound is reached.
///
/// <para>
/// Nothing bounded one. <c>--llm-timeout</c> is an IDLE ceiling — reset by every line the model
/// sends — so a call that keeps producing is never cut, by design: a 12B writing a long YAML had
/// been cut mid-answer and told it had been silent, and the idle rule is the fix for that. But a
/// model that keeps producing forever is then unbounded too: two recorded sessions spent more than
/// twenty minutes inside one call, with no verdict and no way out.
/// </para>
///
/// <para>
/// Two bounds, in that order of preference. <see cref="DefaultMaxOutputTokens"/> is the one that
/// matters: it makes the runaway impossible instead of interrupting it late, and it is what the
/// shell harness in <c>tests/agentic</c> already sets for the same reason. The deadline is only a
/// backstop for what no token count covers — a transport that trickles — so it is deliberately
/// loose: cutting a legitimate answer is the mistake it must not make.
/// </para>
/// </summary>
internal static class TurnLimits
{
    /// <summary>
    /// Tokens a single call may generate. Far above any real answer — a large YAML job plus its
    /// reasoning runs to a few hundred — and far below a degenerate one, which is why a ceiling
    /// can be generous and still bite.
    /// </summary>
    public const int DefaultMaxOutputTokens = 4096;

    /// <summary>
    /// The total call deadline, as a multiple of the idle ceiling. Derived rather than given its
    /// own flag: two timeouts a user must tell apart is a worse surface than one, and the number
    /// that needs tuning is the ceiling above.
    /// </summary>
    public const int CallDeadlineFactor = 3;

    /// <summary>The total a single call may take, whatever it is doing.</summary>
    public static TimeSpan CallDeadline(TimeSpan idleCeiling) => idleCeiling * CallDeadlineFactor;

    /// <summary>
    /// Said when the model was still generating at the ceiling. It is not an endpoint fault and
    /// not a user interrupt, and reporting it as either sends the reader to the wrong knob.
    /// </summary>
    public const string OutputCeilingMessage =
        "The model was still generating when it reached the output ceiling and was stopped, so its "
        + "answer is cut short. Raise --max-output-tokens if the answer was genuinely long; a model "
        + "that reaches it without calling a tool is usually rambling, and a different model or a "
        + "narrower request is the shorter way out.";

    /// <summary>
    /// The opening of <see cref="CallDeadlineMessage"/>, which carries a duration and so cannot be
    /// matched whole. Kept beside it so the message and the test for it cannot drift apart.
    /// </summary>
    private const string CallDeadlinePrefix = "One model call ran for ";

    /// <summary>True when <paramref name="message"/> is the deadline's, whatever duration it names.</summary>
    public static bool IsCallDeadline(string? message) =>
        message is not null && message.StartsWith(CallDeadlinePrefix, StringComparison.Ordinal);

    /// <summary>Said when one call outlived the total deadline, streaming all the while.</summary>
    public static string CallDeadlineMessage(TimeSpan deadline) =>
        $"{CallDeadlinePrefix}{deadline.TotalSeconds:F0}s without finishing and was stopped. It was "
        + "producing output the whole time, so this is not a stalled endpoint — raise --llm-timeout "
        + "if the model is simply slow, or use a smaller request.";
}
