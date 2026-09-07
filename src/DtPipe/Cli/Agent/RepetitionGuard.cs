using System;
using System.Text;

namespace DtPipe.Cli.Agent;

/// <summary>
/// Detects a model stuck regenerating the same text over and over — observed on a temperature-0
/// run where the thinking channel alternated between two near-identical sentences without end.
/// Ollama's own <c>repeat_penalty</c> looks back only <c>repeat_last_n</c> tokens (64 by default);
/// a multi-sentence cycle easily exceeds that window and the built-in penalty never sees it repeat.
///
/// This is a backstop, not a decoding fix: it does not try to make the model behave, it bounds how
/// long dtpipe waits before giving up on a call that will not converge on its own.
///
/// <para>
/// <b>A repeated chunk is not a loop; a repeated cycle is.</b> The content channel carries the
/// deliverable, and a YAML job is repetitive by construction: one branch per target table means the
/// same <c>provider-options / writer / table</c> header verbatim per branch. In a real three-branch
/// job a 96-character chunk already occurs three times, so no chunk length both survives a
/// multi-branch job and still catches a cycle of ~110 characters. What separates the two is
/// progress: between two occurrences of a looping phrase the text is the same, between two branch
/// headers it is not. So the trailing chunk must repeat AND the whole cycle behind it must repeat.
/// </para>
/// </summary>
internal sealed class RepetitionGuard
{
    /// <summary>Shared, user-facing explanation for both clients — a stuck repetition, not an
    /// endpoint problem, so it says what to try instead of pointing at the network.</summary>
    public const string DetectedMessage =
        "The model appears to be stuck repeating the same text and was stopped. This often happens " +
        "with --temperature 0 (fully deterministic decoding) on a weaker or heavily quantized model — " +
        "retry with a temperature above 0, or try a different model.";

    private readonly StringBuilder _buffer = new();
    private readonly int _windowChars;
    private readonly int _matchChars;
    private readonly int _minRepeats;

    /// <param name="windowChars">How much recent text to keep — must hold at least <paramref name="minRepeats"/> cycles of the longest loop worth catching.</param>
    /// <param name="matchChars">Length of the trailing chunk checked for repetition. Long enough that matching it more than by chance is essentially impossible in normal prose.</param>
    /// <param name="minRepeats">How many times that exact chunk must appear before this is called a loop, not a coincidence.</param>
    public RepetitionGuard(int windowChars = 3000, int matchChars = 50, int minRepeats = 3)
    {
        _windowChars = windowChars;
        _matchChars = matchChars;
        _minRepeats = minRepeats;
    }

    /// <summary>Feeds one delta of streamed text. Returns true the first time the trailing
    /// <see cref="_matchChars"/>-long chunk has occurred <see cref="_minRepeats"/> times (this one
    /// included) within the tracked window AND the text between two of those occurrences is itself
    /// repeated — the difference between a loop and a document with a recurring header.</summary>
    public bool Feed(string delta)
    {
        if (string.IsNullOrEmpty(delta)) return false;

        _buffer.Append(delta);
        if (_buffer.Length > _windowChars)
            _buffer.Remove(0, _buffer.Length - _windowChars);

        if (_buffer.Length < _matchChars) return false;

        string window = _buffer.ToString();
        string probe = window[^_matchChars..];

        int count = 0, idx = 0, previous = -1, last = -1;
        while (count < _minRepeats)
        {
            idx = window.IndexOf(probe, idx, StringComparison.Ordinal);
            if (idx < 0) break;
            (previous, last) = (last, idx);
            count++;
            idx++;
        }
        if (count < _minRepeats) return false;

        int period = last - previous;
        if (period <= 0 || window.Length < 2 * period) return false;

        return string.CompareOrdinal(window, window.Length - 2 * period, window, window.Length - period, period) == 0;
    }
}
