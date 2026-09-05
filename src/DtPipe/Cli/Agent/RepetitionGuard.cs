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
    /// <see cref="_matchChars"/>-long chunk is found to already have occurred <see cref="_minRepeats"/>
    /// times (this occurrence included) within the tracked window.</summary>
    public bool Feed(string delta)
    {
        if (string.IsNullOrEmpty(delta)) return false;

        _buffer.Append(delta);
        if (_buffer.Length > _windowChars)
            _buffer.Remove(0, _buffer.Length - _windowChars);

        if (_buffer.Length < _matchChars) return false;

        string window = _buffer.ToString();
        string probe = window[^_matchChars..];

        int count = 0, idx = 0;
        while (count < _minRepeats)
        {
            idx = window.IndexOf(probe, idx, StringComparison.Ordinal);
            if (idx < 0) break;
            count++;
            idx++;
        }
        return count >= _minRepeats;
    }
}
