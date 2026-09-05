using System;
using System.Text;

namespace DtPipe.Cli.Agent;

/// <summary>
/// Incrementally splits a model's content stream into reasoning and answer, when the model marks
/// its reasoning with <c>&lt;think&gt;…&lt;/think&gt;</c> in the content itself (qwen3 and similar)
/// rather than in a dedicated field. Feed each raw content delta to <see cref="Push"/>; it routes
/// the text inside a think block to <see cref="ILlmStreamObserver.OnThinking"/> and the rest to
/// <see cref="ILlmStreamObserver.OnContent"/>.
///
/// A tag may straddle two deltas (<c>"&lt;thi"</c> + <c>"nk&gt;"</c>), so a trailing run that could
/// be the start of a tag is held back until the next push or the final <see cref="Flush"/>.
/// </summary>
internal sealed class ThinkTagSplitter
{
    private const string Open = "<think>";
    private const string Close = "</think>";

    private readonly StringBuilder _buffer = new();
    private bool _inThink;

    /// <summary>Whether the stream is currently inside a think block (for the caller's own display).</summary>
    public bool InThink => _inThink;

    public void Push(string delta, ILlmStreamObserver observer)
    {
        if (string.IsNullOrEmpty(delta)) return;
        _buffer.Append(delta);
        Drain(observer, final: false);
    }

    /// <summary>Emit whatever is left, treating a trailing partial tag as literal text.</summary>
    public void Flush(ILlmStreamObserver observer) => Drain(observer, final: true);

    private void Drain(ILlmStreamObserver observer, bool final)
    {
        var s = _buffer.ToString();
        int pos = 0;

        while (pos < s.Length)
        {
            string wanted = _inThink ? Close : Open;
            int tag = s.IndexOf(wanted, pos, StringComparison.Ordinal);

            if (tag < 0)
            {
                // No complete tag ahead. Emit everything except a tail that might be a tag prefix.
                int safe = final ? s.Length : s.Length - LongestTagPrefixSuffix(s);
                if (safe > pos)
                    Emit(observer, s, pos, safe - pos);
                pos = Math.Max(pos, safe);
                break;
            }

            if (tag > pos)
                Emit(observer, s, pos, tag - pos);

            _inThink = !_inThink;
            pos = tag + wanted.Length;
        }

        _buffer.Clear();
        if (pos < s.Length)
            _buffer.Append(s, pos, s.Length - pos);
    }

    private void Emit(ILlmStreamObserver observer, string s, int start, int length)
    {
        var chunk = s.Substring(start, length);
        if (_inThink) observer.OnThinking(chunk);
        else observer.OnContent(chunk);
    }

    /// <summary>Length of the longest suffix of <paramref name="s"/> that is a proper prefix of
    /// either tag — the run we must hold back in case the rest of the tag arrives next.</summary>
    private static int LongestTagPrefixSuffix(string s)
    {
        int max = Math.Min(s.Length, Math.Max(Open.Length, Close.Length) - 1);
        for (int len = max; len > 0; len--)
        {
            var tail = s.AsSpan(s.Length - len);
            if (Open.AsSpan(0, Math.Min(len, Open.Length)).SequenceEqual(tail) ||
                Close.AsSpan(0, Math.Min(len, Close.Length)).SequenceEqual(tail))
                return len;
        }
        return 0;
    }
}
