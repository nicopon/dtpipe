using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace DtPipe.Cli.Agent;

/// <summary>
/// The turn's transcript as shared mutable state: the loop appends <see cref="TranscriptEntry"/>
/// from a worker thread while a full-screen surface reads it from the UI thread. Every member is
/// guarded by one lock, and <see cref="Version"/> lets a polling repaint skip the work when nothing
/// changed — that is what makes the refresh a pull rather than one marshalled call per token.
/// Pure: no console, no toolkit.
/// </summary>
internal sealed class TranscriptLog
{
    private readonly object _gate = new();
    private readonly List<TranscriptEntry> _entries = new();
    private string? _liveTail;
    private long _version;

    /// <summary>Bumped on every mutation. A reader that saw this value has seen everything.</summary>
    public long Version { get { lock (_gate) return _version; } }

    /// <summary>
    /// The step currently streaming — shown below the committed entries, never part of them.
    /// Writing the value it already holds is not a change: the repaint poll assigns this every
    /// tick, and counting that as a mutation would rebuild the view ten times a second forever.
    /// </summary>
    public string? LiveTail
    {
        get { lock (_gate) return _liveTail; }
        set
        {
            lock (_gate)
            {
                if (string.Equals(_liveTail, value, StringComparison.Ordinal)) return;
                _liveTail = value;
                _version++;
            }
        }
    }

    public void Append(TranscriptEntry entry)
    {
        lock (_gate) { _entries.Add(entry); _version++; }
    }

    /// <summary>A snapshot, safe to read while the turn is still running.</summary>
    public IReadOnlyList<TranscriptEntry> Entries
    {
        get { lock (_gate) return _entries.ToArray(); }
    }

    /// <summary>
    /// Every committed entry's scrollback markup, in order. This is what is replayed to the real
    /// terminal once the full-screen surface has been torn down; the live tail is deliberately
    /// absent, so a half-streamed step never lands in the permanent record.
    /// </summary>
    public IReadOnlyList<string> MarkupLines()
    {
        lock (_gate) return _entries.SelectMany(e => e.MarkupLines).ToArray();
    }

    /// <summary>
    /// The committed text plus the live tail, as plain lines — what a toolkit list pane displays.
    /// <paramref name="maxLines"/> keeps only the tail, so a long transcript costs a bounded list.
    /// </summary>
    public List<string> PlainLines(int maxLines = 500)
    {
        List<TranscriptEntry> entries;
        string? tail;
        lock (_gate)
        {
            entries = new List<TranscriptEntry>(_entries);
            tail = _liveTail;
        }

        var lines = new List<string>();
        foreach (var e in entries)
            lines.AddRange(e.Text.Replace("\r", string.Empty).Split('\n'));
        if (!string.IsNullOrEmpty(tail))
            lines.AddRange(tail.Replace("\r", string.Empty).Split('\n'));

        return lines.Count <= maxLines ? lines : lines.Skip(lines.Count - maxLines).ToList();
    }

    /// <summary>The same content as one string — convenient for assertions and diagnostics.</summary>
    public string PlainText(int maxLines = 500) => string.Join('\n', PlainLines(maxLines));
}
