using System;
using System.Text;
using Spectre.Console;

namespace DtPipe.Cli.Agent;

/// <summary>
/// What a keypress meant beyond editing text. The shell owns the reaction; the editor only
/// classifies — so the same key vocabulary is asserted in tests without a console, and it is the
/// single home for the in-loop shortcuts that were the remainder of lot C.
/// </summary>
public enum EditorSignal
{
    /// <summary>An editing key (or nothing) — <see cref="LineEditor.Text"/> may have changed.</summary>
    None,

    /// <summary>Enter — the line is complete; <see cref="LineEditor.Text"/> carries it.</summary>
    Submit,

    /// <summary>Esc — a soft interrupt: cancel the work in flight, keep the session.</summary>
    Interrupt,

    /// <summary>Ctrl+C — leave the agent.</summary>
    Quit,

    /// <summary>Ctrl+O — cycle the scrollback detail level.</summary>
    CycleDetail,

    /// <summary>Shift+Tab — cycle the operating mode.</summary>
    CycleMode,
}

/// <summary>
/// A single-line input editor driven one <see cref="ConsoleKeyInfo"/> at a time. Pure — it does no
/// console I/O — so a scripted key sequence is asserted directly. The shell reads keys and feeds
/// them here; <see cref="Apply"/> returns the non-text meaning of the key (see <see cref="EditorSignal"/>).
/// </summary>
internal sealed class LineEditor
{
    private readonly StringBuilder _buffer = new();
    private int _cursor;

    public string Text => _buffer.ToString();
    public int Cursor => _cursor;
    public bool IsEmpty => _buffer.Length == 0;

    /// <summary>Applies one keypress. Editing keys mutate the buffer and return <see cref="EditorSignal.None"/>.</summary>
    public EditorSignal Apply(ConsoleKeyInfo key)
    {
        bool ctrl = key.Modifiers.HasFlag(ConsoleModifiers.Control);
        bool shift = key.Modifiers.HasFlag(ConsoleModifiers.Shift);

        switch (key.Key)
        {
            case ConsoleKey.Enter:
                return EditorSignal.Submit;
            case ConsoleKey.Escape:
                return EditorSignal.Interrupt;
            case ConsoleKey.C when ctrl:
                return EditorSignal.Quit;
            case ConsoleKey.O when ctrl:
                return EditorSignal.CycleDetail;
            case ConsoleKey.Tab when shift:
                return EditorSignal.CycleMode;

            case ConsoleKey.Backspace:
                if (_cursor > 0) _buffer.Remove(--_cursor, 1);
                return EditorSignal.None;
            case ConsoleKey.Delete:
                if (_cursor < _buffer.Length) _buffer.Remove(_cursor, 1);
                return EditorSignal.None;
            case ConsoleKey.LeftArrow:
                if (_cursor > 0) _cursor--;
                return EditorSignal.None;
            case ConsoleKey.RightArrow:
                if (_cursor < _buffer.Length) _cursor++;
                return EditorSignal.None;
            case ConsoleKey.Home:
                _cursor = 0;
                return EditorSignal.None;
            case ConsoleKey.End:
                _cursor = _buffer.Length;
                return EditorSignal.None;
            case ConsoleKey.Tab:
                return EditorSignal.None;   // reserved for completion; not text
        }

        if (!ctrl && key.KeyChar != '\0' && !char.IsControl(key.KeyChar))
            _buffer.Insert(_cursor++, key.KeyChar);

        return EditorSignal.None;
    }

    public void Reset()
    {
        _buffer.Clear();
        _cursor = 0;
    }

    public void Set(string text)
    {
        _buffer.Clear();
        _buffer.Append(text ?? string.Empty);
        _cursor = _buffer.Length;
    }

    /// <summary>The input line as markup: a prompt glyph, the text, and a block cursor.</summary>
    public string RenderMarkup(string promptGlyph = "›")
    {
        string text = _buffer.ToString();
        int c = Math.Clamp(_cursor, 0, text.Length);
        string before = Markup.Escape(text[..c]);
        string atCursor = c < text.Length ? Markup.Escape(text[c].ToString()) : " ";
        string after = c < text.Length ? Markup.Escape(text[(c + 1)..]) : string.Empty;
        return $"[grey]{Markup.Escape(promptGlyph)}[/] {before}[invert]{atCursor}[/]{after}";
    }
}
