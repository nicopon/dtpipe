using System.Collections.Generic;

namespace DtPipe.Cli.Agent.Tui;

/// <summary>
/// Folds text to a column width. Pure, so the folding is asserted without a terminal.
///
/// <para>
/// The panels are lists of lines and a list clips what overruns its width rather than folding it,
/// so a long line would simply lose its end off the right edge — and the content these panels show
/// is a model's prose and a tool's JSON, neither of which arrives pre-folded. Words are kept whole
/// where they fit; a word longer than the whole width is cut, because there is nowhere else for it
/// to go.
/// </para>
/// </summary>
internal static class TextWrap
{
    public static IReadOnlyList<string> Fold(string text, int width)
    {
        var source = text.Replace("\r", string.Empty).Split('\n');
        if (width <= 1) return source;

        var folded = new List<string>();
        foreach (var line in source)
        {
            if (line.Length <= width) { folded.Add(line); continue; }
            FoldOne(line, width, folded);
        }
        return folded;
    }

    private static void FoldOne(string line, int width, List<string> into)
    {
        // Indented bodies stay indented: a continuation that starts at column zero reads as a new
        // section rather than as the rest of the one above it.
        int indent = 0;
        while (indent < line.Length && line[indent] == ' ') indent++;
        var pad = indent > 0 && indent < width - 8 ? new string(' ', indent) : string.Empty;

        int cursor = 0;
        while (cursor < line.Length)
        {
            var prefix = into.Count == 0 || cursor == 0 ? string.Empty : pad;
            int room = width - prefix.Length;
            if (room <= 1) room = width;

            if (line.Length - cursor <= room)
            {
                into.Add(prefix + line[cursor..]);
                return;
            }

            int cut = line.LastIndexOf(' ', cursor + room - 1, room);
            if (cut <= cursor) cut = cursor + room;          // one unbroken word: cut it
            into.Add(prefix + line[cursor..cut].TrimEnd());
            cursor = cut;
            while (cursor < line.Length && line[cursor] == ' ') cursor++;
        }
    }
}
