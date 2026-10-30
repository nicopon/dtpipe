using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using DtPipe.Core.Helpers;

namespace DtPipe.Adapters.Common;

/// <summary>
/// How a text reader reads a cell of a column it was given a type for (<c>--column-types</c>,
/// <c>--auto-column-types</c>): an empty cell is null, a text the column's parser reads is its value,
/// and any other text, blank included, stops the run naming the column, the type and where the cell
/// stands. The CSV and XML readers keep their own parsers, which accept different spellings; this
/// class holds what they share, so an unread cell is never written as null by one of them.
/// </summary>
internal static class TypedTextCell
{
    /// <summary>What a column's parser returns for a text it does not read. Never leaves the reader.</summary>
    internal static readonly object Unreadable = new();

    /// <summary>
    /// The value of <paramref name="text"/> under <paramref name="parser"/>: null for an empty text,
    /// <see cref="Unreadable"/> for one the parser does not read.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static object? Parse(Func<string, object?> parser, string text)
        => text.Length == 0 ? null : parser(text);

    /// <summary>
    /// Stops the run on a cell the column's type does not read. <paramref name="sampledRows"/> is the
    /// sample the type was inferred from, or null when the type was declared. The value is not
    /// quoted: a refused cell can hold anything the source holds.
    /// </summary>
    [DoesNotReturn]
    [MethodImpl(MethodImplOptions.NoInlining)]
    internal static void Refuse(string where, string column, Type type, int? sampledRows)
    {
        var hint = TypeHelper.HintFor(type) ?? type.Name;
        throw new InvalidOperationException(sampledRows is { } sample
            ? $"{where}: column '{column}' was inferred as {hint} from the first {sample} row(s) " +
              $"('--auto-column-types'), and its text does not read as {hint}. Declare the column's type " +
              "with '--column-types' ('string' keeps the text), or raise '--max-sample' if the sample " +
              "did not reach this cell."
            : $"{where}: column '{column}' is declared {hint}, and its text does not read as {hint}. " +
              "Correct the cell, or declare the column 'string'.");
    }
}
