using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Runtime.CompilerServices;
using Apache.Arrow;
using Apache.Arrow.Serialization;
using Apache.Arrow.Serialization.Mapping;
using DtPipe.Core.Abstractions;
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

    /// <summary>The value of a <c>datetime</c> cell, under <see cref="TemporalNormalization.TryParseWallClock"/>.</summary>
    internal static object ReadDateTime(string text)
        => TemporalNormalization.TryParseWallClock(text, out var value) ? value : Unreadable;

    /// <summary>The value of a <c>datetimeoffset</c> cell, under <see cref="TemporalNormalization.TryParseInstant"/>.</summary>
    internal static object ReadDateTimeOffset(string text)
        => TemporalNormalization.TryParseInstant(text, out var value) ? value : Unreadable;

    /// <summary>The value of a <c>date32</c> or <c>date64</c> cell: an ISO date, nothing a culture spells.</summary>
    internal static object ReadDate(string text)
        => DateOnly.TryParseExact(text, "yyyy-MM-dd", CultureInfo.InvariantCulture, DateTimeStyles.None, out var value)
            ? value
            : Unreadable;

    /// <summary>The value of a <c>time32</c> or <c>time64</c> cell: a time of day, as the writers spell it.</summary>
    internal static object ReadTime(string text)
        => TimeOnly.TryParseExact(text, TimeFormats, CultureInfo.InvariantCulture, DateTimeStyles.None, out var value)
            ? value
            : Unreadable;

    private static readonly string[] TimeFormats = ["HH:mm:ss", "HH:mm:ss.FFFFFFF"];

    /// <summary>
    /// Whether inference may call a text a date. <see cref="DateTime.TryParse(string, IFormatProvider, DateTimeStyles, out DateTime)"/>
    /// alone also reads <c>1,5</c> as the fifth of January of the current year: a date needs a
    /// separator or a month's name.
    /// </summary>
    internal static bool ReadsAsDate(string text)
        => text.Any(c => char.IsLetter(c) || c is '-' or '/' or ':')
           && DateTime.TryParse(text, CultureInfo.InvariantCulture, DateTimeStyles.None, out _);

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
        var problem = $"holds a text that does not read as {hint}";
        throw sampledRows is { } sample
            ? new ColumnValueRefusedException(
                where, column, $"inferred as {hint} from the first {sample} row(s) by '--auto-column-types'", problem,
                "Declare the column's type with '--column-types' ('string' keeps the text), or raise " +
                "'--max-sample' if the sample did not reach this cell.")
            : new ColumnValueRefusedException(
                where, column, $"declared {hint}", problem, "Correct the cell, or declare the column 'string'.");
    }

    /// <summary>
    /// The refusal of a value a text reader read and its column's Arrow type cannot take, when the batch
    /// is serialized. <paramref name="firstRecord"/> is the one-based position of the batch's first record.
    /// </summary>
    internal static ColumnValueRefusedException Unfit(ArrowValueException fault, Schema schema, string source, long firstRecord)
        => ColumnValueRefusedException.Unfit(
            fault, schema, $"{source} record {firstRecord + (fault.Record ?? 1) - 1}",
            "Correct the value, or declare the column 'string', which keeps its text.");
}
