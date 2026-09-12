namespace DtPipe.Core.Helpers;

/// <summary>
/// The vocabulary of column type hints, and the single place that resolves one.
/// </summary>
/// <remarks>
/// Four copies of this switch existed — one here and one each in the CSV, JSONL and XML readers —
/// and they had drifted: <c>str</c> resolved on CSV and XML but not on JSONL, <c>text</c> only on
/// JSONL, <c>date</c> and <c>timestamp</c> on neither. The copy here was the poorest, knowing eight
/// bare names and no alias, so <c>--compute-types "Col:int32"</c> resolved to nothing and was
/// dropped — while <c>--column-types "Col:int32"</c> two flags away worked. One map means a hint
/// spells the same thing wherever it is written.
/// </remarks>
public static class TypeHelper
{
    private static readonly Dictionary<string, Type> Hints = new(StringComparer.OrdinalIgnoreCase)
    {
        ["uuid"] = typeof(Guid),
        ["guid"] = typeof(Guid),
        ["string"] = typeof(string),
        ["str"] = typeof(string),
        ["text"] = typeof(string),
        ["int"] = typeof(int),
        ["int32"] = typeof(int),
        ["integer"] = typeof(int),
        ["long"] = typeof(long),
        ["int64"] = typeof(long),
        ["double"] = typeof(double),
        ["float64"] = typeof(double),
        ["number"] = typeof(double),
        ["float"] = typeof(float),
        ["float32"] = typeof(float),
        ["single"] = typeof(float),
        ["decimal"] = typeof(decimal),
        ["numeric"] = typeof(decimal),
        ["money"] = typeof(decimal),
        ["bool"] = typeof(bool),
        ["boolean"] = typeof(bool),
        ["datetime"] = typeof(DateTime),
        ["date"] = typeof(DateTime),
        ["datetimeoffset"] = typeof(DateTimeOffset),
        ["timestamp"] = typeof(DateTimeOffset),
        ["datetimetz"] = typeof(DateTimeOffset),
    };

    /// <summary>
    /// The CLR type a hint names, or <c>null</c> when it names none.
    /// </summary>
    /// <remarks>
    /// Null is an answer, not a failure: <c>--compute "Col:row.a ? 1 : 2"</c> splits into three
    /// parts whose middle is not a type, and that is how the two-part form is told from the
    /// typed three-part one. A caller reading a hint the user wrote as a hint wants
    /// <see cref="RequireTypeHint"/> instead.
    /// </remarks>
    public static Type? ParseTypeHint(string typeHint)
        => typeHint is not null && Hints.TryGetValue(typeHint.Trim(), out var type) ? type : null;

    /// <summary>
    /// The CLR type a hint names, refusing one that names none.
    /// </summary>
    /// <remarks>
    /// Every call site used to drop an unresolved hint and carry on with the column's incoming
    /// type, so a typo left no trace at all: the run wrote a String column where an Int32 was
    /// asked for and exited 0.
    /// </remarks>
    public static Type RequireTypeHint(string flag, string column, string? hint)
        => string.IsNullOrWhiteSpace(hint)
            ? throw new InvalidOperationException($"{flag} declares '{column}' with no type. {Accepted}")
            : ParseTypeHint(hint)
              ?? throw new InvalidOperationException(
                  $"{flag} declares '{column}' as '{hint}', which names no type. {Accepted}");

    /// <summary>The accepted spellings, grouped by the type each one names.</summary>
    public static string Accepted { get; } =
        "Accepted: " + string.Join("; ", Hints
            .GroupBy(kv => kv.Value)
            .OrderBy(g => g.Key.Name, StringComparer.Ordinal)
            .Select(g => string.Join(" / ", g.Select(kv => kv.Key).OrderBy(k => k, StringComparer.Ordinal)))) + ".";
}
