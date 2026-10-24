using Apache.Arrow;
using DtPipe.Core.Infrastructure.Arrow;

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
    // One row per type. The first spelling is the one written when a hint is produced from a type;
    // the others are accepted when a hint is read.
    private static readonly (Type Type, string[] Names)[] Table =
    [
        (typeof(Guid), ["uuid", "guid"]),
        (typeof(string), ["string", "str", "text"]),
        (typeof(int), ["int32", "int", "integer"]),
        (typeof(long), ["int64", "long"]),
        (typeof(double), ["double", "float64", "number"]),
        (typeof(float), ["float32", "float", "single"]),
        (typeof(decimal), ["decimal", "numeric", "money"]),
        (typeof(bool), ["bool", "boolean"]),
        (typeof(DateTime), ["datetime", "date"]),
        (typeof(DateTimeOffset), ["datetimeoffset", "timestamp", "datetimetz"]),
    ];

    private static readonly Dictionary<string, Type> Hints = Table
        .SelectMany(row => row.Names.Select(name => (name, row.Type)))
        .ToDictionary(x => x.name, x => x.Type, StringComparer.OrdinalIgnoreCase);

    private static readonly Dictionary<Type, string> Spellings = Table.ToDictionary(row => row.Type, row => row.Names[0]);

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

    /// <summary>
    /// The hint that names <paramref name="type"/>, or <c>null</c> when none does. The reverse of
    /// <see cref="ParseTypeHint"/>: what it returns always parses back to the same type.
    /// </summary>
    public static string? HintFor(Type type) => Spellings.GetValueOrDefault(type);

    /// <summary>
    /// The hint that names the scalar a field carries, or <c>null</c> for a field no hint can name
    /// (a nested type, bytes, an integer width the table lacks). It reads the CLR type through the
    /// metadata-aware map, so an <c>arrow.uuid</c> field answers <c>uuid</c> and a zone-less timestamp
    /// <c>datetime</c>, never a guess from the storage type.
    /// </summary>
    public static string? HintFor(Field field)
    {
        try
        {
            return HintFor(ArrowTypeMapper.GetClrTypeFromField(field));
        }
        catch (NotSupportedException)
        {
            return null;
        }
    }

    /// <summary>The accepted spellings, grouped by the type each one names.</summary>
    public static string Accepted { get; } =
        "Accepted: " + string.Join("; ", Hints
            .GroupBy(kv => kv.Value)
            .OrderBy(g => g.Key.Name, StringComparer.Ordinal)
            .Select(g => string.Join(" / ", g.Select(kv => kv.Key).OrderBy(k => k, StringComparer.Ordinal)))) + ".";
}
