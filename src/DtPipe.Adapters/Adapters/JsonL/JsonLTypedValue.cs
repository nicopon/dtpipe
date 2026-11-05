using System.Globalization;
using System.Text.Json;
using DtPipe.Adapters.Common;
using DtPipe.Adapters.Xml;

namespace DtPipe.Adapters.JsonL;

/// <summary>
/// How the JSONL reader reads a value into the type <c>--column-types</c> declares for its column:
/// only what reads without loss. A number reads into a numeric type when the type holds it (<c>2.0</c>
/// into int32, never <c>1.5</c>, nor a number past the type's range), <c>true</c> and <c>false</c>
/// into bool only, and a string through the type's text parser under <see cref="TypedTextCell"/>'s
/// rule (empty is null, blank is refused). An object or an array never reads into a scalar.
/// </summary>
internal static class JsonLTypedValue
{
    /// <summary>
    /// The reader of a declared column, or null when the column keeps the value as the JSON gives it:
    /// a string column, read as text by the reader. It returns <see cref="TypedTextCell.Unreadable"/> for a value it does not read.
    /// </summary>
    internal static Func<JsonElement, object?>? BuildReader(Type clrType)
    {
        if (clrType == typeof(int))
            return Of(static e => Integral(e, int.MinValue, int.MaxValue) is long v ? (int)v : TypedTextCell.Unreadable,
                XmlTypeInferrer.BuildParser(typeof(int))!);

        if (clrType == typeof(long))
            return Of(static e => Integral(e, long.MinValue, long.MaxValue) ?? TypedTextCell.Unreadable,
                XmlTypeInferrer.BuildParser(typeof(long))!);

        // The text of a double or a float takes a thousands separator ("1,000" reads as 1000) and the
        // spelled-out NaN and Infinity; a number too large for the type is refused, never read as ±∞.
        if (clrType == typeof(double))
            return Of(static e => e.TryGetDouble(out var v) && double.IsFinite(v) ? v : TypedTextCell.Unreadable,
                static s => double.TryParse(s, NumberStyles.Float | NumberStyles.AllowThousands, CultureInfo.InvariantCulture, out var v)
                    && !(double.IsInfinity(v) && Overflowed(s)) ? v : TypedTextCell.Unreadable);

        if (clrType == typeof(float))
            return Of(static e => e.TryGetSingle(out var v) && float.IsFinite(v) ? v : TypedTextCell.Unreadable,
                static s => float.TryParse(s, NumberStyles.Float | NumberStyles.AllowThousands, CultureInfo.InvariantCulture, out var v)
                    && !(float.IsInfinity(v) && Overflowed(s)) ? v : TypedTextCell.Unreadable);

        // Off the JSON text, never through a double, which keeps 15 digits of a decimal's 28.
        if (clrType == typeof(decimal))
            return Of(static e => e.TryGetDecimal(out var v) ? v : TypedTextCell.Unreadable,
                XmlTypeInferrer.BuildParser(typeof(decimal))!);

        if (clrType == typeof(bool))
            return Of(null, XmlTypeInferrer.BuildParser(typeof(bool))!, readsBoolean: true);

        if (clrType == typeof(DateTime) || clrType == typeof(DateTimeOffset) || clrType == typeof(Guid))
            return Of(null, XmlTypeInferrer.BuildParser(clrType)!);

        return null;
    }

    private static Func<JsonElement, object?> Of(
        Func<JsonElement, object?>? number, Func<string, object?> text, bool readsBoolean = false)
        => e => e.ValueKind switch
        {
            JsonValueKind.Null => null,
            JsonValueKind.String => TypedTextCell.Parse(text, e.GetString()!),
            JsonValueKind.Number when number != null => number(e),
            JsonValueKind.True when readsBoolean => true,
            JsonValueKind.False when readsBoolean => false,
            _ => TypedTextCell.Unreadable
        };

    /// <summary>
    /// Whether a text that parsed to ±∞ wrote a number past the type's range rather than the infinity
    /// symbol: the symbol holds no digit.
    /// </summary>
    private static bool Overflowed(string text) => text.AsSpan().IndexOfAnyInRange('0', '9') >= 0;

    /// <summary>
    /// The integer a JSON number is, written <c>2</c>, <c>2.0</c> or <c>2e0</c>, when it lies within
    /// [<paramref name="min"/>, <paramref name="max"/>]; null for a fraction or a number outside.
    /// </summary>
    private static long? Integral(JsonElement e, long min, long max)
    {
        if (e.TryGetInt64(out var l)) return l >= min && l <= max ? l : null;
        return e.TryGetDecimal(out var m) && m == decimal.Truncate(m) && m >= min && m <= max ? (long)m : null;
    }
}
