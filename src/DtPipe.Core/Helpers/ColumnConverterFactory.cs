using System.Globalization;

namespace DtPipe.Core.Helpers;

/// <summary>
/// Compiles a typed converter delegate for a (sourceType → targetType) column pair.
/// Call <see cref="Build"/> once per column during writer initialization, then invoke
/// the returned delegate on each cell value in the write loop.
///
/// Eliminates the per-cell if/else cascade in <see cref="ValueConverter.ConvertValue"/>
/// by pre-selecting the conversion logic at initialization time.
/// </summary>
public static class ColumnConverterFactory
{
    /// <summary>
    /// Returns a <c>Func&lt;object?, object?&gt;</c> that converts a value from
    /// <paramref name="sourceType"/> to <paramref name="targetType"/>.
    /// <list type="bullet">
    ///   <item>Returns an identity lambda when types are already compatible.</item>
    ///   <item>Returns a typed parse lambda for the common string→primitive slow path (CSV).</item>
    ///   <item>Falls back to <see cref="ValueConverter.ConvertValue"/> for all other cases.</item>
    /// </list>
    /// Returns <c>DBNull.Value</c> (not null) for null/missing/unparseable input,
    /// matching the contract expected by ADO.NET parameters.
    /// </summary>
    public static Func<object?, object?> Build(Type? sourceType, Type targetType)
    {
        var underlying = Nullable.GetUnderlyingType(targetType) ?? targetType;

        // Fast path: source is already assignable to target — identity, no conversion needed
        if (sourceType != null && (underlying == sourceType || underlying.IsAssignableFrom(sourceType)))
            return static v => v ?? DBNull.Value;

        // String source — the dominant slow path (CSV readers, text APIs)
        if (sourceType == typeof(string))
            return BuildFromString(underlying);

        // Fallback: delegate to ValueConverter for numeric coercions, long→DateTime, etc.
        return v => v is null || v == DBNull.Value
            ? DBNull.Value
            : ValueConverter.ConvertValue(v, underlying);
    }

    private static Func<object?, object?> BuildFromString(Type target)
    {
        if (target == typeof(int))
            return static v => v is string s && int.TryParse(s, NumberStyles.Integer, CultureInfo.InvariantCulture, out var r)
                ? (object?)r : DBNull.Value;

        if (target == typeof(long))
            return static v => v is string s && long.TryParse(s, NumberStyles.Integer, CultureInfo.InvariantCulture, out var r)
                ? (object?)r : DBNull.Value;

        if (target == typeof(short))
            return static v => v is string s && short.TryParse(s, NumberStyles.Integer, CultureInfo.InvariantCulture, out var r)
                ? (object?)r : DBNull.Value;

        if (target == typeof(double))
            return static v => v is string s && double.TryParse(s, NumberStyles.Float, CultureInfo.InvariantCulture, out var r)
                ? (object?)r : DBNull.Value;

        if (target == typeof(float))
            return static v => v is string s && float.TryParse(s, NumberStyles.Float, CultureInfo.InvariantCulture, out var r)
                ? (object?)r : DBNull.Value;

        if (target == typeof(decimal))
            return static v => v is string s && decimal.TryParse(s, NumberStyles.Number, CultureInfo.InvariantCulture, out var r)
                ? (object?)r : DBNull.Value;

        if (target == typeof(bool))
            return static v =>
            {
                if (v is not string s) return DBNull.Value;
                if (bool.TryParse(s, out var b)) return b;
                return s switch { "1" or "yes" or "true" => true, "0" or "no" or "false" => false, _ => (object?)DBNull.Value };
            };

        if (target == typeof(DateTime))
            return static v => v is string s
                && DateTime.TryParse(s, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var r)
                ? (object?)r : DBNull.Value;

        if (target == typeof(DateTimeOffset))
            return static v => v is string s
                && DateTimeOffset.TryParse(s, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var r)
                ? (object?)r : DBNull.Value;

        if (target == typeof(Guid))
            return static v => v is string s && Guid.TryParse(s, out var r)
                ? (object?)r : DBNull.Value;

        // Fallback for all other string→T conversions
        return v => v is null || v == DBNull.Value
            ? DBNull.Value
            : ValueConverter.ConvertValue(v, target);
    }
}
