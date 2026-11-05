using Apache.Arrow.Serialization.Mapping;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Runtime.CompilerServices;

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
    /// Returns <c>DBNull.Value</c> (not null) for null or DBNull input, and for an empty string
    /// into a parsed type, matching the contract expected by ADO.NET parameters. Text that is not
    /// empty and does not read as the target throws <see cref="InvalidOperationException"/> naming
    /// <paramref name="columnName"/> and the target type: a value the writer cannot read is refused,
    /// never written as NULL.
    /// </summary>
    public static Func<object?, object?> Build(Type? sourceType, Type targetType, string columnName)
    {
        var underlying = Nullable.GetUnderlyingType(targetType) ?? targetType;

        // Fast path: source is already assignable to target — identity, no conversion needed
        if (sourceType != null && (underlying == sourceType || underlying.IsAssignableFrom(sourceType)))
            return static v => v ?? DBNull.Value;

        // String source — the dominant slow path (CSV readers, text APIs)
        if (sourceType == typeof(string))
            return BuildFromString(underlying, columnName);

        // Fallback: delegate to ValueConverter for numeric coercions, DateTimeOffset→DateTime, etc.
        return v => v is null || v == DBNull.Value
            ? DBNull.Value
            : ValueConverter.ConvertValue(v, underlying, columnName);
    }

    // Each parsed target: a null, DBNull or empty cell is DBNull; any other text either reads as the
    // target or is refused. Whitespace alone is not empty.
    private static Func<object?, object?> BuildFromString(Type target, string columnName)
    {
        if (target == typeof(int))
            return v => v is not string { Length: > 0 } s ? DBNull.Value
                : int.TryParse(s, NumberStyles.Integer, CultureInfo.InvariantCulture, out var r) ? r
                : Unreadable(columnName, target);

        if (target == typeof(long))
            return v => v is not string { Length: > 0 } s ? DBNull.Value
                : long.TryParse(s, NumberStyles.Integer, CultureInfo.InvariantCulture, out var r) ? r
                : Unreadable(columnName, target);

        if (target == typeof(short))
            return v => v is not string { Length: > 0 } s ? DBNull.Value
                : short.TryParse(s, NumberStyles.Integer, CultureInfo.InvariantCulture, out var r) ? r
                : Unreadable(columnName, target);

        if (target == typeof(double))
            return v => v is not string { Length: > 0 } s ? DBNull.Value
                : TextNumber.TryReadDouble(s, out var r) ? r
                : Unreadable(columnName, target);

        if (target == typeof(float))
            return v => v is not string { Length: > 0 } s ? DBNull.Value
                : TextNumber.TryReadFloat(s, out var r) ? r
                : Unreadable(columnName, target);

        if (target == typeof(decimal))
            return v => v is not string { Length: > 0 } s ? DBNull.Value
                : TextNumber.TryReadDecimal(s, out var r) ? r
                : Unreadable(columnName, target);

        if (target == typeof(bool))
            return v =>
            {
                if (v is not string { Length: > 0 } s) return DBNull.Value;
                if (bool.TryParse(s, out var b)) return b;
                return s switch
                {
                    "1" or "yes" or "true" => true,
                    "0" or "no" or "false" => false,
                    _ => Unreadable(columnName, target)
                };
            };

        if (target == typeof(DateTime))
            return v => v is not string { Length: > 0 } s ? DBNull.Value
                : TemporalNormalization.TryParseWallClock(s, out var r) ? r
                : Unreadable(columnName, target);

        if (target == typeof(DateTimeOffset))
            return v => v is not string { Length: > 0 } s ? DBNull.Value
                : TemporalNormalization.TryParseInstant(s, out var r) ? r
                : Unreadable(columnName, target);

        if (target == typeof(Guid))
            return v => v is not string { Length: > 0 } s ? DBNull.Value
                : Guid.TryParse(s, out var r) ? r
                : Unreadable(columnName, target);

        // Fallback for all other string→T conversions
        return v => v is null || v == DBNull.Value
            ? DBNull.Value
            : ValueConverter.ConvertValue(v, target);
    }

    // Kept out of the parse lambdas so the per-cell delegate stays small. The value is not quoted:
    // a refused cell can hold anything the source holds.
    [DoesNotReturn]
    [MethodImpl(MethodImplOptions.NoInlining)]
    private static object Unreadable(string columnName, Type target)
        => throw new InvalidOperationException(
            $"Column '{columnName}' is {target.Name} and the source sends text that does not read as " +
            $"{target.Name}, which the writer does not convert: convert it in the query.");
}
