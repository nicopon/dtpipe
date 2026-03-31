using System.Globalization;
using DtPipe.Core.Infrastructure.Arrow;

namespace DtPipe.Core.Helpers;

/// <summary>
/// Centralized value conversion from CLR objects to target types.
/// Used by all database writers to convert source values (often strings from CSV)
/// to the CLR types expected by database parameters.
/// </summary>
public static class ValueConverter
{
    /// <summary>
    /// Converts a value to the specified target CLR type.
    /// Handles null/DBNull, string parsing (DateTime, Guid, bool, etc.), and general type conversion.
    /// </summary>
    /// <param name="val">The source value to convert. May be null, DBNull, or any CLR object.</param>
    /// <param name="targetType">The target CLR type. May be a Nullable type.</param>
    /// <returns>The converted value, or DBNull.Value if the input is null/DBNull.</returns>
    public static object ConvertValue(object? val, Type targetType)
    {
        if (val is null || val == DBNull.Value) return DBNull.Value;

        var underlyingTarget = Nullable.GetUnderlyingType(targetType) ?? targetType;

        if (underlyingTarget.IsInstanceOfType(val)) return val;

        if (val is string s)
        {
            if (underlyingTarget == typeof(Guid))
            {
                if (string.IsNullOrWhiteSpace(s)) return DBNull.Value;
                return Guid.TryParse(s, out var g) ? g : Guid.Parse(s);
            }
            if (underlyingTarget == typeof(DateTime))
            {
                if (DateTime.TryParse(s, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var dt))
                    return dt;
                if (DateTime.TryParse(s, CultureInfo.InvariantCulture, DateTimeStyles.AllowWhiteSpaces, out dt))
                    return dt;
                return DateTime.Parse(s, CultureInfo.InvariantCulture);
            }
            if (underlyingTarget == typeof(DateTimeOffset))
            {
                if (DateTimeOffset.TryParse(s, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var dto))
                    return dto;
                if (DateTimeOffset.TryParse(s, CultureInfo.InvariantCulture, DateTimeStyles.AllowWhiteSpaces, out dto))
                    return dto;
                return DateTimeOffset.Parse(s, CultureInfo.InvariantCulture);
            }
            if (underlyingTarget == typeof(bool))
                return bool.TryParse(s, out var b) ? b : bool.Parse(s);

            if (underlyingTarget.IsEnum)
                return Enum.Parse(underlyingTarget, s, true);

            return Convert.ChangeType(s, underlyingTarget, CultureInfo.InvariantCulture);
        }

        if (val is byte[] bArr)
        {
            if (underlyingTarget == typeof(Guid))
            {
                // Arrow bytes are RFC 4122 big-endian; convert to .NET Guid
                if (bArr.Length == 16) return ArrowTypeMapper.FromArrowUuidBytes(bArr);
            }
        }

        if (val is Guid gVal)
        {
            // Produce RFC 4122 big-endian bytes for Arrow FixedSizeBinary(16) columns
            if (underlyingTarget == typeof(byte[])) return ArrowTypeMapper.ToArrowUuidBytes(gVal);
            if (underlyingTarget == typeof(string)) return gVal.ToString();
        }

        if (val is DateTime dtVal && underlyingTarget == typeof(DateTimeOffset))
        {
            return new DateTimeOffset(dtVal);
        }

        if (val is long longVal)
        {
            if (underlyingTarget == typeof(DateTime))
            {
                // Heuristic: if > 1e12, assume milliseconds, else assume seconds
                if (longVal > 1_000_000_000_000) return DateTimeOffset.FromUnixTimeMilliseconds(longVal).UtcDateTime;
                return DateTimeOffset.FromUnixTimeSeconds(longVal).UtcDateTime;
            }
            if (underlyingTarget == typeof(DateTimeOffset))
            {
                if (longVal > 1_000_000_000_000) return DateTimeOffset.FromUnixTimeMilliseconds(longVal);
                return DateTimeOffset.FromUnixTimeSeconds(longVal);
            }
        }

        return Convert.ChangeType(val, underlyingTarget, CultureInfo.InvariantCulture);
    }
}
