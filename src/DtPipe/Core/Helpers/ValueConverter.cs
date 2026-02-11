using System.Globalization;

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
                return Guid.Parse(s);
            if (underlyingTarget == typeof(DateTime))
            {
                if (DateTime.TryParse(s, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var dt))
                    return dt;
                return DateTime.Parse(s, CultureInfo.InvariantCulture, DateTimeStyles.AllowWhiteSpaces);
            }
            if (underlyingTarget == typeof(DateTimeOffset))
            {
                if (DateTimeOffset.TryParse(s, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var dto))
                    return dto;
                return DateTimeOffset.Parse(s, CultureInfo.InvariantCulture, DateTimeStyles.AllowWhiteSpaces);
            }
            if (underlyingTarget == typeof(bool))
                return bool.Parse(s);

            return Convert.ChangeType(s, underlyingTarget, CultureInfo.InvariantCulture);
        }

        return Convert.ChangeType(val, underlyingTarget, CultureInfo.InvariantCulture);
    }
}
