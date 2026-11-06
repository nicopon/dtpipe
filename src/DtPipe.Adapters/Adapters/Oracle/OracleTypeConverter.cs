using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using Oracle.ManagedDataAccess.Client;

namespace DtPipe.Adapters.Oracle;

/// <summary>
/// Oracle-specific type mapper implementing CLR ↔ Oracle type conversions.
/// </summary>
public class OracleTypeConverter : ITypeMapper
{
    public static readonly OracleTypeConverter Instance = new();

    public DecimalSpelling DecimalType { get; } = new("NUMBER", 38, 38, BareIsExact: true);

    public string MapToProviderType(Type clrType)
    {
        var type = Nullable.GetUnderlyingType(clrType) ?? clrType;

        if (type == typeof(int)) return "NUMBER(10)";
        if (type == typeof(long)) return "NUMBER(19)";
        if (type == typeof(short)) return "NUMBER(5)";
        if (type == typeof(byte)) return "NUMBER(3)";
        // NUMBER(p) is Oracle's only integer type: every width, signed or not, is one.
        if (type == typeof(sbyte)) return "NUMBER(3)";
        if (type == typeof(ushort)) return "NUMBER(5)";
        if (type == typeof(uint)) return "NUMBER(10)";
        if (type == typeof(ulong)) return "NUMBER(20)";
        if (type == typeof(DateOnly)) return "DATE";
        if (type == typeof(bool)) return "NUMBER(1)";
        if (type == typeof(float)) return "FLOAT";
        if (type == typeof(double)) return "FLOAT";
        if (type == typeof(decimal)) return DecimalType.Undeclared;
        if (type == typeof(DateTime)) return "TIMESTAMP";
        if (type == typeof(DateTimeOffset)) return "TIMESTAMP WITH TIME ZONE";
        if (type == typeof(Guid)) return "RAW(16)";
        if (type == typeof(byte[])) return "BLOB";

        return "VARCHAR2(4000)";
    }

    public Type MapFromProviderType(string providerType)
    {
        // Strip parenthetical parts for base type comparison
        var parenIndex = providerType.IndexOf('(');
        var baseType = parenIndex > 0 ? providerType[..parenIndex].Trim() : providerType.Trim();

        return baseType.ToUpperInvariant() switch
        {
            "NUMBER" => typeof(decimal),
            "INTEGER" => typeof(int),
            "FLOAT" => typeof(double),
            "BINARY_FLOAT" => typeof(float),
            "BINARY_DOUBLE" => typeof(double),
            "VARCHAR2" or "NVARCHAR2" or "CHAR" or "NCHAR" or "CLOB" or "NCLOB" => typeof(string),
            "DATE" or "TIMESTAMP" => typeof(DateTime),
            "TIMESTAMP WITH TIME ZONE" or "TIMESTAMP WITH LOCAL TIME ZONE" => typeof(DateTimeOffset),
            "RAW" or "BLOB" => typeof(byte[]),
            _ => typeof(string)
        };
    }

    public string BuildNativeType(string dataType, int? dataLength, int? precision, int? scale, int? charLength)
    {
        return dataType.ToUpperInvariant() switch
        {
            "VARCHAR2" when charLength.HasValue => $"VARCHAR2({charLength})",
            "CHAR" when charLength.HasValue => $"CHAR({charLength})",
            "NVARCHAR2" when charLength.HasValue => $"NVARCHAR2({charLength})",
            "NCHAR" when charLength.HasValue => $"NCHAR({charLength})",
            "NUMBER" when precision.HasValue && scale.HasValue && scale > 0 => $"NUMBER({precision},{scale})",
            "NUMBER" when precision.HasValue => $"NUMBER({precision})",
            "RAW" when dataLength.HasValue => $"RAW({dataLength})",
            _ => dataType.ToUpperInvariant()
        };
    }

    // === Oracle-specific methods (not part of ITypeMapper) ===

    /// <summary>
    /// The value ODP.NET binds for <paramref name="value"/> so the column holds what it says: a UUID as
    /// its RFC 4122 bytes, an instant with its offset (a bare DateTimeOffset is read in the session's
    /// zone), a date as the midnight an Oracle DATE stores, an unsigned integer as the number it is.
    /// </summary>
    public static object ToBindValue(object value) => value switch
    {
        Guid g => Apache.Arrow.Serialization.Mapping.Rfc4122Guid.ToBigEndianBytes(g),
        DateTimeOffset dto => new global::Oracle.ManagedDataAccess.Types.OracleTimeStampTZ(
            dto.DateTime, dto.Offset.ToString(dto.Offset < TimeSpan.Zero ? "\\-hh\\:mm" : "\\+hh\\:mm")),
        DateOnly d => d.ToDateTime(TimeOnly.MinValue),
        sbyte i8 => (int)i8,
        ushort u16 => (int)u16,
        uint u32 => (long)u32,
        ulong u64 => (decimal)u64,
        _ => value
    };

    /// <summary>
    /// Maps a CLR type to OracleDbType for parameter binding.
    /// This is Oracle-specific and not part of the generic interface.
    /// </summary>
    public static OracleDbType GetOracleDbType(Type type)
    {
        type = Nullable.GetUnderlyingType(type) ?? type;

        if (type == typeof(int) || type == typeof(short) || type == typeof(byte) || type == typeof(sbyte) || type == typeof(ushort))
            return OracleDbType.Int32;
        if (type == typeof(long) || type == typeof(uint))
            return OracleDbType.Int64;
        if (type == typeof(ulong))
            return OracleDbType.Decimal;
        if (type == typeof(decimal))
            return OracleDbType.Decimal;
        if (type == typeof(bool))
            return OracleDbType.Int16;
        if (type == typeof(float))
            return OracleDbType.Single;
        if (type == typeof(double))
            return OracleDbType.Double;
        if (type == typeof(DateTime) || type == typeof(DateOnly))
            return OracleDbType.Date;
        if (type == typeof(DateTimeOffset))
            return OracleDbType.TimeStampTZ;
        if (type == typeof(Guid))
            return OracleDbType.Raw;
        if (type == typeof(byte[]))
            return OracleDbType.Blob;

        return OracleDbType.Varchar2;
    }

    /// <summary>
    /// Maps native Oracle type string to OracleDbType for parameter binding.
    /// </summary>
    public static OracleDbType? MapNativeTypeToOracleDbType(string nativeType)
    {
        // Every parenthesised precision goes, not only the text after the first one: cutting at the
        // first parenthesis reads TIMESTAMP(6) WITH TIME ZONE as TIMESTAMP and drops the zone.
        var baseType = System.Text.RegularExpressions.Regex.Replace(nativeType, @"\s*\([^)]*\)", "").Trim();

        return baseType.ToUpperInvariant() switch
        {
            "RAW" => OracleDbType.Raw,
            "BLOB" => OracleDbType.Blob,
            "CLOB" => OracleDbType.Clob,
            "NCLOB" => OracleDbType.NClob,
            "DATE" => OracleDbType.Date,
            "TIMESTAMP" => OracleDbType.TimeStamp,
            "TIMESTAMP WITH TIME ZONE" => OracleDbType.TimeStampTZ,
            "TIMESTAMP WITH LOCAL TIME ZONE" => OracleDbType.TimeStampLTZ,
            "VARCHAR2" => OracleDbType.Varchar2,
            "NVARCHAR2" => OracleDbType.NVarchar2,
            "CHAR" => OracleDbType.Char,
            "NCHAR" => OracleDbType.NChar,
            "NUMBER" => OracleDbType.Decimal,
            "FLOAT" => OracleDbType.BinaryDouble,
            "BINARY_FLOAT" => OracleDbType.BinaryFloat,
            "BINARY_DOUBLE" => OracleDbType.BinaryDouble,
            "INTERVAL YEAR TO MONTH" => OracleDbType.IntervalYM,
            "INTERVAL DAY TO SECOND" => OracleDbType.IntervalDS,
            _ => null
        };
    }
}
