using DtPipe.Core.Abstractions;

namespace DtPipe.Adapters.DuckDB;

/// <summary>
/// DuckDB-specific type mapper implementing CLR ↔ DuckDB type conversions.
/// </summary>
public class DuckDbTypeConverter : ITypeMapper
{
    public static readonly DuckDbTypeConverter Instance = new();

    public DecimalSpelling DecimalType { get; } = new("DECIMAL", 38, 38);

    public string MapToProviderType(Type clrType)
    {
        var underlying = Nullable.GetUnderlyingType(clrType) ?? clrType;

        return underlying switch
        {
            _ when underlying == typeof(int) => "INTEGER",
            _ when underlying == typeof(long) => "BIGINT",
            _ when underlying == typeof(short) => "SMALLINT",
            _ when underlying == typeof(byte) => "UTINYINT",
            _ when underlying == typeof(sbyte) => "TINYINT",
            _ when underlying == typeof(ushort) => "USMALLINT",
            _ when underlying == typeof(uint) => "UINTEGER",
            _ when underlying == typeof(ulong) => "UBIGINT",
            _ when underlying == typeof(Int128) => "HUGEINT",
            _ when underlying == typeof(bool) => "BOOLEAN",
            _ when underlying == typeof(float) => "FLOAT",
            _ when underlying == typeof(double) => "DOUBLE",
            _ when underlying == typeof(decimal) => DecimalType.Undeclared,
            _ when underlying == typeof(DateTime) => "TIMESTAMP",
            _ when underlying == typeof(DateTimeOffset) => "TIMESTAMPTZ",
            _ when underlying == typeof(DateOnly) => "DATE",
            _ when underlying == typeof(TimeOnly) => "TIME",
            _ when underlying == typeof(Guid) => "UUID",
            _ when underlying == typeof(byte[]) => "BLOB",
            _ when ListElementOf(underlying) is { } element => MapToProviderType(element) + "[]",
            _ => "VARCHAR"
        };
    }

    /// <summary>
    /// The element of a list the writer's appender writes as a LIST, or null for any other type: a list
    /// of composites or of bytes has no form there, and an untyped list says nothing of its element.
    /// </summary>
    public static Type? ListElementOf(Type clrType)
    {
        var element = clrType.IsArray ? clrType.GetElementType()
            : clrType.IsGenericType && clrType.GetGenericTypeDefinition() == typeof(List<>) ? clrType.GetGenericArguments()[0]
            : null;
        return element is not null && ListElements.Contains(Nullable.GetUnderlyingType(element) ?? element) ? element : null;
    }

    private static readonly HashSet<Type> ListElements =
    [
        typeof(bool), typeof(sbyte), typeof(short), typeof(int), typeof(long),
        typeof(byte), typeof(ushort), typeof(uint), typeof(ulong),
        typeof(float), typeof(double), typeof(decimal), typeof(string),
        typeof(DateTime), typeof(DateTimeOffset), typeof(DateOnly), typeof(TimeOnly), typeof(Guid)
    ];

    public Type MapFromProviderType(string providerType)
    {
        var baseType = providerType.Split('(')[0].Trim().ToUpperInvariant();

        return baseType switch
        {
            "INTEGER" or "INT" or "INT4" => typeof(int),
            "BIGINT" or "INT8" => typeof(long),
            "SMALLINT" or "INT2" => typeof(short),
            "TINYINT" or "INT1" => typeof(sbyte),
            "UTINYINT" => typeof(byte),
            "USMALLINT" => typeof(ushort),
            "UINTEGER" => typeof(uint),
            "UBIGINT" => typeof(ulong),
            "HUGEINT" => typeof(Int128),
            "DATE" => typeof(DateOnly),
            "TIME" => typeof(TimeOnly),
            "TIMESTAMPTZ" or "TIMESTAMP WITH TIME ZONE" => typeof(DateTimeOffset),
            "BOOLEAN" or "BOOL" or "LOGICAL" => typeof(bool),
            "FLOAT" or "FLOAT4" or "REAL" => typeof(float),
            "DOUBLE" or "FLOAT8" => typeof(double),
            "DECIMAL" or "NUMERIC" => typeof(decimal),
            "TIMESTAMP" or "DATETIME" => typeof(DateTime),
            "UUID" => typeof(Guid),
            "BLOB" or "BINARY" or "VARBINARY" => typeof(byte[]),
            _ => typeof(string)
        };
    }

    public string BuildNativeType(string dataType, int? dataLength, int? precision, int? scale, int? charLength)
    {
        // Fallback as acceptable per Tâche 2.2 step 3
        return dataType;
    }
}
