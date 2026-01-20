namespace QueryDump.Providers.DuckDB;

/// <summary>
/// Maps CLR types to DuckDB SQL types.
/// </summary>
public static class DuckDbTypeMapper
{
    public static string MapClrType(Type clrType)
    {
        var underlying = Nullable.GetUnderlyingType(clrType) ?? clrType;

        return underlying switch
        {
            _ when underlying == typeof(int) => "INTEGER",
            _ when underlying == typeof(long) => "BIGINT",
            _ when underlying == typeof(short) => "SMALLINT",
            _ when underlying == typeof(byte) => "TINYINT",
            _ when underlying == typeof(bool) => "BOOLEAN",
            _ when underlying == typeof(float) => "FLOAT",
            _ when underlying == typeof(double) => "DOUBLE",
            _ when underlying == typeof(decimal) => "DECIMAL",
            _ when underlying == typeof(DateTime) => "TIMESTAMP",
            _ when underlying == typeof(DateTimeOffset) => "TIMESTAMP",
            _ when underlying == typeof(Guid) => "UUID",
            _ when underlying == typeof(byte[]) => "BLOB",
            _ => "VARCHAR"
        };
    }
}
