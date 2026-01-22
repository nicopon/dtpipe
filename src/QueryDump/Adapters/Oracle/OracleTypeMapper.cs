namespace QueryDump.Adapters.Oracle;

/// <summary>
/// Maps CLR types to Oracle SQL types.
/// </summary>
public static class OracleTypeMapper
{
    public static string MapClrType(Type clrType)
    {
        var underlying = Nullable.GetUnderlyingType(clrType) ?? clrType;

        return underlying switch
        {
            _ when underlying == typeof(int) => "NUMBER(10)",
            _ when underlying == typeof(long) => "NUMBER(19)",
            _ when underlying == typeof(short) => "NUMBER(5)",
            _ when underlying == typeof(byte) => "NUMBER(3)",
            _ when underlying == typeof(bool) => "NUMBER(1)",
            _ when underlying == typeof(float) => "BINARY_FLOAT",
            _ when underlying == typeof(double) => "BINARY_DOUBLE",
            _ when underlying == typeof(decimal) => "NUMBER(38,10)",
            _ when underlying == typeof(DateTime) => "TIMESTAMP",
            _ when underlying == typeof(DateTimeOffset) => "TIMESTAMP WITH TIME ZONE",
            _ when underlying == typeof(Guid) => "RAW(16)",
            _ when underlying == typeof(byte[]) => "BLOB",
            _ => "VARCHAR2(4000)"
        };
    }
}
