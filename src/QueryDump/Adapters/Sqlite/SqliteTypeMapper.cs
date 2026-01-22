namespace QueryDump.Adapters.Sqlite;

/// <summary>
/// Maps CLR types to SQLite SQL types.
/// </summary>
public static class SqliteTypeMapper
{
    public static string MapClrType(Type clrType)
    {
        var underlying = Nullable.GetUnderlyingType(clrType) ?? clrType;

        return underlying switch
        {
            _ when underlying == typeof(int) => "INTEGER",
            _ when underlying == typeof(long) => "INTEGER",
            _ when underlying == typeof(short) => "INTEGER",
            _ when underlying == typeof(byte) => "INTEGER",
            _ when underlying == typeof(bool) => "INTEGER",
            _ when underlying == typeof(float) => "REAL",
            _ when underlying == typeof(double) => "REAL",
            _ when underlying == typeof(decimal) => "REAL",
            _ when underlying == typeof(DateTime) => "TEXT",
            _ when underlying == typeof(DateTimeOffset) => "TEXT",
            _ when underlying == typeof(byte[]) => "BLOB",
            _ when underlying == typeof(Guid) => "TEXT",
            _ => "TEXT"
        };
    }
}
