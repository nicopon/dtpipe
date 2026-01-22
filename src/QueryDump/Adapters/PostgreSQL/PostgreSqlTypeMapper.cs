using System;
using QueryDump.Core;

namespace QueryDump.Adapters.PostgreSQL;

public class PostgreSqlTypeMapper : ITypeMapper
{
    public static readonly PostgreSqlTypeMapper Instance = new();

    public string MapClrType(Type clrType)
    {
        var type = Nullable.GetUnderlyingType(clrType) ?? clrType;

        return type switch
        {
            Type t when t == typeof(string) => "TEXT",
            Type t when t == typeof(char) => "CHAR(1)",
            Type t when t == typeof(bool) => "BOOLEAN",
            Type t when t == typeof(byte) => "SMALLINT",
            Type t when t == typeof(sbyte) => "SMALLINT",
            Type t when t == typeof(short) => "SMALLINT",
            Type t when t == typeof(ushort) => "INTEGER",
            Type t when t == typeof(int) => "INTEGER",
            Type t when t == typeof(uint) => "BIGINT",
            Type t when t == typeof(long) => "BIGINT",
            Type t when t == typeof(ulong) => "NUMERIC(20,0)",
            Type t when t == typeof(float) => "REAL",
            Type t when t == typeof(double) => "DOUBLE PRECISION",
            Type t when t == typeof(decimal) => "NUMERIC",
            Type t when t == typeof(DateTime) => "TIMESTAMP",
            Type t when t == typeof(DateTimeOffset) => "TIMESTAMPTZ",
            Type t when t == typeof(TimeSpan) => "INTERVAL",
            Type t when t == typeof(Guid) => "UUID",
            Type t when t == typeof(byte[]) => "BYTEA",
            _ => "TEXT"
        };
    }
}
