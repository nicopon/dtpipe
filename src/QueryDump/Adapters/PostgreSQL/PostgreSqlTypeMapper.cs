using System;
using QueryDump.Core.Abstractions;
using QueryDump.Cli.Abstractions;
using QueryDump.Core.Models;

namespace QueryDump.Adapters.PostgreSQL;

public class PostgreSqlTypeMapper : ITypeMapper
{
    public static readonly PostgreSqlTypeMapper Instance = new();

    public string MapToProviderType(Type clrType)
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

    public Type MapFromProviderType(string providerType)
    {
        var baseType = providerType.Split('(')[0].Trim().ToLowerInvariant();

        return baseType switch
        {
            "boolean" or "bool" => typeof(bool),
            "smallint" or "int2" => typeof(short),
            "integer" or "int" or "int4" => typeof(int),
            "bigint" or "int8" => typeof(long),
            "real" or "float4" => typeof(float),
            "double precision" or "float8" => typeof(double),
            "numeric" or "decimal" => typeof(decimal),
            "timestamp" or "timestamp without time zone" => typeof(DateTime),
            "timestamptz" or "timestamp with time zone" => typeof(DateTimeOffset),
            "interval" => typeof(TimeSpan),
            "uuid" => typeof(Guid),
            "bytea" => typeof(byte[]),
            "text" or "varchar" or "character varying" or "char" or "character" => typeof(string),
            _ => typeof(string)
        };
    }
}
