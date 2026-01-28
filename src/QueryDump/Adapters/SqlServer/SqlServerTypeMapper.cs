using System;

namespace QueryDump.Adapters.SqlServer;

internal static class SqlServerTypeMapper
{
    public static string MapToProviderType(Type clrType)
    {
        var type = Nullable.GetUnderlyingType(clrType) ?? clrType;
        if (type == typeof(int)) return "INT";
        if (type == typeof(long)) return "BIGINT";
        if (type == typeof(short)) return "SMALLINT";
        if (type == typeof(byte)) return "TINYINT";
        if (type == typeof(string)) return "NVARCHAR(MAX)";
        if (type == typeof(DateTime)) return "DATETIME2";
        if (type == typeof(bool)) return "BIT";
        if (type == typeof(double)) return "FLOAT";
        if (type == typeof(decimal)) return "DECIMAL(18,2)";
        if (type == typeof(Guid)) return "UNIQUEIDENTIFIER";
        if (type == typeof(byte[])) return "VARBINARY(MAX)";
        return "NVARCHAR(MAX)";
    }

    public static Type MapFromProviderType(string providerType)
    {
        var baseType = providerType.Split('(')[0].Trim().ToLowerInvariant();

        return baseType switch
        {
            "int" => typeof(int),
            "bigint" => typeof(long),
            "smallint" => typeof(short),
            "tinyint" => typeof(byte),
            "bit" => typeof(bool),
            "decimal" or "numeric" or "money" => typeof(decimal),
            "float" => typeof(double),
            "real" => typeof(float),
            "datetime" or "datetime2" or "date" => typeof(DateTime),
            "uniqueidentifier" => typeof(Guid),
            "nvarchar" or "varchar" or "text" or "ntext" or "char" or "nchar" => typeof(string),
            "binary" or "varbinary" or "image" => typeof(byte[]),
            _ => typeof(string)
        };
    }
}
