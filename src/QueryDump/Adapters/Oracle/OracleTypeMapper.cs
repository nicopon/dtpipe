using System;
using Oracle.ManagedDataAccess.Client;

namespace QueryDump.Adapters.Oracle;

internal static class OracleTypeMapper
{
    public static OracleDbType GetOracleDbType(Type type)
    {
        type = Nullable.GetUnderlyingType(type) ?? type;

        if (type == typeof(int) || type == typeof(short) || type == typeof(byte))
            return OracleDbType.Int32;
        if (type == typeof(long))
            return OracleDbType.Int64;
        if (type == typeof(decimal))
            return OracleDbType.Decimal;
        if (type == typeof(bool))
            return OracleDbType.Int16;
        if (type == typeof(float))
            return OracleDbType.Single;
        if (type == typeof(double))
            return OracleDbType.Double;
        if (type == typeof(DateTime))
            return OracleDbType.Date;
        if (type == typeof(DateTimeOffset))
            return OracleDbType.TimeStampTZ;
        if (type == typeof(Guid))
            return OracleDbType.Raw;
        if (type == typeof(byte[]))
            return OracleDbType.Blob;

        return OracleDbType.Varchar2;
    }

    public static string MapToProviderType(Type type)
    {
        type = Nullable.GetUnderlyingType(type) ?? type;

        if (type == typeof(int)) return "NUMBER(10)";
        if (type == typeof(long)) return "NUMBER(19)";
        if (type == typeof(short)) return "NUMBER(5)";
        if (type == typeof(byte)) return "NUMBER(3)";
        if (type == typeof(bool)) return "NUMBER(1)"; 
        if (type == typeof(float)) return "FLOAT"; 
        if (type == typeof(double)) return "FLOAT"; 
        if (type == typeof(decimal)) return "NUMBER(38, 4)"; 
        if (type == typeof(DateTime)) return "TIMESTAMP";
        if (type == typeof(DateTimeOffset)) return "TIMESTAMP WITH TIME ZONE";
        if (type == typeof(Guid)) return "RAW(16)";
        if (type == typeof(byte[])) return "BLOB";
        
        return "VARCHAR2(4000)";
    }
}
