namespace DtPipe.Adapters.DuckDB;

/// <summary>
/// Maps CLR types to DuckDB SQL types.
/// </summary>
public static class DuckDbTypeMapper
{
	public static string MapToProviderType(Type clrType)
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

	public static Type MapFromProviderType(string providerType)
	{
		var baseType = providerType.Split('(')[0].Trim().ToUpperInvariant();

		return baseType switch
		{
			"INTEGER" or "INT" or "INT4" => typeof(int),
			"BIGINT" or "INT8" => typeof(long),
			"SMALLINT" or "INT2" => typeof(short),
			"TINYINT" or "INT1" => typeof(byte),
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
}
