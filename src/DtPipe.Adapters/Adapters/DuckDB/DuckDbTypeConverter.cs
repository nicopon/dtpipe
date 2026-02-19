using DtPipe.Core.Abstractions;

namespace DtPipe.Adapters.DuckDB;

/// <summary>
/// DuckDB-specific type mapper implementing CLR ↔ DuckDB type conversions.
/// </summary>
public class DuckDbTypeConverter : ITypeMapper
{
	public static readonly DuckDbTypeConverter Instance = new();

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
