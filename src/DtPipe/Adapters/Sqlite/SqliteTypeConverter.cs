using DtPipe.Core.Abstractions;

namespace DtPipe.Adapters.Sqlite;

/// <summary>
/// SQLite-specific type mapper implementing CLR ↔ SQLite type conversions.
/// </summary>
public class SqliteTypeConverter : ITypeMapper
{
	public static readonly SqliteTypeConverter Instance = new();

	public string MapToProviderType(Type clrType)
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

	public Type MapFromProviderType(string providerType)
	{
		var upperType = providerType.Split('(')[0].Trim().ToUpperInvariant();

		if (upperType.Contains("INT")) return typeof(long);
		if (upperType.Contains("CHAR") || upperType.Contains("TEXT") || upperType.Contains("CLOB")) return typeof(string);
		if (upperType.Contains("BLOB")) return typeof(byte[]);
		if (upperType.Contains("REAL") || upperType.Contains("FLOA") || upperType.Contains("DOUB")) return typeof(double);
		if (upperType.Contains("BOOL")) return typeof(bool);
		if (upperType.Contains("DATE") || upperType.Contains("TIME")) return typeof(DateTime);
		if (upperType.Contains("DECIMAL") || upperType.Contains("NUMERIC")) return typeof(decimal);

		return typeof(string);
	}

	public string BuildNativeType(string dataType, int? dataLength, int? precision, int? scale, int? charLength)
	{
		// Fallback as acceptable per Tâche 2.2 step 3
		return dataType;
	}
}
