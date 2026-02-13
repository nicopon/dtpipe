using DtPipe.Core.Abstractions;

namespace DtPipe.Adapters.PostgreSQL;

public class PostgreSqlTypeConverter : ITypeMapper
{
	public static readonly PostgreSqlTypeConverter Instance = new();

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

	public string BuildNativeType(string dataType, int? dataLength, int? precision, int? scale, int? charLength)
	{
		// Simplified: return the type as-is for adapters that don't need detailed native type building
		// Fallback as acceptable per Tâche 2.2 step 3
		return dataType;
	}

	public NpgsqlTypes.NpgsqlDbType MapToNpgsqlDbType(Type clrType)
	{
		var type = Nullable.GetUnderlyingType(clrType) ?? clrType;

		return type switch
		{
			Type t when t == typeof(string) => NpgsqlTypes.NpgsqlDbType.Text,
			Type t when t == typeof(char) => NpgsqlTypes.NpgsqlDbType.Char,
			Type t when t == typeof(bool) => NpgsqlTypes.NpgsqlDbType.Boolean,
			Type t when t == typeof(byte) => NpgsqlTypes.NpgsqlDbType.Smallint,
			Type t when t == typeof(sbyte) => NpgsqlTypes.NpgsqlDbType.Smallint,
			Type t when t == typeof(short) => NpgsqlTypes.NpgsqlDbType.Smallint,
			Type t when t == typeof(ushort) => NpgsqlTypes.NpgsqlDbType.Integer,
			Type t when t == typeof(int) => NpgsqlTypes.NpgsqlDbType.Integer,
			Type t when t == typeof(uint) => NpgsqlTypes.NpgsqlDbType.Bigint,
			Type t when t == typeof(long) => NpgsqlTypes.NpgsqlDbType.Bigint,
			Type t when t == typeof(ulong) => NpgsqlTypes.NpgsqlDbType.Numeric,
			Type t when t == typeof(float) => NpgsqlTypes.NpgsqlDbType.Real,
			Type t when t == typeof(double) => NpgsqlTypes.NpgsqlDbType.Double,
			Type t when t == typeof(decimal) => NpgsqlTypes.NpgsqlDbType.Numeric,
			Type t when t == typeof(DateTime) => NpgsqlTypes.NpgsqlDbType.Timestamp,
			Type t when t == typeof(DateTimeOffset) => NpgsqlTypes.NpgsqlDbType.TimestampTz,
			Type t when t == typeof(TimeSpan) => NpgsqlTypes.NpgsqlDbType.Interval,
			Type t when t == typeof(Guid) => NpgsqlTypes.NpgsqlDbType.Uuid,
			Type t when t == typeof(byte[]) => NpgsqlTypes.NpgsqlDbType.Bytea,
			_ => NpgsqlTypes.NpgsqlDbType.Text
		};
	}
}
