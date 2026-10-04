using DtPipe.Core.Abstractions;

namespace DtPipe.Adapters.MySql;

/// <summary>
/// MySQL-specific type mapper implementing CLR ↔ MySQL type conversions.
/// </summary>
public class MySqlTypeConverter : ITypeMapper
{
	public static readonly MySqlTypeConverter Instance = new();

	/// <summary>
	/// Length used for a string column that participates in a key. LONGTEXT cannot be indexed
	/// without a prefix length, so key columns get a VARCHAR instead — 255 utf8mb4 characters is
	/// 1020 bytes, comfortably inside InnoDB's 3072-byte index limit. Same trade-off SQL Server
	/// makes with NVARCHAR(450); see <see cref="MySqlDataWriter.GetCreateTableSql"/>.
	/// </summary>
	public const string KeyStringType = "VARCHAR(255)";

	public DecimalSpelling DecimalType { get; } = new("DECIMAL", 65, 30);

	public string MapToProviderType(Type clrType)
	{
		var type = Nullable.GetUnderlyingType(clrType) ?? clrType;

		return type switch
		{
			Type t when t == typeof(string) => "LONGTEXT",
			Type t when t == typeof(char) => "CHAR(1)",
			// TINYINT(1) is the shape MySqlConnector reads back as bool; a plain TINYINT would
			// round-trip as sbyte and quietly change the column's CLR type across a copy.
			Type t when t == typeof(bool) => "TINYINT(1)",
			Type t when t == typeof(byte) => "TINYINT UNSIGNED",
			Type t when t == typeof(sbyte) => "TINYINT",
			Type t when t == typeof(short) => "SMALLINT",
			Type t when t == typeof(ushort) => "SMALLINT UNSIGNED",
			Type t when t == typeof(int) => "INT",
			Type t when t == typeof(uint) => "INT UNSIGNED",
			Type t when t == typeof(long) => "BIGINT",
			Type t when t == typeof(ulong) => "BIGINT UNSIGNED",
			Type t when t == typeof(float) => "FLOAT",
			Type t when t == typeof(double) => "DOUBLE",
			// MySQL's maximum DECIMAL precision is 65 digits with at most 30 decimals. (38,9)
			// covers the CLR decimal range without truncating the scale the source carried.
			Type t when t == typeof(decimal) => "DECIMAL(38,9)",
			Type t when t == typeof(DateTime) => "DATETIME(6)",
			// Deliberately DATETIME, not TIMESTAMP: TIMESTAMP is limited to 1970-2038 and would
			// reject dates a source can legitimately hold. The zone offset is not preserved —
			// MySQL has no offset-carrying type — so this narrows DateTimeOffset to a local instant.
			Type t when t == typeof(DateTimeOffset) => "DATETIME(6)",
			Type t when t == typeof(DateOnly) => "DATE",
			Type t when t == typeof(TimeSpan) => "TIME(6)",
			Type t when t == typeof(TimeOnly) => "TIME(6)",
			// MySQL has no UUID type. CHAR(36) is the form MySqlConnector reads back as Guid
			// under its default GuidFormat, which keeps the round trip lossless.
			Type t when t == typeof(Guid) => "CHAR(36)",
			Type t when t == typeof(byte[]) => "LONGBLOB",
			_ => "LONGTEXT"
		};
	}

	public Type MapFromProviderType(string providerType)
	{
		var normalized = providerType.Trim().ToLowerInvariant();

		// Two widths carry meaning rather than capacity, and both must be matched before the
		// parenthesised part is discarded: TINYINT(1) is how MySQL spells a boolean, and CHAR(36)
		// is how it spells a UUID. Callers pass COLUMN_TYPE (not DATA_TYPE) so the width survives.
		if (normalized.StartsWith("tinyint(1)", StringComparison.Ordinal)) return typeof(bool);
		if (normalized.StartsWith("char(36)", StringComparison.Ordinal)) return typeof(Guid);

		var isUnsigned = normalized.Contains("unsigned", StringComparison.Ordinal);
		// The attributes trail the type name and may arrive without a width ("int unsigned"),
		// so stripping them has to happen independently of the parenthesised part.
		var baseType = normalized.Split('(')[0]
			.Replace("unsigned", string.Empty, StringComparison.Ordinal)
			.Replace("zerofill", string.Empty, StringComparison.Ordinal)
			.Trim();

		return baseType switch
		{
			"bool" or "boolean" => typeof(bool),
			"bit" => typeof(bool),
			"tinyint" => isUnsigned ? typeof(byte) : typeof(sbyte),
			"smallint" => isUnsigned ? typeof(ushort) : typeof(short),
			"mediumint" or "int" or "integer" => isUnsigned ? typeof(uint) : typeof(int),
			"bigint" => isUnsigned ? typeof(ulong) : typeof(long),
			"float" => typeof(float),
			"double" or "double precision" or "real" => typeof(double),
			"decimal" or "numeric" => typeof(decimal),
			"datetime" or "timestamp" => typeof(DateTime),
			"date" => typeof(DateTime),
			"time" => typeof(TimeSpan),
			"year" => typeof(short),
			"binary" or "varbinary" or "tinyblob" or "blob" or "mediumblob" or "longblob" => typeof(byte[]),
			_ => typeof(string)
		};
	}

	public string BuildNativeType(string dataType, int? dataLength, int? precision, int? scale, int? charLength)
	{
		// Introspection already hands back COLUMN_TYPE, which is the fully-rendered native type
		// ("varchar(255)", "decimal(38,9)", "int unsigned"). Re-deriving it from the parts would
		// only lose the unsigned/zerofill attributes that COLUMN_TYPE carries and DATA_TYPE drops.
		var typeLower = dataType.ToLowerInvariant();
		if (typeLower.Contains('(') || typeLower.Contains(' ')) return dataType;

		var length = charLength ?? dataLength;
		if (length is > 0 && (typeLower.Contains("char") || typeLower.Contains("binary")))
			return $"{dataType}({length.Value})";

		if (precision.HasValue && scale.HasValue && (typeLower == "decimal" || typeLower == "numeric"))
			return $"{dataType}({precision.Value},{scale.Value})";

		return dataType;
	}
}
