using DtPipe.Core.Abstractions;

namespace DtPipe.Adapters.SqlServer;

/// <summary>
/// SQL Server-specific type mapper implementing CLR ↔ SQL Server type conversions.
/// </summary>
public class SqlServerTypeConverter : ITypeMapper
{
    public static readonly SqlServerTypeConverter Instance = new();

    public string MapToProviderType(Type clrType)
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

    public Type MapFromProviderType(string providerType)
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

    	public string BuildNativeType(string dataType, int? dataLength, int? precision, int? scale, int? charLength)
	{
		var typeLower = dataType.ToLowerInvariant();
		var fullType = dataType;
		var length = charLength ?? dataLength;

		// Handle char/varchar/binary/varbinary
		if (length.HasValue && (typeLower.Contains("char") || typeLower.Contains("binary")))
		{
			var lenStr = length.Value == -1 ? "MAX" : length.Value.ToString();
			return $"{fullType}({lenStr})";
		}

		// Handle decimal/numeric
		if (precision.HasValue && scale.HasValue && (typeLower == "decimal" || typeLower == "numeric"))
		{
			return $"{fullType}({precision.Value},{scale.Value})";
		}

		// Handle datetime types with precision
		if (precision.HasValue && (typeLower.Contains("datetime2") || typeLower.Contains("datetimeoffset") || typeLower.Contains("time")))
		{
			return $"{fullType}({precision.Value})";
		}

		return fullType;
	}
}
