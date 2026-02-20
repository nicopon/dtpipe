using System.ComponentModel;
using DtPipe.Core.Options;

namespace DtPipe.Adapters.Csv;

public record CsvWriterOptions : IWriterOptions
{
	public static string Prefix => CsvConstants.ProviderName;
	public static string DisplayName => "CSV Writer";

	[Description("CSV field separator")]
	public string Separator { get; init; } = ",";

	[Description("Include header row in CSV")]
	public bool Header { get; init; } = true;

	[Description("CSV quote character")]
	public char Quote { get; init; } = '"';

	[Description("Date format for CSV (ISO 8601)")]
	public string DateFormat { get; init; } = "yyyy-MM-dd"; // ISO 8601

	[Description("Timestamp format for CSV")]
	public string TimestampFormat { get; init; } = "yyyy-MM-dd HH:mm:ss.ffffff"; // ISO 8601

	[Description("Decimal separator")]
	public string DecimalSeparator { get; init; } = "."; // DuckDB default

	[Description("String to use for null values")]
	public string? NullValue { get; init; } = null; // Empty string for null
}
