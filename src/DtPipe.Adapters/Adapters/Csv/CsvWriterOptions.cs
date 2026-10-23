using System.ComponentModel;
using DtPipe.Core.Attributes;
using DtPipe.Core.Options;

namespace DtPipe.Adapters.Csv;

[Description("Writes data to a CSV file.")]
[ComponentHelp(
    usageNotes: "Connection string is a file path ending in '.csv' (or the 'csv:' prefix; '-'/bare 'csv' for stdout). In YAML, use 'provider-options' -> 'csv-writer' (suffixed, since the reader shares the same 'csv' prefix) to set the separator, quote character, null representation, or date/timestamp formats.",
    examples: new[] {
        "main:\n  input: \"<adapter-prefix>:<source>\"\n  provider-options:\n    csv-writer:\n      separator: \";\"\n      quote: \"'\"\n      null-value: \"NULL\"\n  output: \"data.csv\""
    })]
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
