using System.ComponentModel;
using QueryDump.Core.Options;

namespace QueryDump.Providers.Csv;

public class CsvReaderOptions : IOptionSet
{
    public static string Prefix => "csv-reader";
    public static string DisplayName => "CSV Reader";

    [Description("CSV field delimiter character")]
    public string Delimiter { get; set; } = ",";

    [Description("Whether the CSV file has a header row")]
    public bool HasHeader { get; set; } = true;

    [Description("File encoding (e.g., UTF-8, ISO-8859-1)")]
    public string Encoding { get; set; } = "UTF-8";
}
