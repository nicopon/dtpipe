using System.ComponentModel;
using DtPipe.Adapters.Common;
using DtPipe.Core.Attributes;
using DtPipe.Core.Options;

namespace DtPipe.Adapters.Csv;

[Description("Reads data from a CSV file.")]
[ComponentHelp(
    usageNotes: "Connection string is a file path ending in '.csv' (or the 'csv:' prefix; '-'/bare 'csv' for stdin). In YAML, use 'provider-options' -> 'csv' to set the separator, header presence, encoding, or explicit column types.",
    examples: new[] {
        "main:\n  input: \"data.csv\"\n  provider-options:\n    csv:\n      separator: \";\"\n      has-header: true\n  output: \"<adapter-prefix>:<target>\""
    })]
public class CsvReaderOptions : TextSourceOptions, IOptionSet
{
    public static string Prefix => "csv";
    public static string DisplayName => "CSV Reader";

    [Description("CSV field separator")]
    public string Separator { get; set; } = ",";

    [Description("Whether the CSV file has a header row")]
    public bool HasHeader { get; set; } = true;
}
