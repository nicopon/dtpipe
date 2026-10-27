using System.ComponentModel;
using DtPipe.Adapters.Common;
using DtPipe.Core.Attributes;
using DtPipe.Core.Options;

namespace DtPipe.Adapters.Csv;

[Description("Reads data from a CSV file.")]
[ComponentHelp(
    usageNotes: "Connection string is a file path ending in '.csv' (or the 'csv:' prefix; '-'/bare 'csv' for stdin). In YAML, use 'provider-options' -> 'csv' to set the separator, header presence, encoding, or explicit column types. When '--auto-column-types' is set, column types are inferred from the first 100 rows ('--max-sample'); '--max-sample 0' disables inference and leaves columns as strings.",
    examples: new[] {
        "main:\n  input: \"data.csv\"\n  provider-options:\n    csv:\n      separator: \";\"\n      has-header: true\n  output: \"<adapter-prefix>:<target>\""
    })]
public class CsvReaderOptions : TextSourceOptions, IOptionSet, IHasColumnTypeHints
{
    public static string Prefix => "csv";
    public static string DisplayName => "CSV Reader";

    /// <summary>Rows sampled to infer column types when '--max-sample' is not given.</summary>
    public const int DefaultMaxSample = 100;

    public CsvReaderOptions()
    {
        MaxSample = DefaultMaxSample;
    }

    [Description("CSV field separator")]
    public string Separator { get; set; } = ",";

    [Description("Whether the CSV file has a header row")]
    public bool HasHeader { get; set; } = true;
}
