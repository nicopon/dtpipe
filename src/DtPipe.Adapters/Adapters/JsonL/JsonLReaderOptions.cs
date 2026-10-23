using System.ComponentModel;
using DtPipe.Adapters.Common;
using DtPipe.Core.Options;
using DtPipe.Core.Attributes;

namespace DtPipe.Adapters.JsonL;

[Description("Reads data from a JSONL (newline-delimited JSON) file.")]
[ComponentHelp(
    usageNotes: "Connection string is a file path ending in '.jsonl' (or the 'jsonl:' prefix; '-' for stdin). By default each line is one JSON record; set '--path' (a dot-path, e.g. 'items.data') to instead stream records from a nested array inside a single JSON document, with nested objects preserved as struct columns.",
    examples: new[] {
        "main:\n  input: \"events.jsonl\"\n  provider-options:\n    jsonl:\n      path: \"items.data\"\n  output: \"<adapter-prefix>:<target>\""
    })]
public class JsonLReaderOptions : NavigableSourceOptions, IOptionSet, IHasSchemaOverride
{
    public static string Prefix => JsonLConstants.ProviderName;
    public static string DisplayName => "JsonL Reader";

    [Description("JSONL file path (use '-' for stdin)")]
    public string Jsonl { get; set; } = "";

    /// <summary>Full Arrow schema JSON. Set by --export-job; consumed by --job. Not a CLI flag.</summary>
    public string Schema { get; set; } = "";
}
