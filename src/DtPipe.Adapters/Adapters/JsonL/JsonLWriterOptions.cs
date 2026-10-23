using System.ComponentModel;
using DtPipe.Core.Attributes;
using DtPipe.Core.Options;

namespace DtPipe.Adapters.JsonL;

[Description("Writes data to a JSONL (newline-delimited JSON) file.")]
[ComponentHelp(
    usageNotes: "Connection string is a file path ending in '.jsonl' (or the 'jsonl:' prefix; '-' for stdout). In YAML, use 'provider-options' -> 'jsonl-writer' to change the encoding; leave indentation off (the default), since pretty-printed JSON breaks the one-record-per-line contract.",
    examples: new[] {
        "main:\n  input: \"<adapter-prefix>:<source>\"\n  output: \"events.jsonl\""
    })]
public class JsonLWriterOptions : IOptionSet
{
    public static string Prefix => JsonLConstants.ProviderName;
    public static string DisplayName => "JsonL Writer";

    [Description("JSONL file path (use '-' for stdout)")]
    public string Jsonl { get; set; } = "";

    [Description("File encoding (e.g., UTF-8)")]
    public string Encoding { get; set; } = "UTF-8";

    [Description("Whether to indent the JSON output (not recommended for JsonL)")]
    public bool Indented { get; set; } = false;
}
