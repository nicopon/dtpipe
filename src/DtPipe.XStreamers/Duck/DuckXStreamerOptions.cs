using System.ComponentModel;
using DtPipe.Core.Options;

namespace DtPipe.XStreamers.Duck;

public class DuckXStreamerOptions : IOptionSet, ICliOptionMetadata
{
    public static string Prefix => "duck-xstream";
    public static string DisplayName => "DuckDB XStreamer";

    [Description("SQL Query to execute. Use 'main' and 'ref[i]' aliases for branches.")]
    public string[] Query { get; set; } = Array.Empty<string>();

    [Description("Name of the main input branch.")]
    public string[] Main { get; set; } = Array.Empty<string>();

    [Description("Names of reference input branches (comma separated).")]
    public string[] Ref { get; set; } = Array.Empty<string>();

    public IReadOnlyDictionary<string, string> PropertyToFlag => new Dictionary<string, string>
    {
        { nameof(Query), "--query" },
        { nameof(Main), "--main" },
        { nameof(Ref), "--ref" }
    };
}
