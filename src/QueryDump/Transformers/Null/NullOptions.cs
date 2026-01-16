using System.ComponentModel;
using QueryDump.Core.Options;
using QueryDump.Core.Attributes;

namespace QueryDump.Transformers.Null;

public class NullOptions : ITransformerOptions
{
    public static string Prefix => "null";
    public static string DisplayName => "Null Transformer";

    [CliOption("--null", Description = "Column(s) to set to null (repeatable)")]
    public IEnumerable<string> Columns { get; set; } = Array.Empty<string>();
}
