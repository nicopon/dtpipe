using System.ComponentModel;
using QueryDump.Core.Options;
using QueryDump.Core.Attributes;

namespace QueryDump.Transformers.Overwrite;

public class OverwriteOptions : ITransformerOptions
{
    public static string Prefix => "overwrite";
    public static string DisplayName => "Static Overwrite Transformer";

    [CliOption("--overwrite", Description = "Column:value mapping to overwrite with static value (repeatable)")]
    public IEnumerable<string> Mappings { get; set; } = Array.Empty<string>();
}
