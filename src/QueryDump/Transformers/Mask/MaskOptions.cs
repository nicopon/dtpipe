using QueryDump.Core.Options;
using QueryDump.Core.Attributes;

namespace QueryDump.Transformers.Mask;

public class MaskOptions : ITransformerOptions
{
    public static string Prefix => "mask";
    public static string DisplayName => "Mask Transformer";

    [CliOption("--mask", Description = "Mask column: COLUMN:pattern (# = keep, other = replace)")]
    public IEnumerable<string> Mappings { get; set; } = [];

    [CliOption("--mask-skip-null", Description = "Skip mask when source value is null")]
    public bool SkipNull { get; set; } = false;
}
