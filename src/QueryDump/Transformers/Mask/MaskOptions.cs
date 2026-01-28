using QueryDump.Core.Options;
using QueryDump.Core.Attributes;

namespace QueryDump.Transformers.Mask;

public class MaskOptions : ITransformerOptions
{
    public static string Prefix => "mask";
    public static string DisplayName => "Mask Transformer";

    [CliOption(Description = "Mask column: COLUMN:pattern (# = keep, other = replace)")]
    public IEnumerable<string> Mask { get; set; } = [];

    [CliOption(Description = "Skip mask when source value is null")]
    public bool SkipNull { get; set; } = false;
}
