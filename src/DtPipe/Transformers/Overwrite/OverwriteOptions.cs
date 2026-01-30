using System.ComponentModel;
using DtPipe.Core.Options;
using DtPipe.Core.Attributes;

namespace DtPipe.Transformers.Overwrite;

public class OverwriteOptions : ITransformerOptions
{
    public static string Prefix => "overwrite";
    public static string DisplayName => "Static Overwrite Transformer";

    [CliOption(Description = "Column:value mapping to overwrite with static value (repeatable)")]
    public IEnumerable<string> Overwrite { get; set; } = Array.Empty<string>();

    [CliOption(Description = "Skip overwrite when source value is null")]
    public bool SkipNull { get; set; } = false;
}
