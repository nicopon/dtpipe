using DtPipe.Core.Options;
using DtPipe.Cli.Attributes;

namespace DtPipe.Transformers.Script;

public record ComputeOptions : ITransformerOptions
{
    public static string Prefix => "compute";
    public static string DisplayName => "Compute (JS)";
    
    [CliOption(Description = "Column:script mapping (e.g. TITLE:value.substring(0,5))", Aliases = new[] { "--script" })]
    public IReadOnlyList<string> Compute { get; init; } = [];

    [CliOption(Description = "Skip script execution when input value is null", Aliases = new[] { "--script-skip-null" })]
    public bool SkipNull { get; init; } = false;
}
