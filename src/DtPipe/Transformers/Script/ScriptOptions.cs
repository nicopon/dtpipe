using DtPipe.Core.Options;
using DtPipe.Cli.Attributes;

namespace DtPipe.Transformers.Script;

public record ScriptOptions : ITransformerOptions
{
    public static string Prefix => "script";
    public static string DisplayName => "Javascript Scripting";
    
    [CliOption(Description = "Column:script mapping (e.g. TITLE:value.substring(0,5))")]
    public IReadOnlyList<string> Script { get; init; } = [];

    [CliOption(Description = "Skip script execution when input value is null")]
    public bool SkipNull { get; init; } = false;
}
