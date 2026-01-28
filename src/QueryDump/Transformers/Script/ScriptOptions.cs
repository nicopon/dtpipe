using System.ComponentModel;
using QueryDump.Core.Options;
using QueryDump.Core.Attributes;

namespace QueryDump.Transformers.Script;

public record ScriptOptions : ITransformerOptions
{
    public static string Prefix => "script";
    public static string DisplayName => "Javascript Scripting";
    
    [CliOption(Description = "Column:script mapping (e.g. TITLE:value.substring(0,5))")]
    public IReadOnlyList<string> Script { get; init; } = [];

    [CliOption(Description = "Skip script execution when input value is null")]
    public bool SkipNull { get; init; } = false;
}
