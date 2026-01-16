using System.ComponentModel;
using QueryDump.Core.Options;
using QueryDump.Core.Attributes;

namespace QueryDump.Transformers.Clone;

public class CloneOptions : ITransformerOptions
{
    public static string Prefix => "clone";
    public static string DisplayName => "Clone/Template Transformer";

    [CliOption("--clone", Description = "Target:Template mapping (repeatable, e.g. 'Notes:{{Title}}')")]
    public IEnumerable<string> Mappings { get; set; } = Array.Empty<string>();
}
