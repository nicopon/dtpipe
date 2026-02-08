using DtPipe.Cli.Attributes;
using DtPipe.Core.Options;

namespace DtPipe.Transformers.Expand;

public class ExpandOptions : IOptionSet
{
    public static string Prefix => "expand";
    public static string DisplayName => "Expand Options";

    [CliOption("expand", Description = "A JavaScript expression that returns an array of rows. Each element becomes a new row.")]
    public string[]? Expand { get; set; }
}
