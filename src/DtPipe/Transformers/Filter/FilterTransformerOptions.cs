using DtPipe.Cli.Attributes;
using DtPipe.Core.Options;

namespace DtPipe.Transformers.Filter;

public class FilterTransformerOptions : IOptionSet
{
	public static string Prefix => "filter";
	public static string DisplayName => "Filter Options";

	[CliOption("--filter", Description = "Filter expression(s). Multiple filters are applied sequentially.")]
	public string[]? Filters { get; set; }
}
