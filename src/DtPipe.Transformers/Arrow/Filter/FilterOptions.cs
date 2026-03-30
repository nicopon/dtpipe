using DtPipe.Core.Attributes;
using DtPipe.Core.Options;

namespace DtPipe.Transformers.Arrow.Filter;

public class FilterOptions : ITransformerOptions
{
	public static string Prefix => "filter";
	public static string DisplayName => "Filter Options";

	[ComponentOption("--filter", Description = "Filter expression(s). Multiple filters are applied sequentially.")]
	public string[]? Filters { get; set; }
}
