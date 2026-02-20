using DtPipe.Core.Options;

namespace DtPipe.Adapters.Arrow;

public class ArrowReaderOptions : IOptionSet
{
	public static string Prefix => ArrowConstants.ProviderName;
	public static string DisplayName => "Arrow Reader";
}
