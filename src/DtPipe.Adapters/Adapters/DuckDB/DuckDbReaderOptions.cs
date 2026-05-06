using DtPipe.Adapters.Common;
using DtPipe.Core.Options;

namespace DtPipe.Adapters.DuckDB;

public class DuckDbReaderOptions : QueryableReaderOptions, IProviderOptions
{
	public static string Prefix => DuckDbConstants.ProviderName;
	public static string DisplayName => "DuckDB Reader";
}
