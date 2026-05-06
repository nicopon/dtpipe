using DtPipe.Adapters.Common;
using DtPipe.Core.Options;

namespace DtPipe.Adapters.PostgreSQL;

public class PostgreSqlReaderOptions : QueryableReaderOptions, IProviderOptions
{
	public static string Prefix => PostgreSqlConstants.ProviderName;
	public static string DisplayName => "PostgreSQL Reader";
}
