using DtPipe.Adapters.Common;
using DtPipe.Core.Options;

namespace DtPipe.Adapters.SqlServer;

public class SqlServerReaderOptions : QueryableReaderOptions, IProviderOptions
{
	public static string Prefix => SqlServerConstants.ProviderName;
	public static string DisplayName => "SQL Server Reader";
}
