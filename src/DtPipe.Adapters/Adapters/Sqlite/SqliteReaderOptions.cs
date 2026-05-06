using DtPipe.Adapters.Common;
using DtPipe.Core.Options;

namespace DtPipe.Adapters.Sqlite;

public class SqliteReaderOptions : QueryableReaderOptions, IProviderOptions
{
	public static string Prefix => "sqlite";
	public static string DisplayName => "SQLite Reader";
}
