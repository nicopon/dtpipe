using DtPipe.Configuration;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Core.Options;

namespace DtPipe.Adapters.Sqlite;

public class SqliteReaderDescriptor : IProviderDescriptor<IStreamReader>
{
	public string ProviderName => "sqlite";

	public Type OptionsType => typeof(EmptyOptions);

	public bool RequiresQuery => true;

	public bool CanHandle(string connectionString)
	{
		return SqliteConnectionHelper.CanHandle(connectionString);
	}

	public IStreamReader Create(string connectionString, object options, DumpOptions context, IServiceProvider serviceProvider)
	{
		var connStr = SqliteConnectionHelper.ToDataSourceConnectionString(connectionString);

		return new SqliteStreamReader(
			connStr,
			context.Query!,
			context.QueryTimeout);
	}
}
