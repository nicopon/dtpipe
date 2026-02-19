using DtPipe.Core.Abstractions;

namespace DtPipe.Adapters.Sqlite;

public class SqliteReaderDescriptor : IProviderDescriptor<IStreamReader>
{
	public string ProviderName => "sqlite";

	public Type OptionsType => typeof(SqliteReaderOptions);

	public bool RequiresQuery => true;

	public bool CanHandle(string connectionString)
	{
		return SqliteConnectionHelper.CanHandle(connectionString);
	}

	public IStreamReader Create(string connectionString, object options, IServiceProvider serviceProvider)
	{
		var connStr = SqliteConnectionHelper.ToDataSourceConnectionString(connectionString);
		var readerOptions = (SqliteReaderOptions)options;

		return new SqliteStreamReader(
			connStr,
			readerOptions.Query!, // Query is set by CliStreamReaderFactory
			0);    // Timeout will be set similarly
	}
}
