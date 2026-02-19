using DtPipe.Core.Abstractions;

namespace DtPipe.Adapters.PostgreSQL;

public class PostgreSqlReaderDescriptor : IProviderDescriptor<IStreamReader>
{
	public string ProviderName => PostgreSqlConstants.ProviderName;

	public Type OptionsType => typeof(PostgreSqlReaderOptions);

	public bool RequiresQuery => true;

	public bool CanHandle(string connectionString)
	{
		return PostgreSqlConnectionHelper.CanHandle(connectionString);
	}

	public IStreamReader Create(string connectionString, object options, IServiceProvider serviceProvider)
	{
		var readerOptions = (PostgreSqlReaderOptions)options;
		return new PostgreSqlReader(
			PostgreSqlConnectionHelper.GetConnectionString(connectionString),
			readerOptions.Query!, // Query is set by CliStreamReaderFactory
			0      // Timeout will be set similarly
		);
	}
}
