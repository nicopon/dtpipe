using DtPipe.Core.Abstractions;

namespace DtPipe.Adapters.SqlServer;

public class SqlServerReaderDescriptor : IProviderDescriptor<IStreamReader>
{
	public string ProviderName => SqlServerConstants.ProviderName;

	public Type OptionsType => typeof(SqlServerReaderOptions);

	public bool RequiresQuery => true;

	public bool CanHandle(string connectionString)
	{
		return SqlServerConnectionHelper.CanHandle(connectionString);
	}

	public IStreamReader Create(string connectionString, object options, IServiceProvider serviceProvider)
	{
		return new SqlServerStreamReader(
			SqlServerConnectionHelper.GetConnectionString(connectionString),
			((SqlServerReaderOptions)options).Query!, // Query is set by CliStreamReaderFactory
			(SqlServerReaderOptions)options,
			0);    // Timeout will be set similarly
	}
}
