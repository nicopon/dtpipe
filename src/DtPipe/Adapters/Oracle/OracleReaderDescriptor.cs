using DtPipe.Core.Abstractions;

namespace DtPipe.Adapters.Oracle;

public class OracleReaderDescriptor : IProviderDescriptor<IStreamReader>
{
	public string ProviderName => OracleConstants.ProviderName;

	public Type OptionsType => typeof(OracleReaderOptions);

	public bool RequiresQuery => true;

	public bool CanHandle(string connectionString)
	{
		return OracleConnectionHelper.CanHandle(connectionString);
	}

	public IStreamReader Create(string connectionString, object options, IServiceProvider serviceProvider)
	{
		var readerOptions = (OracleReaderOptions)options;
		return new OracleStreamReader(
			OracleConnectionHelper.GetConnectionString(connectionString),
			readerOptions.Query!, // Query is set by CliStreamReaderFactory
			readerOptions,
			0      // Timeout will be set similarly
		);
	}
}
