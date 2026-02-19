using DtPipe.Core.Abstractions;

namespace DtPipe.Adapters.DuckDB;

public class DuckDbReaderDescriptor : IProviderDescriptor<IStreamReader>
{
	public string ProviderName => DuckDbConstants.ProviderName;

	public Type OptionsType => typeof(DuckDbReaderOptions);

	public bool RequiresQuery => true;

	public bool CanHandle(string connectionString)
	{
		return DuckDbConnectionHelper.CanHandle(connectionString);
	}

	public IStreamReader Create(string connectionString, object options, IServiceProvider serviceProvider)
	{
		var finalConnectionString = connectionString;

		if (!finalConnectionString.Contains("DataSource=", StringComparison.OrdinalIgnoreCase)
			&& !finalConnectionString.StartsWith("Data Source=", StringComparison.OrdinalIgnoreCase))
		{
			finalConnectionString = $"Data Source={finalConnectionString}";
		}

		var duckOptions = (DuckDbReaderOptions)options;
		return new DuckDataSourceReader(
			finalConnectionString,
			duckOptions.Query!, // Query is now populated by factory if available
			duckOptions,
			0);    // Timeout will be set similarly
	}
}
