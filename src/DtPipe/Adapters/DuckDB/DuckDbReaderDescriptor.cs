using DtPipe.Configuration;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Core.Options;

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

	public IStreamReader Create(string connectionString, object options, DumpOptions context, IServiceProvider serviceProvider)
	{
		var finalConnectionString = connectionString;

		if (!finalConnectionString.Contains("DataSource=", StringComparison.OrdinalIgnoreCase)
			&& !finalConnectionString.StartsWith("Data Source=", StringComparison.OrdinalIgnoreCase))
		{
			finalConnectionString = $"Data Source={finalConnectionString}";
		}

		return new DuckDataSourceReader(
			finalConnectionString,
			context.Query!,
			(DuckDbReaderOptions)options,
			context.QueryTimeout);
	}
}
