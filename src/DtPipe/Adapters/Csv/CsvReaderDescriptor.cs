using DtPipe.Core.Abstractions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace DtPipe.Adapters.Csv;

public class CsvReaderDescriptor : IProviderDescriptor<IStreamReader>
{


	public string ProviderName => "csv";

	public Type OptionsType => typeof(CsvReaderOptions);

	public bool RequiresQuery => false;

	public bool CanHandle(string connectionString)
	{
		if (string.IsNullOrWhiteSpace(connectionString)) return false;

		return connectionString.EndsWith(".csv", StringComparison.OrdinalIgnoreCase);
	}

	public IStreamReader Create(string connectionString, object options, IServiceProvider serviceProvider)
	{
		var filePath = connectionString;
		var logger = serviceProvider.GetService<ILogger<CsvStreamReader>>();

		return new CsvStreamReader(filePath, (CsvReaderOptions)options, logger);
	}
}
