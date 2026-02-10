using DtPipe.Configuration;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Core.Options;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace DtPipe.Adapters.Parquet;

public class ParquetReaderDescriptor : IProviderDescriptor<IStreamReader>
{


	public string ProviderName => "parquet";

	public Type OptionsType => typeof(EmptyOptions);

	public bool RequiresQuery => false;

	public bool CanHandle(string connectionString)
	{
		if (string.IsNullOrWhiteSpace(connectionString)) return false;

		return connectionString.EndsWith(".parquet", StringComparison.OrdinalIgnoreCase);
	}

	public IStreamReader Create(string connectionString, object options, DumpOptions context, IServiceProvider serviceProvider)
	{
		var filePath = connectionString;
		var logger = serviceProvider.GetService<ILogger<ParquetStreamReader>>();

		return new ParquetStreamReader(filePath, logger);
	}
}
