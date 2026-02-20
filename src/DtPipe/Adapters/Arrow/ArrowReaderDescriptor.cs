using DtPipe.Core.Abstractions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace DtPipe.Adapters.Arrow;

public class ArrowReaderDescriptor : IProviderDescriptor<IStreamReader>
{
	public string ProviderName => ArrowConstants.ProviderName;

	public Type OptionsType => typeof(ArrowReaderOptions);

	public bool RequiresQuery => false;

	public bool CanHandle(string connectionString)
	{
		if (string.IsNullOrWhiteSpace(connectionString)) return false;

		return connectionString.Equals("arrow", StringComparison.OrdinalIgnoreCase) ||
			   connectionString.StartsWith("arrow:", StringComparison.OrdinalIgnoreCase) ||
			   connectionString.EndsWith(".arrow", StringComparison.OrdinalIgnoreCase) ||
			   connectionString.EndsWith(".arrowfile", StringComparison.OrdinalIgnoreCase);
	}

	public IStreamReader Create(string connectionString, object options, IServiceProvider serviceProvider)
	{
		var path = connectionString;
		if (path.StartsWith("arrow:", StringComparison.OrdinalIgnoreCase))
		{
			path = path.Substring(6);
		}
		else if (path.Equals("arrow", StringComparison.OrdinalIgnoreCase))
		{
			path = "-";
		}

		var logger = serviceProvider.GetService<ILogger<ArrowAdapterStreamReader>>();
		return new ArrowAdapterStreamReader(path, (ArrowReaderOptions)options, logger);
	}
}
