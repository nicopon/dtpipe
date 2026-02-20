using DtPipe.Core.Abstractions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace DtPipe.Adapters.JsonL;

public class JsonLReaderDescriptor : IProviderDescriptor<IStreamReader>
{
	public string ProviderName => JsonLConstants.ProviderName;

	public Type OptionsType => typeof(JsonLReaderOptions);

	public bool RequiresQuery => false;

	public bool CanHandle(string connectionString)
	{
		if (string.IsNullOrWhiteSpace(connectionString)) return false;

		return connectionString.Equals("jsonl", StringComparison.OrdinalIgnoreCase) ||
			   connectionString.StartsWith("jsonl:", StringComparison.OrdinalIgnoreCase) ||
			   connectionString.EndsWith(".jsonl", StringComparison.OrdinalIgnoreCase);
	}

	public IStreamReader Create(string connectionString, object options, IServiceProvider serviceProvider)
	{
		var path = connectionString;
		if (path.StartsWith("jsonl:", StringComparison.OrdinalIgnoreCase))
		{
			path = path.Substring(6);
		}
		else if (path.Equals("jsonl", StringComparison.OrdinalIgnoreCase))
		{
			path = "-";
		}

		var logger = serviceProvider.GetService<ILogger<JsonLStreamReader>>();
		return new JsonLStreamReader(path, (JsonLReaderOptions)options, logger);
	}
}
