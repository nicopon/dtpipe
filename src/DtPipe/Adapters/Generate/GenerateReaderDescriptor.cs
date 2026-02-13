using DtPipe.Configuration;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Core.Options;

namespace DtPipe.Adapters.Generate;

public class GenerateReaderDescriptor : IProviderDescriptor<IStreamReader>
{
	public string ProviderName => GenerateConstants.ProviderName;

	public Type OptionsType => typeof(GenerateReaderOptions);

	public bool RequiresQuery => false;

	public bool CanHandle(string connectionString)
	{
		return connectionString.StartsWith(GenerateConstants.ProviderName + ":", StringComparison.OrdinalIgnoreCase) ||
		       connectionString.StartsWith(GenerateConstants.LegacyProviderName + ":", StringComparison.OrdinalIgnoreCase);
	}

	public IStreamReader Create(string connectionString, object options, DumpOptions context, IServiceProvider serviceProvider)
	{
		var sampleOptions = (GenerateReaderOptions)options;

		string config = connectionString;
		if (config.StartsWith(GenerateConstants.ProviderName + ":", StringComparison.OrdinalIgnoreCase))
		{
			config = config.Substring(GenerateConstants.ProviderName.Length + 1);
		}
		else if (config.StartsWith(GenerateConstants.LegacyProviderName + ":", StringComparison.OrdinalIgnoreCase))
		{
			config = config.Substring(GenerateConstants.LegacyProviderName.Length + 1);
		}

		var parts = config.Split(';', StringSplitOptions.RemoveEmptyEntries);

		if (parts.Length > 0 && long.TryParse(parts[0], out long count))
		{
			sampleOptions.RowCount = count;
		}

		return new GenerateReader(
			connectionString,
			context.Query ?? "",
			sampleOptions);
	}
}
