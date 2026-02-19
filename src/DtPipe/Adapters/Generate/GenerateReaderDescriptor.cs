using DtPipe.Core.Abstractions;

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

	public IStreamReader Create(string connectionString, object options, IServiceProvider serviceProvider)
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
		foreach (var part in parts)
		{
			if (long.TryParse(part, out long count))
			{
				sampleOptions.RowCount = count;
			}
			else if (part.StartsWith("rate=", StringComparison.OrdinalIgnoreCase))
			{
				if (int.TryParse(part.Substring(5), out int rate))
				{
					sampleOptions.RowsPerSecond = rate;
				}
			}
		}

		return new GenerateReader(
			connectionString,
			"", // Query will be set via Reader instance or JobService before execution
			sampleOptions);
	}
}
