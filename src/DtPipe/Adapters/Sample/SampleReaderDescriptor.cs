using DtPipe.Configuration;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Core.Options;

namespace DtPipe.Adapters.Sample;

public class SampleReaderDescriptor : IProviderDescriptor<IStreamReader>
{
	public string ProviderName => SampleConstants.ProviderName;

	public Type OptionsType => typeof(SampleReaderOptions);

	public bool RequiresQuery => false;

	public bool CanHandle(string connectionString)
	{
		return connectionString.StartsWith("sample:", StringComparison.OrdinalIgnoreCase);
	}

	public IStreamReader Create(string connectionString, object options, DumpOptions context, IServiceProvider serviceProvider)
	{
		var sampleOptions = (SampleReaderOptions)options;

		string config = connectionString;
		if (config.StartsWith("sample:", StringComparison.OrdinalIgnoreCase))
		{
			config = config.Substring(7);
		}

		var parts = config.Split(';', StringSplitOptions.RemoveEmptyEntries);

		if (parts.Length > 0 && long.TryParse(parts[0], out long count))
		{
			sampleOptions.RowCount = count;
		}

		return new SampleReader(
			connectionString,
			context.Query ?? "",
			sampleOptions);
	}
}
