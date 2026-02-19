using DtPipe.Core.Abstractions;

namespace DtPipe.Adapters.Csv;

public class CsvWriterDescriptor : IProviderDescriptor<IDataWriter>
{
	public string ProviderName => CsvConstants.ProviderName;

	public Type OptionsType => typeof(CsvWriterOptions);

	public bool RequiresQuery => false;

	public bool CanHandle(string connectionString)
	{
		return connectionString.EndsWith(".csv", StringComparison.OrdinalIgnoreCase);
	}

	public IDataWriter Create(string connectionString, object options, IServiceProvider serviceProvider)
	{
		return new CsvDataWriter(connectionString, (CsvWriterOptions)options);
	}
}
