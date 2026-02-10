using DtPipe.Configuration;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Core.Options;

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

	public IDataWriter Create(string connectionString, object options, DumpOptions context, IServiceProvider serviceProvider)
	{
		return new CsvDataWriter(connectionString, (CsvWriterOptions)options);
	}
}
