using DtPipe.Core.Abstractions;

namespace DtPipe.Adapters.Parquet;

public class ParquetWriterDescriptor : IProviderDescriptor<IDataWriter>
{
	public string ProviderName => ParquetConstants.ProviderName;

	public Type OptionsType => typeof(ParquetWriterOptions);

	public bool RequiresQuery => false;

	public bool CanHandle(string connectionString)
	{
		return connectionString.EndsWith(".parquet", StringComparison.OrdinalIgnoreCase);
	}

	public IDataWriter Create(string connectionString, object options, IServiceProvider serviceProvider)
	{
		return new ParquetDataWriter(connectionString);
	}
}
