using DtPipe.Core.Abstractions;

namespace DtPipe.Adapters.Arrow;

public class ArrowWriterDescriptor : IProviderDescriptor<IDataWriter>
{
	public string ProviderName => ArrowConstants.ProviderName;

	public Type OptionsType => typeof(ArrowWriterOptions);

	public bool RequiresQuery => false;

	public bool CanHandle(string connectionString)
	{
		if (string.IsNullOrWhiteSpace(connectionString)) return false;

		return connectionString.Equals("arrow", StringComparison.OrdinalIgnoreCase) ||
			   connectionString.StartsWith("arrow:", StringComparison.OrdinalIgnoreCase) ||
			   connectionString.EndsWith(".arrow", StringComparison.OrdinalIgnoreCase) ||
			   connectionString.EndsWith(".arrowfile", StringComparison.OrdinalIgnoreCase);
	}

	public IDataWriter Create(string connectionString, object options, IServiceProvider serviceProvider)
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

		return new ArrowAdapterDataWriter(path, (ArrowWriterOptions)options);
	}
}
