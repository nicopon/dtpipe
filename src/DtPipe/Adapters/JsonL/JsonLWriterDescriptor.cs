using DtPipe.Core.Abstractions;

namespace DtPipe.Adapters.JsonL;

public class JsonLWriterDescriptor : IProviderDescriptor<IDataWriter>
{
	public string ProviderName => JsonLConstants.ProviderName;

	public Type OptionsType => typeof(JsonLWriterOptions);

	public bool RequiresQuery => false;

	public bool CanHandle(string connectionString)
	{
		if (string.IsNullOrWhiteSpace(connectionString)) return false;

		return connectionString.Equals("jsonl", StringComparison.OrdinalIgnoreCase) ||
			   connectionString.StartsWith("jsonl:", StringComparison.OrdinalIgnoreCase) ||
			   connectionString.EndsWith(".jsonl", StringComparison.OrdinalIgnoreCase);
	}

	public IDataWriter Create(string connectionString, object options, IServiceProvider serviceProvider)
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

		return new JsonLDataWriter(path, (JsonLWriterOptions)options);
	}
}
