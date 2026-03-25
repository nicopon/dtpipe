using DtPipe.Core.Abstractions;
using DtPipe.Core.Options;

using DtPipe.Core.Pipelines;

namespace DtPipe.Transformers.Columnar.Null;

public class NullDataTransformerFactory : IDataTransformerFactory
{
	private readonly OptionsRegistry _registry;

	public NullDataTransformerFactory(OptionsRegistry registry)
	{
		_registry = registry;
	}

	public string ComponentName => "null";

	public bool CanHandle(string connectionString) => false;

	public string Category => "Transformers";
	public Type OptionsType => typeof(DtPipe.Transformers.Columnar.Null.NullOptions);

	public IDataTransformer CreateFromOptions(DtPipe.Transformers.Columnar.Null.NullOptions options)
	{
		return new NullDataTransformer(options);
	}

	public IDataTransformer CreateFromConfiguration(IEnumerable<(string Option, string Value)> configuration)
	{
		var options = new DtPipe.Transformers.Columnar.Null.NullOptions
		{
			Columns = [.. configuration.Select(x => x.Value)]
		};
		return new NullDataTransformer(options);
	}

	public IDataTransformer? CreateFromYamlConfig(TransformerConfig config)
	{
		// For null transformer, Mappings keys are the column names (values are ignored)
		if (config.Mappings == null || config.Mappings.Count == 0)
			return null;

		var options = new DtPipe.Transformers.Columnar.Null.NullOptions { Columns = [.. config.Mappings.Keys] };
		return new NullDataTransformer(options);
	}
}
