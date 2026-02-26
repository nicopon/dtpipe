using DtPipe.Core.Abstractions;
using DtPipe.Core.Options;

using DtPipe.Core.Pipelines;

namespace DtPipe.Transformers.Row.Null;

public interface INullDataTransformerFactory : IDataTransformerFactory { }

public class NullDataTransformerFactory : INullDataTransformerFactory
{
	private readonly OptionsRegistry _registry;

	public NullDataTransformerFactory(OptionsRegistry registry)
	{
		_registry = registry;
	}

	public string ComponentName => "null";

	public bool CanHandle(string connectionString) => false;

	public string Category => "Transformers";
	public Type OptionsType => typeof(DtPipe.Transformers.Row.Null.NullOptions);

	public IDataTransformer CreateFromOptions(DtPipe.Transformers.Row.Null.NullOptions options)
	{
		return new NullDataTransformer(options);
	}

	public IDataTransformer CreateFromConfiguration(IEnumerable<(string Option, string Value)> configuration)
	{
		var options = new DtPipe.Transformers.Row.Null.NullOptions
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

		var options = new DtPipe.Transformers.Row.Null.NullOptions { Columns = [.. config.Mappings.Keys] };
		return new NullDataTransformer(options);
	}
}
