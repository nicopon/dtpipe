using DtPipe.Core.Abstractions;
using DtPipe.Core.Options;
using DtPipe.Core.Pipelines;

namespace DtPipe.Transformers.Arrow.Overwrite;

public class OverwriteDataTransformerFactory : IDataTransformerFactory
{
	private readonly OptionsRegistry _registry;

	public OverwriteDataTransformerFactory(OptionsRegistry registry)
	{
		_registry = registry;
	}

	public string ComponentName => "overwrite";

	public bool CanHandle(string connectionString) => false;

	public string Category => "Transformers";
	public Type OptionsType => typeof(DtPipe.Transformers.Arrow.Overwrite.OverwriteOptions);


	public IDataTransformer CreateFromConfiguration(IEnumerable<(string Option, string Value)> configuration)
	{
		// Get config options (like SkipNull) from registry-bound options
		var registryOptions = _registry.Get<OverwriteOptions>();

		var options = new DtPipe.Transformers.Arrow.Overwrite.OverwriteOptions
		{
			Overwrite = configuration.Select(x => x.Value),
			SkipNull = registryOptions.SkipNull
		};
		return new OverwriteDataTransformer(options);
	}

	public IDataTransformer? CreateFromYamlConfig(TransformerConfig config)
	{
		if (config.Mappings == null || config.Mappings.Count == 0)
			return null;

		// Convert YAML dict to "COLUMN:value" or "COLUMN=value" format
		// If value is empty, just return key (which might already contain the separator like "Col=Val")
		var mappings = config.Mappings.Select(kvp => string.IsNullOrEmpty(kvp.Value) ? kvp.Key : $"{kvp.Key}:{kvp.Value}");

		var skipNull = false;
		if (config.Options != null && config.Options.TryGetValue("skip-null", out var snStr))
		{
			bool.TryParse(snStr, out skipNull);
		}

		var options = new DtPipe.Transformers.Arrow.Overwrite.OverwriteOptions { Overwrite = mappings, SkipNull = skipNull };
		return new OverwriteDataTransformer(options);
	}
}
