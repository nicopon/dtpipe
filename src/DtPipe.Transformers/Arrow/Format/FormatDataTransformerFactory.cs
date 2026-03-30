using DtPipe.Core.Abstractions;
using DtPipe.Core.Options;
using DtPipe.Core.Pipelines;

namespace DtPipe.Transformers.Arrow.Format;

public class FormatDataTransformerFactory(OptionsRegistry registry) : IDataTransformerFactory
{
	private readonly OptionsRegistry _registry = registry;

	public string ComponentName => "format";

	public bool CanHandle(string connectionString) => false;

	public string Category => "Transformers";
	public Type OptionsType => typeof(DtPipe.Transformers.Arrow.Format.FormatOptions);

	public IDataTransformer CreateFromOptions(DtPipe.Transformers.Arrow.Format.FormatOptions options)
	{
		return new FormatDataTransformer(options);
	}

	public IDataTransformer CreateFromConfiguration(IEnumerable<(string Option, string Value)> configuration)
	{
		// Get config options (like SkipNull) from registry-bound options
		var registryOptions = _registry.Get<DtPipe.Transformers.Arrow.Format.FormatOptions>();

		var options = new DtPipe.Transformers.Arrow.Format.FormatOptions
		{
			Format = configuration.Select(x => x.Value),
			SkipNull = registryOptions.SkipNull
		};
		var configStr = string.Join(" | ", configuration.Select(x => $"{x.Option}={x.Value}"));

		return new FormatDataTransformer(options);
	}

	public IDataTransformer? CreateFromYamlConfig(TransformerConfig config)
	{
		if (config.Mappings == null || config.Mappings.Count == 0)
			return null;

		// Convert YAML dict to "COLUMN:template" format
		var mappings = config.Mappings.Select(kvp => $"{kvp.Key}:{kvp.Value}");

		var skipNull = false;
		if (config.Options != null && config.Options.TryGetValue("skip-null", out var snStr))
		{
			bool.TryParse(snStr, out skipNull);
		}

		var options = new FormatOptions { Format = mappings, SkipNull = skipNull };
		return new FormatDataTransformer(options);
	}
}
