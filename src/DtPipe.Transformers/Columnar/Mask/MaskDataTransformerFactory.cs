using DtPipe.Core.Abstractions;
using DtPipe.Core.Options;

using DtPipe.Core.Pipelines;

namespace DtPipe.Transformers.Columnar.Mask;

public class MaskDataTransformerFactory(OptionsRegistry registry) : IDataTransformerFactory
{
	private readonly OptionsRegistry _registry = registry;

	public string ComponentName => "mask";

	public bool CanHandle(string connectionString) => false;

	public string Category => "Transformers";
	public Type OptionsType => typeof(DtPipe.Transformers.Columnar.Mask.MaskOptions);

	public IDataTransformer CreateFromOptions(DtPipe.Transformers.Columnar.Mask.MaskOptions options)
	{
		return new MaskDataTransformer(options);
	}

	public IDataTransformer CreateFromConfiguration(IEnumerable<(string Option, string Value)> configuration)
	{
		// Get config options (like SkipNull) from registry-bound options
		var registryOptions = _registry.Get<MaskOptions>();

		var options = new DtPipe.Transformers.Columnar.Mask.MaskOptions
		{
			Mask = [.. configuration.Select(x => x.Value)],
			SkipNull = registryOptions.SkipNull
		};
		return new MaskDataTransformer(options);
	}

	public IDataTransformer? CreateFromYamlConfig(TransformerConfig config)
	{
		// For mask transformer, Mappings are key=column, value=pattern
		if (config.Mappings == null || config.Mappings.Count == 0)
			return null;

		// For mask transformer, Mappings are key=column, value=pattern
		// If value is empty, use key only (implies default mask)
		var mappings = config.Mappings.Select(kvp => string.IsNullOrEmpty(kvp.Value) ? kvp.Key : $"{kvp.Key}:{kvp.Value}").ToList();

		var skipNull = false;
		if (config.Options != null && config.Options.TryGetValue("skip-null", out var snStr))
		{
			bool.TryParse(snStr, out skipNull);
		}

		var options = new MaskOptions { Mask = mappings, SkipNull = skipNull };
		return new MaskDataTransformer(options);
	}
}
