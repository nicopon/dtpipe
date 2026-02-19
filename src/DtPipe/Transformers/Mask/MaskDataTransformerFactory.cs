using System.CommandLine;
using DtPipe.Cli;
using DtPipe.Cli.Abstractions;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Options;

using DtPipe.Core.Pipelines;

namespace DtPipe.Transformers.Mask;

public interface IMaskDataTransformerFactory : IDataTransformerFactory, ICliContributor { }

public class MaskDataTransformerFactory(OptionsRegistry registry) : IMaskDataTransformerFactory
{
	private readonly OptionsRegistry _registry = registry;

	public string ProviderName => TransformerType;

	public bool CanHandle(string connectionString) => false;

	public static IEnumerable<Type> GetSupportedOptionTypes()
	{
		yield return ComponentOptionsHelper.GetOptionsType<MaskDataTransformer>();
	}

	public string Category => "Transformer Options";

	public string TransformerType => MaskOptions.Prefix; // "mask"

	private IEnumerable<Option>? _cliOptions;

	public IEnumerable<Option> GetCliOptions()
	{
		return _cliOptions ??= [.. GetSupportedOptionTypes().SelectMany(CliOptionBuilder.GenerateOptionsForType)];
	}

	public void BindOptions(ParseResult parseResult, OptionsRegistry registry)
	{
		var options = GetCliOptions();

		foreach (var type in GetSupportedOptionTypes())
		{
			var boundOptions = CliOptionBuilder.BindForType(type, parseResult, options);
			registry.RegisterByType(type, boundOptions);
		}
	}

	// Create(DumpOptions) removed


	public IDataTransformer CreateFromConfiguration(IEnumerable<(string Option, string Value)> configuration)
	{
		// Get config options (like SkipNull) from registry-bound options
		var registryOptions = _registry.Get<MaskOptions>();

		var options = new MaskOptions
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
