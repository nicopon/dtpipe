using System.CommandLine;
using System.CommandLine.Parsing;
using DtPipe.Cli;
using DtPipe.Cli.Abstractions;
using DtPipe.Configuration;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Core.Options;

namespace DtPipe.Transformers.Mask;

public class MaskDataTransformerFactory(OptionsRegistry registry) : IDataTransformerFactory
{
	private readonly OptionsRegistry _registry = registry;

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

	public IDataTransformer? Create(DumpOptions options)
	{
		var maskOptions = _registry.Get<MaskOptions>();

		// Return null if no mappings, to skip execution overhead
		if (!maskOptions.Mask.Any())
		{
			return null;
		}

		return new MaskDataTransformer(maskOptions);
	}

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
