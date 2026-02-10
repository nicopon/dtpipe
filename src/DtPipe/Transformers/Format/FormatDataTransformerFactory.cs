using System.CommandLine;
using System.CommandLine.Parsing;
using System.Linq;
using DtPipe.Cli;
using DtPipe.Cli.Abstractions;
using DtPipe.Configuration;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Core.Options;

namespace DtPipe.Transformers.Format;

public interface IFormatDataTransformerFactory : IDataTransformerFactory { }

public class FormatDataTransformerFactory(OptionsRegistry registry) : IDataTransformerFactory
{
	private readonly OptionsRegistry _registry = registry;

	public static IEnumerable<Type> GetSupportedOptionTypes()
	{
		yield return ComponentOptionsHelper.GetOptionsType<FormatDataTransformer>();
	}

	public string Category => "Transformer Options";

	public string TransformerType => FormatOptions.Prefix; // "format"

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
		var formatOptions = _registry.Get<FormatOptions>();

		if (!formatOptions.Format.Any())
		{
			return null;
		}

		return new FormatDataTransformer(formatOptions);
	}

	public IDataTransformer CreateFromConfiguration(IEnumerable<(string Option, string Value)> configuration)
	{
		// Get config options (like SkipNull) from registry-bound options
		var registryOptions = _registry.Get<FormatOptions>();

		var options = new FormatOptions
		{
			Format = configuration.Select(x => x.Value),
			SkipNull = registryOptions.SkipNull
		};
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
