using System.CommandLine;
using DtPipe.Cli;
using DtPipe.Cli.Abstractions;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Options;
using DtPipe.Core.Pipelines;

namespace DtPipe.Transformers.Format;

public interface IFormatDataTransformerFactory : IDataTransformerFactory, ICliContributor { }

public class FormatDataTransformerFactory(OptionsRegistry registry) : IFormatDataTransformerFactory
{
	private readonly OptionsRegistry _registry = registry;

	public string ProviderName => TransformerType;

	public bool CanHandle(string connectionString) => false;

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

	// Create(DumpOptions) removed


	public IDataTransformer CreateFromConfiguration(IEnumerable<(string Option, string Value)> configuration)
	{
		// Get config options (like SkipNull) from registry-bound options
		var registryOptions = _registry.Get<FormatOptions>();

		var options = new FormatOptions
		{
			Format = configuration.Select(x => x.Value),
			SkipNull = registryOptions.SkipNull
		};
		// Debugging: verify config
		var configStr = string.Join(" | ", configuration.Select(x => $"{x.Option}={x.Value}"));
		// Console.WriteLine($"[FormatFactoryDebug] Config: {configStr}");

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
