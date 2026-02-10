using System.CommandLine;
using System.CommandLine.Parsing;
using System.Linq;
using DtPipe.Cli;
using DtPipe.Cli.Abstractions;
using DtPipe.Configuration;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Core.Options;

namespace DtPipe.Transformers.Null;

public interface INullDataTransformerFactory : IDataTransformerFactory { }

public class NullDataTransformerFactory(OptionsRegistry registry) : IDataTransformerFactory
{
	private readonly OptionsRegistry _registry = registry;

	public static IEnumerable<Type> GetSupportedOptionTypes()
	{
		yield return ComponentOptionsHelper.GetOptionsType<NullDataTransformer>();
	}

	public string Category => "Transformer Options";

	public string TransformerType => NullOptions.Prefix; // "null"

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
		var nullOptions = _registry.Get<NullOptions>();

		// Return null if no columns to nullify, to skip execution overhead
		if (!nullOptions.Columns.Any())
		{
			return null;
		}

		return new NullDataTransformer(nullOptions);
	}

	public IDataTransformer CreateFromConfiguration(IEnumerable<(string Option, string Value)> configuration)
	{
		var options = new NullOptions
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

		var options = new NullOptions { Columns = [.. config.Mappings.Keys] };
		return new NullDataTransformer(options);
	}
}
