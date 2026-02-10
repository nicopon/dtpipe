using System.CommandLine;
using DtPipe.Cli;
using DtPipe.Configuration;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Options;
using DtPipe.Core.Services;

namespace DtPipe.Transformers.Filter;

public class FilterDataTransformerFactory : IDataTransformerFactory
{
	private readonly OptionsRegistry _registry;
	private readonly IJsEngineProvider _jsEngineProvider;

	public FilterDataTransformerFactory(OptionsRegistry registry, IJsEngineProvider jsEngineProvider)
	{
		_registry = registry;
		_jsEngineProvider = jsEngineProvider;
	}

	public string Category => "Transformer Options";
	public string TransformerType => "filter";

	public static IEnumerable<Type> GetSupportedOptionTypes()
	{
		yield return ComponentOptionsHelper.GetOptionsType<FilterDataTransformer>();
	}

	public IEnumerable<Option> GetCliOptions()
	{
		var list = new List<Option>();
		foreach (var type in GetSupportedOptionTypes())
		{
			var (options, _) = CliOptionBuilder.GenerateOptionsWithMetadataForType(type);
			list.AddRange(options);
		}
		return list;
	}

	public void BindOptions(ParseResult parseResult, OptionsRegistry registry)
	{
		var allOptions = GetCliOptions();
		foreach (var type in GetSupportedOptionTypes())
		{
			var boundOptions = CliOptionBuilder.BindForType(type, parseResult, allOptions);
			registry.RegisterByType(type, boundOptions);
		}
	}

	public IDataTransformer? Create(DumpOptions options)
	{
		var filterOptions = _registry.Get<FilterTransformerOptions>();

		if (filterOptions.Filters == null || filterOptions.Filters.Length == 0)
		{
			return null;
		}

		return new FilterDataTransformer(filterOptions, _jsEngineProvider);
	}

	public IDataTransformer CreateFromConfiguration(IEnumerable<(string Option, string Value)> configuration)
	{
		var filters = new List<string>();

		foreach (var (option, value) in configuration)
		{
			if (string.Equals(option, "filter", StringComparison.OrdinalIgnoreCase) ||
				string.Equals(option, "--filter", StringComparison.OrdinalIgnoreCase) ||
				string.Equals(option, "where", StringComparison.OrdinalIgnoreCase) ||
				string.Equals(option, "--where", StringComparison.OrdinalIgnoreCase))
			{
				if (value.StartsWith("@"))
				{
					var filePath = value.Substring(1);
					if (File.Exists(filePath))
					{
						filters.Add(File.ReadAllText(filePath));
					}
					else
					{
						// Keep as is if not found, or throw? The logic in Compute uses "if exists read else string".
						// Logic: ResolveScriptContent(value)
						filters.Add(value); // Simplified for now, should resolve file content
					}
				}
				else
				{
					filters.Add(value);
				}
			}
		}

		if (filters.Count == 0) return new FilterDataTransformer(new FilterTransformerOptions(), _jsEngineProvider);

		return new FilterDataTransformer(new FilterTransformerOptions { Filters = filters.ToArray() }, _jsEngineProvider);
	}

	public IDataTransformer? CreateFromYamlConfig(TransformerConfig config)
	{
		// Support 'filter' or 'where'
		var filters = new List<string>();

		// Check script property for main filter expressions
		if (config.Script != null)
		{
			filters.AddRange(config.Script.Values);
		}

		if (filters.Count == 0) return null;

		return new FilterDataTransformer(new FilterTransformerOptions { Filters = filters.ToArray() }, _jsEngineProvider);
	}

	public Task<int?> HandleCommandAsync(ParseResult parseResult, CancellationToken ct = default)
	{
		return Task.FromResult<int?>(null);
	}
}
