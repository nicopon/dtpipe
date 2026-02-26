using DtPipe.Core.Abstractions;
using DtPipe.Core.Options;
using DtPipe.Transformers.Services;
using DtPipe.Core.Pipelines;

namespace DtPipe.Transformers.Row.Filter;

public class FilterDataTransformerFactory : IDataTransformerFactory
{
	private readonly OptionsRegistry _registry;
	private readonly IJsEngineProvider _jsEngineProvider;
	public string Category => "Transformers";
	public Type OptionsType => typeof(DtPipe.Transformers.Row.Filter.FilterTransformerOptions);

	public FilterDataTransformerFactory(OptionsRegistry registry, IJsEngineProvider jsEngineProvider)
	{
		_registry = registry;
		_jsEngineProvider = jsEngineProvider;
	}

	public string ComponentName => "filter";

	public bool CanHandle(string connectionString) => false;

	public IDataTransformer CreateFromOptions(DtPipe.Transformers.Row.Filter.FilterTransformerOptions options)
	{
		return new FilterDataTransformer(options, _jsEngineProvider);
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
				filters.Add(value);
			}
		}

		var options = new DtPipe.Transformers.Row.Filter.FilterTransformerOptions { Filters = filters.ToArray() };
		return new FilterDataTransformer(options, _jsEngineProvider);
	}

	public IDataTransformer? CreateFromYamlConfig(TransformerConfig config)
	{
		var filters = new List<string>();

		if (config.Mappings != null)
		{
			// In Filter, Mappings might be used as "COL: expression" or just expressions in key/value
			// But usually it's healthier to use config.Mappings if it's a dict
			foreach (var kvp in config.Mappings)
			{
				filters.Add($"{kvp.Key}:{kvp.Value}");
			}
		}

		if (config.Options != null && config.Options.TryGetValue("filter", out var f))
		{
			filters.Add(f);
		}

		if (filters.Count == 0) return null;

		return new FilterDataTransformer(new DtPipe.Transformers.Row.Filter.FilterTransformerOptions { Filters = filters.ToArray() }, _jsEngineProvider);
	}
}
