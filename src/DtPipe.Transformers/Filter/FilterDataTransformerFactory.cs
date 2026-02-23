using DtPipe.Core.Abstractions;
using DtPipe.Core.Options;
using DtPipe.Transformers.Services;
using DtPipe.Core.Pipelines;

namespace DtPipe.Transformers.Filter;

public class FilterDataTransformerFactory : IDataTransformerFactory
{
	private readonly OptionsRegistry _registry;
	private readonly IJsEngineProvider _jsEngineProvider;
	public string Category => "Transformers";
	public Type OptionsType => typeof(FilterTransformerOptions);

	public FilterDataTransformerFactory(OptionsRegistry registry, IJsEngineProvider jsEngineProvider)
	{
		_registry = registry;
		_jsEngineProvider = jsEngineProvider;
	}

	public string ComponentName => "filter";

	public bool CanHandle(string connectionString) => false;

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
		if (config.Compute != null)
		{
			filters.AddRange(config.Compute.Values);
		}

		if (filters.Count == 0) return null;

		return new FilterDataTransformer(new FilterTransformerOptions { Filters = filters.ToArray() }, _jsEngineProvider);
	}
}
