using DtPipe.Core.Abstractions;
using DtPipe.Core.Options;
using DtPipe.Transformers.Services;
using DtPipe.Core.Pipelines;

using DtPipe.Transformers.Abstract;

namespace DtPipe.Transformers.Arrow.Filter;

public class FilterDataTransformerFactory : TransformerFactoryBase<FilterOptions>
{

	public override string ComponentName => "filter";
	private readonly OptionsRegistry _registry;
	private readonly IJsEngineProvider _jsEngineProvider;
	private readonly DtPipe.Core.Expressions.IStringContentResolver _resolver;

	public FilterDataTransformerFactory(OptionsRegistry registry, IJsEngineProvider jsEngineProvider, DtPipe.Core.Expressions.IStringContentResolver? resolver = null)
	{
		_registry = registry;
		_jsEngineProvider = jsEngineProvider;
		_resolver = resolver ?? DtPipe.Core.Expressions.DefaultStringContentResolver.Instance;
	}

	public override string Category => "Transformers";



	protected override IDataTransformer? CreateFromTypedOptions(FilterOptions options)
	{
		var resolved = options.Filters?.Select(f => 
			_resolver.ResolveAsync(f).GetAwaiter().GetResult() ?? f
		).ToArray();
		
		var newOptions = new FilterOptions { Filters = resolved };
		return new FilterDataTransformer(newOptions, _jsEngineProvider);
	}

	public override IDataTransformer CreateFromConfiguration(IEnumerable<(string Option, string Value)> configuration)
	{
		var filters = new List<string>();

		foreach (var (option, value) in configuration)
		{
			if (string.Equals(option, "filter", StringComparison.OrdinalIgnoreCase) ||
				string.Equals(option, "--filter", StringComparison.OrdinalIgnoreCase) ||
				string.Equals(option, "where", StringComparison.OrdinalIgnoreCase) ||
				string.Equals(option, "--where", StringComparison.OrdinalIgnoreCase))
			{
				filters.Add(_resolver.ResolveAsync(value).GetAwaiter().GetResult() ?? value);
			}
		}

		var options = new DtPipe.Transformers.Arrow.Filter.FilterOptions { Filters = filters.ToArray() };
		return new FilterDataTransformer(options, _jsEngineProvider);
	}

	public override object? CreateOptionsFromYaml(TransformerConfig config)
	{
		// A mapping is an expression, split on its first ':' by the CLI export — rejoin it.
		var filters = (config.Mappings ?? [])
			.Select(kvp => string.IsNullOrEmpty(kvp.Value) ? kvp.Key : $"{kvp.Key}:{kvp.Value}")
			.ToArray();

		return filters.Length == 0
			? null
			: new DtPipe.Transformers.Arrow.Filter.FilterOptions { Filters = filters };
	}
}
