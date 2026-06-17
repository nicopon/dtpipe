using DtPipe.Core.Abstractions;
using DtPipe.Core.Options;
using DtPipe.Core.Expressions;
using DtPipe.Core.Security;
using DtPipe.Transformers.Services;
using DtPipe.Core.Pipelines;

namespace DtPipe.Transformers.Row.Expand;

public class ExpandDataTransformerFactory : IDataTransformerFactory
{
	private readonly OptionsRegistry _registry;
	private readonly IJsEngineProvider _jsEngineProvider;
	private readonly IStringContentResolver _resolver;

	public ExpandDataTransformerFactory(OptionsRegistry registry, IJsEngineProvider jsEngineProvider, IStringContentResolver? resolver = null)
	{
		_registry = registry;
		_jsEngineProvider = jsEngineProvider;
		_resolver = resolver ?? DefaultStringContentResolver.Instance;
	}

	public string Category => "Transformers";
	public Type OptionsType => typeof(DtPipe.Transformers.Row.Expand.ExpandOptions);

	public string ComponentName => "expand";

	public bool CanHandle(string connectionString) => false;

	public IDataTransformer? CreateFromOptions(object options) =>
		options is ExpandOptions o ? CreateFromOptions(o) : null;

	public IDataTransformer CreateFromOptions(DtPipe.Transformers.Row.Expand.ExpandOptions options)
	{
		var resolved = options.Expand?.Select(e =>
			_resolver.ResolveAsync(e).GetAwaiter().GetResult() ?? e
		).ToArray();
		return new ExpandDataTransformer(new ExpandOptions { Expand = resolved }, _jsEngineProvider);
	}

	public IDataTransformer CreateFromConfiguration(IEnumerable<(string Option, string Value)> configuration)
	{
		var expands = new List<string>();

		foreach (var (option, value) in configuration)
		{
			if (string.Equals(option, "expand", StringComparison.OrdinalIgnoreCase) ||
				string.Equals(option, "--expand", StringComparison.OrdinalIgnoreCase))
			{
				expands.Add(
					_resolver.ResolveAsync(value).GetAwaiter().GetResult() ?? value);
			}
		}

		if (expands.Count == 0) return new ExpandDataTransformer(new DtPipe.Transformers.Row.Expand.ExpandOptions(), _jsEngineProvider);

 		return new ExpandDataTransformer(new DtPipe.Transformers.Row.Expand.ExpandOptions { Expand = expands.ToArray() }, _jsEngineProvider);
	}

	public IDataTransformer? CreateFromYamlConfig(TransformerConfig config)
	{
		var expands = new List<string>();

		if (config.Mappings != null)
		{
			foreach (var kvp in config.Mappings)
			{
				// BuildTransformerConfigsFromCli splits the expression on the first ':' into a key:value mapping.
				// Reconstruct the original expression by joining with ':' (same pattern as FilterDataTransformerFactory).
				if (string.IsNullOrEmpty(kvp.Value))
					expands.Add(kvp.Key);
				else
					expands.Add($"{kvp.Key}:{kvp.Value}");
			}
		}

		if (expands.Count == 0) return null;

		return new ExpandDataTransformer(new DtPipe.Transformers.Row.Expand.ExpandOptions { Expand = expands.ToArray() }, _jsEngineProvider);
	}
}
