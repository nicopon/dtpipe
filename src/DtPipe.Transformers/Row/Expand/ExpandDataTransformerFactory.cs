using DtPipe.Core.Abstractions;
using DtPipe.Core.Options;
using DtPipe.Core.Expressions;
using DtPipe.Core.Security;
using DtPipe.Transformers.Services;
using DtPipe.Core.Pipelines;

using DtPipe.Transformers.Abstract;

namespace DtPipe.Transformers.Row.Expand;

public class ExpandDataTransformerFactory : TransformerFactoryBase<ExpandOptions>
{

	public override string ComponentName => "expand";
	private readonly OptionsRegistry _registry;
	private readonly IJsEngineProvider _jsEngineProvider;
	private readonly IStringContentResolver _resolver;

	public ExpandDataTransformerFactory(OptionsRegistry registry, IJsEngineProvider jsEngineProvider, IStringContentResolver? resolver = null)
	{
		_registry = registry;
		_jsEngineProvider = jsEngineProvider;
		_resolver = resolver ?? DefaultStringContentResolver.Instance;
	}

	public override string Category => "Transformers";



	protected override IDataTransformer? CreateFromTypedOptions(ExpandOptions options)
	{
		var resolved = options.Expand?.Select(e =>
			_resolver.ResolveAsync(e).GetAwaiter().GetResult() ?? e
		).ToArray();
		return new ExpandDataTransformer(new ExpandOptions { Expand = resolved }, _jsEngineProvider);
	}

	public override IDataTransformer CreateFromConfiguration(IEnumerable<(string Option, string Value)> configuration)
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

	public override object? CreateOptionsFromYaml(TransformerConfig config)
	{
		// BuildTransformerConfigsFromCli splits the expression on its first ':' into a key:value
		// mapping. Rejoin it to recover the original expression.
		var expands = (config.Mappings ?? [])
			.Select(kvp => string.IsNullOrEmpty(kvp.Value) ? kvp.Key : $"{kvp.Key}:{kvp.Value}")
			.ToArray();

		return expands.Length == 0
			? null
			: new DtPipe.Transformers.Row.Expand.ExpandOptions { Expand = expands };
	}
}
