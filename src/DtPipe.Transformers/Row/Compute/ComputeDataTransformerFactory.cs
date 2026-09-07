using DtPipe.Core.Abstractions;
using DtPipe.Core.Options;
using DtPipe.Core.Expressions;
using DtPipe.Core.Security;
using DtPipe.Transformers.Services;
using DtPipe.Core.Pipelines;

using DtPipe.Transformers.Abstract;

namespace DtPipe.Transformers.Row.Compute;

public class ComputeDataTransformerFactory : TransformerFactoryBase<ComputeOptions>
{

	public override string ComponentName => "compute";
	private readonly OptionsRegistry _registry;
	private readonly IJsEngineProvider _jsEngineProvider;
	private readonly IStringContentResolver _resolver;

	public ComputeDataTransformerFactory(OptionsRegistry registry, IJsEngineProvider jsEngineProvider, IStringContentResolver? resolver = null)
	{
		_registry = registry;
		_jsEngineProvider = jsEngineProvider;
		_resolver = resolver ?? DefaultStringContentResolver.Instance;
	}



	public override string Category => "Transformers";

	public override IDataTransformer CreateFromConfiguration(IEnumerable<(string Option, string Value)> configuration)
	{
		var mappings = new List<string>();
		bool skipNull = false;
		foreach (var (option, value) in configuration)
		{
			if (option == "compute" || option == "--compute" || option == "script" || option == "--script")
			{
				var parts = value.Split(':', 2);
				if (parts.Length == 2)
				{
					mappings.Add($"{parts[0]}:{ResolveScriptContent(parts[1])}");
				}
				else
				{
					mappings.Add(value);
				}
			}
			else if (option == "compute-skip-null" || option == "--compute-skip-null" || option == "script-skip-null" || option == "--script-skip-null")
			{
				if (bool.TryParse(value, out var b)) skipNull = b;
			}
		}

		return new ComputeDataTransformer(new DtPipe.Transformers.Row.Compute.ComputeOptions
		{
			Compute = mappings.ToArray(),
			SkipNull = skipNull
		}, _jsEngineProvider);
	}

	protected override IDataTransformer? CreateFromTypedOptions(ComputeOptions options)
	{
		// Resolve @file references in Compute entries (single resolution point for both CLI and YAML paths)
		var resolved = options.Compute.Select(c =>
		{
			var sep = c.IndexOf(':');
			return sep > 0
				? c[..sep] + ":" + ResolveScriptContent(c[(sep + 1)..])
				: ResolveScriptContent(c);
		}).ToList();
		return new ComputeDataTransformer(options with { Compute = resolved }, _jsEngineProvider);
	}

	public override object? CreateOptionsFromYaml(TransformerConfig config)
	{
		// Raw values; @file resolution happens in CreateFromTypedOptions.
		var mappings = (config.Mappings ?? [])
			.Select(kvp => string.IsNullOrEmpty(kvp.Value) ? kvp.Key : $"{kvp.Key}:{kvp.Value}")
			.ToList();

		return mappings.Count == 0 ? null : new ComputeOptions { Compute = mappings };
	}

	private string ResolveScriptContent(string script)
	{
		if (string.IsNullOrWhiteSpace(script)) return script;
		return _resolver.ResolveAsync(script).GetAwaiter().GetResult() ?? script;
	}
}
