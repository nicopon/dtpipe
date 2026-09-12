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
		return new ExpandDataTransformer(
			new ExpandOptions { Expand = resolved, ExpandTypes = options.ExpandTypes },
			_jsEngineProvider);
	}

	public override IDataTransformer CreateFromConfiguration(IEnumerable<(string Option, string Value)> configuration)
	{
		var expands = new List<string>();
		var declared = NewDeclarationMap();

		foreach (var (option, value) in configuration)
		{
			var name = option.TrimStart('-');
			if (string.Equals(name, "expand", StringComparison.OrdinalIgnoreCase))
				expands.Add(_resolver.ResolveAsync(value).GetAwaiter().GetResult() ?? value);
			else if (string.Equals(name, "expand-types", StringComparison.OrdinalIgnoreCase))
				AddDeclarations(declared, value);
		}

		if (expands.Count == 0) return new ExpandDataTransformer(new ExpandOptions(), _jsEngineProvider);

		return new ExpandDataTransformer(
			new ExpandOptions { Expand = expands.ToArray(), ExpandTypes = declared },
			_jsEngineProvider);
	}

	public override object? CreateOptionsFromYaml(TransformerConfig config)
	{
		// BuildTransformerConfigsFromCli splits the expression on its first ':' into a key:value
		// mapping. Rejoin it to recover the original expression.
		var expands = (config.Mappings ?? [])
			.Select(kvp => string.IsNullOrEmpty(kvp.Value) ? kvp.Key : $"{kvp.Key}:{kvp.Value}")
			.ToArray();

		if (expands.Length == 0) return null;

		var declared = NewDeclarationMap();
		if (config.Options is not null
			&& config.Options.TryGetValue("expand-types", out var value))
			AddDeclarations(declared, value);

		return new ExpandOptions { Expand = expands, ExpandTypes = declared };
	}

	private static Dictionary<string, string> NewDeclarationMap() => new(StringComparer.OrdinalIgnoreCase);

	/// <summary>
	/// Reads <c>name:type</c> declarations, comma-separated so one YAML key can carry several —
	/// a mapping key cannot repeat, while the command-line flag can.
	/// </summary>
	private static void AddDeclarations(Dictionary<string, string> into, string value)
	{
		foreach (var entry in value.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
		{
			var sep = entry.IndexOf(':');
			if (sep > 0) into[entry[..sep].Trim()] = entry[(sep + 1)..].Trim();
			else into[entry] = string.Empty;
		}
	}
}
