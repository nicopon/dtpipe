using DtPipe.Core.Abstractions;
using DtPipe.Core.Options;
using DtPipe.Transformers.Services;
using DtPipe.Core.Pipelines;

namespace DtPipe.Transformers.Row.Expand;

public class ExpandDataTransformerFactory : IDataTransformerFactory
{
	private readonly OptionsRegistry _registry;
	private readonly IJsEngineProvider _jsEngineProvider;
	public string Category => "Transformers";
	public Type OptionsType => typeof(DtPipe.Transformers.Row.Expand.ExpandOptions);

	public ExpandDataTransformerFactory(OptionsRegistry registry, IJsEngineProvider jsEngineProvider)
	{
		_registry = registry;
		_jsEngineProvider = jsEngineProvider;
	}

	public string ComponentName => "expand";

	public bool CanHandle(string connectionString) => false;

	public IDataTransformer? CreateFromOptions(object options) =>
		options is ExpandOptions o ? CreateFromOptions(o) : null;

	public IDataTransformer CreateFromOptions(DtPipe.Transformers.Row.Expand.ExpandOptions options)
	{
		// Resolve @file references in Expand entries
		var resolved = options.Expand?.Select(e =>
		{
			if (!e.StartsWith("@")) return e;
			var path = e[1..];
			return File.Exists(path) ? File.ReadAllText(path) : e;
		}).ToArray();
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
				if (value.StartsWith("@"))
				{
					var filePath = value.Substring(1);
					if (File.Exists(filePath))
					{
						expands.Add(File.ReadAllText(filePath));
					}
					else
					{
						expands.Add(value);
					}
				}
				else
				{
					expands.Add(value);
				}
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
