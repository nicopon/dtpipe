using DtPipe.Core.Abstractions;
using DtPipe.Core.Options;
using DtPipe.Core.Security;
using DtPipe.Transformers.Services;
using DtPipe.Core.Pipelines;

namespace DtPipe.Transformers.Row.Compute;

public class ComputeDataTransformerFactory : IDataTransformerFactory
{
	private readonly OptionsRegistry _registry;
	private readonly IJsEngineProvider _jsEngineProvider;

	public ComputeDataTransformerFactory(OptionsRegistry registry, IJsEngineProvider jsEngineProvider)
	{
		_registry = registry;
		_jsEngineProvider = jsEngineProvider;
	}

	public string ComponentName => "compute";

	public bool CanHandle(string connectionString) => false;

	public string Category => "Transformers";
	public Type OptionsType => typeof(DtPipe.Transformers.Row.Compute.ComputeOptions);

	public IDataTransformer CreateFromConfiguration(IEnumerable<(string Option, string Value)> configuration)
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

	public IDataTransformer? CreateFromOptions(object options) =>
		options is ComputeOptions o ? CreateFromOptions(o) : null;

	public IDataTransformer CreateFromOptions(DtPipe.Transformers.Row.Compute.ComputeOptions options)
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

	public IDataTransformer? CreateFromYamlConfig(TransformerConfig config)
	{
		var mappings = new List<string>();

		if (config.Mappings != null && config.Mappings.Any())
		{
			foreach (var kvp in config.Mappings)
			{
				// Raw values; @file resolution happens in CreateFromOptions
				if (string.IsNullOrEmpty(kvp.Value))
					mappings.Add(kvp.Key);
				else
					mappings.Add($"{kvp.Key}:{kvp.Value}");
			}
		}

		if (!mappings.Any()) return null;

		bool skipNull = false;
		if (config.Options != null && config.Options.TryGetValue("skip-null", out var snStr))
			bool.TryParse(snStr, out skipNull);

		return CreateFromOptions(new ComputeOptions { Compute = mappings, SkipNull = skipNull });
	}

	private static string ResolveScriptContent(string script)
	{
		if (string.IsNullOrWhiteSpace(script)) return script;
		return DefaultStringContentResolver.Instance.ResolveAsync(script).GetAwaiter().GetResult() ?? script;
	}
}
