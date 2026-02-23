
using DtPipe.Core.Abstractions;
using DtPipe.Core.Options;
using DtPipe.Transformers.Services;
using DtPipe.Core.Pipelines;

namespace DtPipe.Transformers.Script;

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
	public Type OptionsType => typeof(ComputeOptions);

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

		return new ComputeDataTransformer(new ComputeOptions { Compute = mappings, SkipNull = skipNull }, _jsEngineProvider);
	}

	public IDataTransformer? CreateFromYamlConfig(TransformerConfig config)
	{
		var mappings = new List<string>();

		if (config.Compute != null && config.Compute.Any())
		{
			mappings.AddRange(config.Compute.Select(kvp => $"{kvp.Key}:{ResolveScriptContent(kvp.Value)}"));
		}

		if (!mappings.Any()) return null;

		bool skipNull = false;
		if (config.Options != null && config.Options.TryGetValue("skip-null", out var snStr))
		{
			bool.TryParse(snStr, out skipNull);
		}

		return new ComputeDataTransformer(new ComputeOptions { Compute = mappings, SkipNull = skipNull }, _jsEngineProvider);
	}

	private static string ResolveScriptContent(string script)
	{
		if (string.IsNullOrWhiteSpace(script)) return script;
		if (script.StartsWith("@"))
		{
			var path = script.Substring(1);
			if (File.Exists(path)) return File.ReadAllText(path);
			return script;
		}
		if (File.Exists(script)) return File.ReadAllText(script);
		return script;
	}
}
