using System.CommandLine;
using DtPipe.Cli;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Options;
using DtPipe.Core.Services;
using DtPipe.Core.Pipelines;
using DtPipe.Cli.Abstractions;

namespace DtPipe.Transformers.Script;

public class ComputeDataTransformerFactory : IDataTransformerFactory, ICliContributor
{
	private readonly OptionsRegistry _registry;
	private readonly IJsEngineProvider _jsEngineProvider;

	public ComputeDataTransformerFactory(OptionsRegistry registry, IJsEngineProvider jsEngineProvider)
	{
		_registry = registry;
		_jsEngineProvider = jsEngineProvider;
	}

	public string ProviderName => TransformerType;

	public bool CanHandle(string connectionString) => false;

	public static IEnumerable<Type> GetSupportedOptionTypes()
	{
		yield return ComponentOptionsHelper.GetOptionsType<ComputeDataTransformer>();
	}

	public string Category => "Transformer Options";
	public string TransformerType => ComputeOptions.Prefix;

	public IEnumerable<Option> GetCliOptions()
	{
		var list = new List<Option>();
		foreach (var type in GetSupportedOptionTypes())
		{
			var (options, _) = CliOptionBuilder.GenerateOptionsWithMetadataForType(type);
			list.AddRange(options);
		}
		return list;
	}

	public void BindOptions(ParseResult parseResult, OptionsRegistry registry)
	{
		var allOptions = GetCliOptions();
		foreach (var type in GetSupportedOptionTypes())
		{
			var boundOptions = CliOptionBuilder.BindForType(type, parseResult, allOptions);
			registry.RegisterByType(type, boundOptions);
		}
	}

	// Create(DumpOptions) removed


	public IDataTransformer CreateFromConfiguration(IEnumerable<(string Option, string Value)> configuration)
	{
		// Manual parsing for CLI options
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
		// Support both specific 'compute' config or legacy 'script' if mapped
		var mappings = new List<string>();

		if (config.Script != null && config.Script.Any())
		{
			mappings.AddRange(config.Script.Select(kvp => $"{kvp.Key}:{ResolveScriptContent(kvp.Value)}"));
		}

		// Use Script property as carrier for backward compatibility
		if (!mappings.Any()) return null;

		bool skipNull = false;
		if (config.Options != null && config.Options.TryGetValue("skip-null", out var snStr))
		{
			bool.TryParse(snStr, out skipNull);
		}

		return new ComputeDataTransformer(new ComputeOptions { Compute = mappings, SkipNull = skipNull }, _jsEngineProvider);
	}

	public Task<int?> HandleCommandAsync(ParseResult parseResult, CancellationToken ct = default)
	{
		return Task.FromResult<int?>(null);
	}
	private static string ResolveScriptContent(string script)
	{
		if (string.IsNullOrWhiteSpace(script)) return script;

		// Explicit @ syntax
		if (script.StartsWith("@"))
		{
			var path = script.Substring(1);
			if (File.Exists(path))
			{
				return File.ReadAllText(path);
			}
			// Fallback: return as-is if file not found
			return script;
		}

		// Implicit syntax
		// Only load if it looks like a file path
		if (File.Exists(script))
		{
			return File.ReadAllText(script);
		}

		return script;
	}
}
