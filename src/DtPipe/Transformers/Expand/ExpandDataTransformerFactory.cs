using System.CommandLine;
using DtPipe.Cli;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Options;
using DtPipe.Core.Services;
using DtPipe.Core.Pipelines;
using DtPipe.Cli.Abstractions;

namespace DtPipe.Transformers.Expand;

public class ExpandDataTransformerFactory : IDataTransformerFactory, ICliContributor
{
	private readonly OptionsRegistry _registry;
	private readonly IJsEngineProvider _jsEngineProvider;

	public ExpandDataTransformerFactory(OptionsRegistry registry, IJsEngineProvider jsEngineProvider)
	{
		_registry = registry;
		_jsEngineProvider = jsEngineProvider;
	}

	public string ProviderName => TransformerType;

	public bool CanHandle(string connectionString) => false;

	public string Category => "Transformer Options";
	public string TransformerType => "expand";

	public static IEnumerable<Type> GetSupportedOptionTypes()
	{
		yield return ComponentOptionsHelper.GetOptionsType<ExpandDataTransformer>();
	}

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

		if (expands.Count == 0) return new ExpandDataTransformer(new ExpandOptions(), _jsEngineProvider);

		return new ExpandDataTransformer(new ExpandOptions { Expand = expands.ToArray() }, _jsEngineProvider);
	}

	public IDataTransformer? CreateFromYamlConfig(TransformerConfig config)
	{
		var expands = new List<string>();

		if (config.Script != null)
		{
			expands.AddRange(config.Script.Values);
		}

		if (expands.Count == 0) return null;

		return new ExpandDataTransformer(new ExpandOptions { Expand = expands.ToArray() }, _jsEngineProvider);
	}

	public Task<int?> HandleCommandAsync(ParseResult parseResult, CancellationToken ct = default)
	{
		return Task.FromResult<int?>(null);
	}
}
