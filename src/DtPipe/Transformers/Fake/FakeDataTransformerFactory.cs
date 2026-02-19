using System.CommandLine;
using DtPipe.Cli;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Options;
using DtPipe.Core.Pipelines;
using DtPipe.Cli.Abstractions;

namespace DtPipe.Transformers.Fake;

/// <summary>
/// Factory for creating fake data transformers.
/// </summary>
public interface IFakeDataTransformerFactory : IDataTransformerFactory, ICliContributor
{
}

public class FakeDataTransformerFactory : IFakeDataTransformerFactory
{
	private readonly OptionsRegistry _registry;
	private IEnumerable<Option>? _cliOptions;
	private Dictionary<string, string>? _aliasToProperty;

	public FakeDataTransformerFactory(OptionsRegistry registry)
	{
		_registry = registry;
	}

	public string ProviderName => TransformerType;

	public bool CanHandle(string connectionString) => false;

	public static IEnumerable<Type> GetSupportedOptionTypes()
	{
		yield return ComponentOptionsHelper.GetOptionsType<FakeDataTransformer>();
	}

	public string Category => "Transformer Options";

	public string TransformerType => FakeOptions.Prefix; // "fake"

	public IEnumerable<Option> GetCliOptions()
	{
		if (_cliOptions != null) return _cliOptions;

		// Manual option for listing fakers (not bound to POCO yet)
		var list = new List<Option>
		{
			new Option<bool>("--fake-list")
			{
				Description = "List all available fake data generators and exit"
			}
		};

		foreach (var type in GetSupportedOptionTypes())
		{
			var (options, aliasMap) = CliOptionBuilder.GenerateOptionsWithMetadataForType(type);
			list.AddRange(options);
			_aliasToProperty = aliasMap; // Store for CreateFromConfiguration
		}

		return _cliOptions = list;
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
		// Ensure alias map is initialized
		if (_aliasToProperty == null) GetCliOptions();

		// Defaults from registry
		var globalOptions = _registry.Get<FakeOptions>();

		var mappings = new List<string>();
		var locale = globalOptions.Locale;
		var seedColumn = globalOptions.SeedColumn;
		var deterministic = globalOptions.Deterministic;
		var seed = globalOptions.Seed;
		var skipNull = globalOptions.SkipNull;

		foreach (var (option, value) in configuration)
		{
			if (_aliasToProperty!.TryGetValue(option, out var propertyName))
			{
				switch (propertyName)
				{
					case nameof(FakeOptions.Fake):
						mappings.Add(value);
						break;
					case nameof(FakeOptions.Locale):
						locale = value;
						break;
					case nameof(FakeOptions.SeedColumn):
						seedColumn = value;
						break;
					case nameof(FakeOptions.Seed):
						if (int.TryParse(value, out var s)) seed = s;
						break;
					case nameof(FakeOptions.Deterministic):
						if (bool.TryParse(value, out var d)) deterministic = d;
						break;
					case nameof(FakeOptions.SkipNull):
						if (bool.TryParse(value, out var sn)) skipNull = sn;
						break;
				}
			}
		}

		var options = new FakeOptions
		{
			Fake = mappings,
			Locale = locale,
			Seed = seed,
			SeedColumn = seedColumn,
			Deterministic = deterministic,
			SkipNull = skipNull
		};

		return new FakeDataTransformer(options);
	}

	public IDataTransformer? CreateFromYamlConfig(TransformerConfig config)
	{
		if (config.Fake == null || config.Fake.Count == 0)
			return null;

		// Convert YAML dict mappings to list format: "COLUMN:faker.method"
		var mappings = config.Fake.Select(kvp => $"{kvp.Key}:{kvp.Value}").ToList();

		// Parse options from YAML
		var locale = "en";
		string? seedColumn = null;
		int? seed = null;
		var deterministic = false;
		var skipNull = false;

		if (config.Options != null)
		{
			if (config.Options.TryGetValue("locale", out var loc)) locale = loc;
			if (config.Options.TryGetValue("seed-column", out var sc)) seedColumn = sc;
			if (config.Options.TryGetValue("seed", out var seedStr) && int.TryParse(seedStr, out var s)) seed = s;
			if (config.Options.TryGetValue("deterministic", out var detStr) && bool.TryParse(detStr, out var d)) deterministic = d;
			if (config.Options.TryGetValue("skip-null", out var snStr) && bool.TryParse(snStr, out var sn)) skipNull = sn;
		}

		var options = new FakeOptions
		{
			Fake = mappings,
			Locale = locale,
			Seed = seed,
			SeedColumn = seedColumn,
			Deterministic = deterministic,
			SkipNull = skipNull
		};

		return new FakeDataTransformer(options);
	}

	public Task<int?> HandleCommandAsync(ParseResult parseResult, CancellationToken ct = default)
	{
		// Check for --fake-list flag
		if (parseResult.Tokens.Any(t => t.Value == "--fake-list"))
		{
			// Handle flag if present
			var option = GetCliOptions().FirstOrDefault(o => o.Name == "--fake-list") as Option<bool>;
			var isFakeList = option != null && parseResult.GetValue(option);
			if (isFakeList)
			{
				PrintFakerList();
				return Task.FromResult<int?>(0);
			}
		}
		return Task.FromResult<int?>(null);
	}

	private static void PrintFakerList()
	{
		var registry = new FakerRegistry();
		Console.WriteLine("Available fakers (use format: COLUMN:dataset.method)");
		Console.WriteLine();
		foreach (var (dataset, methods) in registry.ListAll())
		{
			Console.WriteLine($"{char.ToUpper(dataset[0])}{dataset[1..]}:");
			foreach (var (method, description) in methods)
			{
				Console.WriteLine($"  {$"{dataset}.{method}".ToLowerInvariant(),-30} {description}");
			}
			Console.WriteLine();
		}
	}
}
