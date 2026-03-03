using DtPipe.Core.Abstractions;
using DtPipe.Core.Options;
using DtPipe.Core.Pipelines;

namespace DtPipe.Transformers.Columnar.Fake;

/// <summary>
/// Factory for creating fake data transformers.
/// </summary>
public interface IFakeDataTransformerFactory : IDataTransformerFactory
{
}

public class FakeDataTransformerFactory : IFakeDataTransformerFactory
{
	private readonly OptionsRegistry _registry;

	public FakeDataTransformerFactory(OptionsRegistry registry)
	{
		_registry = registry;
	}

	public string ComponentName => "fake";

	public bool CanHandle(string connectionString) => false;

	public string Category => "Transformers";
	public Type OptionsType => typeof(DtPipe.Transformers.Columnar.Fake.FakeOptions);

	public IDataTransformer CreateFromConfiguration(IEnumerable<(string Option, string Value)> configuration)
	{
		var globalOptions = _registry.Get<DtPipe.Transformers.Columnar.Fake.FakeOptions>();
		var mappings = new List<string>();
		var locale = globalOptions.Locale;
		var seedColumn = globalOptions.SeedColumn;
		var deterministic = globalOptions.Deterministic;
		var seed = globalOptions.Seed;
		var skipNull = globalOptions.SkipNull;

		foreach (var (option, value) in configuration)
		{
			var opt = option.ToLowerInvariant();
			if (opt == "fake" || opt == "--fake") mappings.Add(value);
			else if (opt == "fake-locale" || opt == "--fake-locale" || opt == "locale") locale = value;
			else if (opt == "fake-seed-column" || opt == "--fake-seed-column" || opt == "seed-column") seedColumn = value;
			else if (opt == "fake-seed" || opt == "--fake-seed" || opt == "seed") { if (int.TryParse(value, out var sVal)) seed = sVal; }
			else if (opt == "fake-deterministic" || opt == "--fake-deterministic" || opt == "deterministic") { if (bool.TryParse(value, out var dVal)) deterministic = dVal; }
			else if (opt == "fake-skip-null" || opt == "--fake-skip-null" || opt == "skip-null") { if (bool.TryParse(value, out var snVal)) skipNull = snVal; }
		}

		var options = new DtPipe.Transformers.Columnar.Fake.FakeOptions
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

	public IDataTransformer CreateFromOptions(DtPipe.Transformers.Columnar.Fake.FakeOptions options)
	{
		return new FakeDataTransformer(options);
	}

	public IDataTransformer? CreateFromYamlConfig(TransformerConfig config)
	{
		var mappings = new List<string>();
		if (config.Mappings != null)
		{
			foreach (var kvp in config.Mappings)
			{
				mappings.Add($"{kvp.Key}:{kvp.Value}");
			}
		}

		var options = new DtPipe.Transformers.Columnar.Fake.FakeOptions { Fake = mappings };

		if (config.Options != null)
		{
			if (config.Options.TryGetValue("locale", out var loc)) options = options with { Locale = loc };
			if (config.Options.TryGetValue("seed", out var s) && int.TryParse(s, out var sv)) options = options with { Seed = sv };
			if (config.Options.TryGetValue("seed-column", out var sc)) options = options with { SeedColumn = sc };
			if (config.Options.TryGetValue("deterministic", out var d) && bool.TryParse(d, out var dv)) options = options with { Deterministic = dv };
			if (config.Options.TryGetValue("skip-null", out var sn) && bool.TryParse(sn, out var snv)) options = options with { SkipNull = snv };
		}
		return new FakeDataTransformer(options);
	}
}
