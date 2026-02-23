using DtPipe.Core.Abstractions;
using DtPipe.Core.Options;
using DtPipe.Core.Pipelines;

namespace DtPipe.Transformers.Fake;

/// <summary>
/// Factory for creating fake data transformers.
/// </summary>
public interface IFakeDataTransformerFactory : IDataTransformerFactory
{
}

public class FakeDataTransformerFactory : IFakeDataTransformerFactory
{
	private readonly OptionsRegistry _registry;
	// fields removed

	public FakeDataTransformerFactory(OptionsRegistry registry)
	{
		_registry = registry;
	}

	public string ComponentName => "fake";

	public bool CanHandle(string connectionString) => false;

	public string Category => "Transformers";
	public Type OptionsType => typeof(FakeOptions);


	public IDataTransformer CreateFromConfiguration(IEnumerable<(string Option, string Value)> configuration)
	{
		var globalOptions = _registry.Get<FakeOptions>();
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
            else if (opt == "fake-seed" || opt == "--fake-seed" || opt == "seed") { if (int.TryParse(value, out var s)) seed = s; }
            else if (opt == "fake-deterministic" || opt == "--fake-deterministic" || opt == "deterministic") { if (bool.TryParse(value, out var d)) deterministic = d; }
            else if (opt == "fake-skip-null" || opt == "--fake-skip-null" || opt == "skip-null") { if (bool.TryParse(value, out var sn)) skipNull = sn; }
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

}
