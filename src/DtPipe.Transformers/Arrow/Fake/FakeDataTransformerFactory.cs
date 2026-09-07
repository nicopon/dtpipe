using DtPipe.Core.Abstractions;
using DtPipe.Core.Options;
using DtPipe.Core.Pipelines;

using DtPipe.Transformers.Abstract;

namespace DtPipe.Transformers.Arrow.Fake;

public class FakeDataTransformerFactory : TransformerFactoryBase<FakeOptions>
{

	public override string ComponentName => "fake";

	public FakeDataTransformerFactory(OptionsRegistry registry) : base(registry) { }



	public override string Category => "Transformers";

	public override IDataTransformer CreateFromConfiguration(IEnumerable<(string Option, string Value)> configuration)
	{
		var globalOptions = Registry.Get<DtPipe.Transformers.Arrow.Fake.FakeOptions>();
		var mappings = new List<string>();
		var locale = globalOptions.Locale;
		var seedColumns = new List<string>(globalOptions.SeedColumn);
		var seedRow = globalOptions.SeedRow;
		var seed = globalOptions.Seed;
		var skipNull = globalOptions.SkipNull;

		foreach (var (option, value) in configuration)
		{
			var opt = option.ToLowerInvariant();
			if (opt == "fake" || opt == "--fake") mappings.Add(value);
			else if (opt == "fake-locale" || opt == "--fake-locale" || opt == "locale") locale = value;
			else if (opt == "fake-seed-column" || opt == "--fake-seed-column" || opt == "seed-column")
			{
				if (!string.IsNullOrEmpty(value))
				{
					// Support comma-separated columns for composite seeds
					seedColumns.AddRange(value.Split(',').Select(x => x.Trim()).Where(x => !string.IsNullOrEmpty(x)));
				}
			}
			else if (opt == "fake-seed" || opt == "--fake-seed" || opt == "seed") { if (int.TryParse(value, out var sVal)) seed = sVal; }
			else if (opt == "fake-deterministic" || opt == "--fake-deterministic" || opt == "deterministic")
			{
				// Throw explicit exception for the deprecated option to guide users
				throw new ArgumentException("The option '--fake-deterministic' has been renamed to '--fake-seed-row'. Please update your scripts.");
			}
			else if (opt == "fake-seed-row" || opt == "--fake-seed-row" || opt == "seed-row") { if (bool.TryParse(value, out var srVal)) seedRow = srVal; }
			else if (opt == "fake-skip-null" || opt == "--fake-skip-null" || opt == "skip-null") { if (bool.TryParse(value, out var snVal)) skipNull = snVal; }
		}

		var options = new DtPipe.Transformers.Arrow.Fake.FakeOptions
		{
			Fake = mappings,
			Locale = locale,
			Seed = seed,
			SeedColumn = seedColumns,
			SeedRow = seedRow,
			SkipNull = skipNull
		};

		return new FakeDataTransformer(options);
	}

	protected override IDataTransformer? CreateFromTypedOptions(FakeOptions options)
	{
		return new FakeDataTransformer(options);
	}

	public override object? CreateOptionsFromYaml(TransformerConfig config)
	{
		// 'deterministic' is not a property, so the generic binder would report it as an unknown
		// key and suggest nothing: the edit distance to 'seed-row' is far past the threshold.
		// Naming the rename here is what turns a dead job file into a one-line fix.
		if (config.Options?.ContainsKey("deterministic") == true)
			throw new ArgumentException("The YAML option 'deterministic' has been renamed to 'seed-row'. Please update your configuration.");

		var mappings = (config.Mappings ?? []).Select(kvp => $"{kvp.Key}:{kvp.Value}").ToList();
		return new DtPipe.Transformers.Arrow.Fake.FakeOptions { Fake = mappings };
	}
}
