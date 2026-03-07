using System.CommandLine;
using System.CommandLine.Completions;
using System.CommandLine.Parsing;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Pipelines;
using DtPipe.Core.Options;
using DtPipe.Transformers.Columnar.Fake;

namespace DtPipe.Cli.Infrastructure;

public class CliDataTransformerFactory : IDataTransformerFactory, ICliContributor
{
	private readonly IDataTransformerFactory _inner;
	private readonly FakerRegistry _fakerRegistry = new();

	public CliDataTransformerFactory(IDataTransformerFactory inner)
	{
		_inner = inner;
	}

	public string ComponentName => _inner.ComponentName;
	public string Category => _inner.Category;

	public string? BoundComponentName => null; // Global transformers

	private IReadOnlyDictionary<string, CliPipelinePhase>? _flagPhases;
	public IReadOnlyDictionary<string, CliPipelinePhase> FlagPhases
	{
		get
		{
			if (_flagPhases == null)
			{
				var phases = new Dictionary<string, CliPipelinePhase>(StringComparer.OrdinalIgnoreCase);
				foreach (var opt in GetCliOptions())
				{
					phases[opt.Name] = CliPipelinePhase.Transformer;
					foreach (var alias in opt.Aliases)
					{
						phases[alias] = CliPipelinePhase.Transformer;
					}
				}
				_flagPhases = phases;
			}
			return _flagPhases;
		}
	}

	private IReadOnlyDictionary<string, string>? _flagDependencies;
	public IReadOnlyDictionary<string, string> FlagDependencies
	{
		get
		{
			if (_flagDependencies == null)
			{
				var deps = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
				var primaryFlagName = ComponentName.ToLowerInvariant();
				string requiredFlag = $"--{primaryFlagName}";

				foreach (var opt in GetCliOptions())
				{
					// If the option is NOT the primary flag itself
					if (!opt.Name.Equals(requiredFlag, StringComparison.OrdinalIgnoreCase))
					{
						deps[opt.Name] = requiredFlag;
						foreach (var alias in opt.Aliases)
						{
							deps[alias] = requiredFlag;
						}
					}
				}
				_flagDependencies = deps;
			}
			return _flagDependencies;
		}
	}
	public Type OptionsType => _inner.OptionsType;

	public bool CanHandle(string connectionString) => _inner.CanHandle(connectionString);

	public IDataTransformer CreateFromConfiguration(IEnumerable<(string Option, string Value)> configuration)
	{
		return _inner.CreateFromConfiguration(configuration);
	}

	public IDataTransformer? CreateFromYamlConfig(TransformerConfig config)
	{
		return _inner.CreateFromYamlConfig(config);
	}

	public IEnumerable<Option> GetCliOptions()
	{
		var options = CliOptionBuilder.GenerateOptionsForType(OptionsType).ToList();

		if (ComponentName == "fake")
		{
			var fakeOpt = options.FirstOrDefault(o => o.Name == "--fake");
			if (fakeOpt != null)
			{
				fakeOpt.CompletionSources.Add(SuggestFakers);
			}
		}

		return options;
	}

	private IEnumerable<CompletionItem> SuggestFakers(CompletionContext context)
	{
		var word = context.WordToComplete ?? "";
		var colonIndex = word.IndexOf(':');
		if (colonIndex < 0) return Enumerable.Empty<CompletionItem>();

		var columnPart = word[..(colonIndex + 1)];
		var fakerPart = word[(colonIndex + 1)..];

		var all = _fakerRegistry.ListAll();

		var dotIndex = fakerPart.IndexOf('.');
		if (dotIndex < 0)
		{
			// Suggest Families
			return all
				.Select(g => g.Dataset.ToLowerInvariant() + ".")
				.Where(f => f.StartsWith(fakerPart, StringComparison.OrdinalIgnoreCase))
				.Select(f => new CompletionItem(columnPart + f + "[NOSUSP]"));
		}
		else
		{
			// Suggest Methods in Family
			var family = fakerPart[..dotIndex];
			var methodPrefix = fakerPart[(dotIndex + 1)..];

			var group = all.FirstOrDefault(g => g.Dataset.Equals(family, StringComparison.OrdinalIgnoreCase));
			if (group.Methods == null) return Enumerable.Empty<CompletionItem>();

			return group.Methods
				.Select(m => m.Method.ToLowerInvariant())
				.Where(m => m.StartsWith(methodPrefix, StringComparison.OrdinalIgnoreCase))
				.Select(m => new CompletionItem(columnPart + family.ToLowerInvariant() + "." + m));
		}
	}

	public void BindOptions(ParseResult parseResult, OptionsRegistry registry)
	{
		var options = GetCliOptions();
		var existingOptions = registry.Get(OptionsType);
		CliOptionBuilder.BindForType(OptionsType, existingOptions, parseResult, options, null);
		registry.RegisterByType(OptionsType, existingOptions);
	}
}
