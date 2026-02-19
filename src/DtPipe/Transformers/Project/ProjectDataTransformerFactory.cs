using System.CommandLine;
using DtPipe.Cli;
using DtPipe.Cli.Abstractions;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Options;
using DtPipe.Core.Pipelines;

namespace DtPipe.Transformers.Project;

public class ProjectDataTransformerFactory(OptionsRegistry registry) : IDataTransformerFactory, ICliContributor
{
	private readonly OptionsRegistry _registry = registry;

	public string ProviderName => TransformerType;

	public bool CanHandle(string connectionString) => false;

	public static IEnumerable<Type> GetSupportedOptionTypes()
	{
		yield return ComponentOptionsHelper.GetOptionsType<ProjectDataTransformer>();
	}

	public string Category => "Transformer Options";
	public string TransformerType => ProjectOptions.Prefix;
	public int Priority => 100; // Last in pipeline

	private IEnumerable<Option>? _cliOptions;

	public IEnumerable<Option> GetCliOptions()
	{
		return _cliOptions ??= [.. GetSupportedOptionTypes().SelectMany(CliOptionBuilder.GenerateOptionsForType)];
	}

	public void BindOptions(ParseResult parseResult, OptionsRegistry registry)
	{
		var options = GetCliOptions();

		foreach (var type in GetSupportedOptionTypes())
		{
			var boundOptions = CliOptionBuilder.BindForType(type, parseResult, options);
			registry.RegisterByType(type, boundOptions);
		}
	}

	// Create(DumpOptions) removed


	public IDataTransformer CreateFromConfiguration(IEnumerable<(string Option, string Value)> configuration)
	{
		var configDict = configuration.ToDictionary(k => k.Option, v => v.Value, StringComparer.OrdinalIgnoreCase);

		// Handle config dictionary to options
		var options = new ProjectOptions();
		if (configDict.TryGetValue("--project", out var project)) options.Project = project;
		if (configDict.TryGetValue("--drop", out var drop)) options.Drop = drop;

		return new ProjectDataTransformer(options);
	}

	public IDataTransformer? CreateFromYamlConfig(TransformerConfig config)
	{
		var options = new ProjectOptions();

		// Handle "project" (whitelist)
		// Mappings keys are columns to keep
		if (config.Mappings != null && config.Mappings.Count > 0)
		{
			options.Project = string.Join(",", config.Mappings.Keys);
		}
		else if (config.Options != null && config.Options.TryGetValue("project", out var projectVal))
		{
			// Alternative: --project defined as option in yaml manually
			options.Project = projectVal;
		}

		// Handle "drop" (blacklist)
		if (config.Options != null && config.Options.TryGetValue("drop", out var dropVal))
		{
			options.Drop = dropVal;
		}

		if (string.IsNullOrWhiteSpace(options.Project) && string.IsNullOrWhiteSpace(options.Drop))
			return null;

		return new ProjectDataTransformer(options);
	}
}
