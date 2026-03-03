using DtPipe.Core.Abstractions;
using DtPipe.Core.Options;
using DtPipe.Core.Pipelines;

namespace DtPipe.Transformers.Columnar.Project;

public class ProjectDataTransformerFactory(OptionsRegistry registry) : IDataTransformerFactory
{
	private readonly OptionsRegistry _registry = registry;

	public string ComponentName => "project";

	public bool CanHandle(string connectionString) => false;

	public string Category => "Transformers";
	public Type OptionsType => typeof(DtPipe.Transformers.Columnar.Project.ProjectOptions);

	public IDataTransformer CreateFromOptions(DtPipe.Transformers.Columnar.Project.ProjectOptions options)
	{
		return new ProjectDataTransformer(options);
	}

	public IDataTransformer CreateFromConfiguration(IEnumerable<(string Option, string Value)> configuration)
	{
		var configDict = configuration.ToDictionary(k => k.Option, v => v.Value, StringComparer.OrdinalIgnoreCase);

		// Handle config dictionary to options
		var options = new ProjectOptions();
		if (configDict.TryGetValue("project", out var project1) || configDict.TryGetValue("--project", out project1)) options.Project = project1;
		if (configDict.TryGetValue("drop", out var drop1) || configDict.TryGetValue("--drop", out drop1)) options.Drop = drop1;

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
