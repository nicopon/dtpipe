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
		var options = new ProjectOptions();
        var projects = new List<string>();
        var drops = new List<string>();
        var renames = new List<string>();

        foreach (var (opt, val) in configuration)
        {
            if (opt.Equals("project", StringComparison.OrdinalIgnoreCase) || opt.Equals("--project", StringComparison.OrdinalIgnoreCase))
                projects.Add(val);
            else if (opt.Equals("drop", StringComparison.OrdinalIgnoreCase) || opt.Equals("--drop", StringComparison.OrdinalIgnoreCase))
                drops.Add(val);
            else if (opt.Equals("rename", StringComparison.OrdinalIgnoreCase) || opt.Equals("--rename", StringComparison.OrdinalIgnoreCase))
                renames.Add(val);
        }

        options.Project = projects;
        options.Drop = drops;
        options.Rename = renames;

		return new ProjectDataTransformer(options);
	}

	public IDataTransformer? CreateFromYamlConfig(TransformerConfig config)
	{
		var options = new ProjectOptions();

		// Handle "project" (whitelist)
		if (config.Mappings != null && config.Mappings.Count > 0)
		{
			options.Project = config.Mappings.Keys;
		}
		else if (config.Options != null && config.Options.TryGetValue("project", out var projectVal))
		{
			options.Project = new[] { projectVal };
		}

		// Handle "drop" (blacklist)
		if (config.Options != null && config.Options.TryGetValue("drop", out var dropVal))
		{
			options.Drop = new[] { dropVal };
		}

        // Handle "rename"
        if (config.Options != null && config.Options.TryGetValue("rename", out var renameVal))
        {
            options.Rename = new[] { renameVal };
        }

		if (!options.Project.Any() && !options.Drop.Any() && !options.Rename.Any())
			return null;

		return new ProjectDataTransformer(options);
	}
}
