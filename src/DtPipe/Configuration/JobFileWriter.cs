using DtPipe.Core.Pipelines;
using DtPipe.Core.Models;
using YamlDotNet.Serialization;
using YamlDotNet.Serialization.NamingConventions;

namespace DtPipe.Configuration;

/// <summary>
/// Writes JobDefinition to YAML file.
/// Used by --export-job to export CLI configuration to YAML.
/// </summary>
public static class JobFileWriter
{
#pragma warning disable CS8603
	private static readonly ISerializer Serializer = new SerializerBuilder()
		.WithNamingConvention(HyphenatedNamingConvention.Instance)
		.ConfigureDefaultValuesHandling(DefaultValuesHandling.OmitNull | DefaultValuesHandling.OmitDefaults)
		.WithAttributeOverride<JobDefinition>(j => j.NoStats, new YamlIgnoreAttribute())
		.Build();
#pragma warning restore CS8603

	/// <summary>
	/// Writes a JobDefinition to a YAML file as a DAG with a 'main' branch.
	/// </summary>
	public static void Write(string filePath, DtPipe.Core.Models.JobDefinition job)
	{
		Write(filePath, new Dictionary<string, JobDefinition> { { "main", job } });
	}

	/// <summary>
	/// Writes a dictionary of JobDefinitions (DAG) to a YAML file.
	/// </summary>
	public static void Write(string filePath, Dictionary<string, DtPipe.Core.Models.JobDefinition> jobs)
	{
		var yaml = Serializer.Serialize(jobs);
		File.WriteAllText(filePath, yaml);
		Console.Error.WriteLine($"DAG configuration exported to: {filePath}");
	}
}
