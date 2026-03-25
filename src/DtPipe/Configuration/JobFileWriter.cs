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
		.WithAttributeOverride<JobDefinition>(j => j.ProviderOptions, new YamlIgnoreAttribute())
		.WithAttributeOverride<TransformerConfig>(t => t.Mask, new YamlIgnoreAttribute())
		.WithAttributeOverride<TransformerConfig>(t => t.Fake, new YamlIgnoreAttribute())
		.WithAttributeOverride<TransformerConfig>(t => t.Format, new YamlIgnoreAttribute())
		.WithAttributeOverride<TransformerConfig>(t => t.Compute, new YamlIgnoreAttribute())
		.WithAttributeOverride<TransformerConfig>(t => t.Filter, new YamlIgnoreAttribute())
		.WithAttributeOverride<TransformerConfig>(t => t.Window, new YamlIgnoreAttribute())
		.WithAttributeOverride<TransformerConfig>(t => t.Expand, new YamlIgnoreAttribute())
		.WithAttributeOverride<TransformerConfig>(t => t.Overwrite, new YamlIgnoreAttribute())
		.Build();
#pragma warning restore CS8603

	/// <summary>
	/// Writes a JobDefinition to a YAML file.
	/// </summary>
	public static void Write(string filePath, DtPipe.Core.Models.JobDefinition job)
	{
		var yaml = Serializer.Serialize(job);
		File.WriteAllText(filePath, yaml);
		Console.Error.WriteLine($"Job configuration exported to: {filePath}");
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
