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
	/// <summary>
	/// Key order for an exported branch: where it reads, what it does, where it writes, how it is
	/// tuned, then the nested per-provider detail. YamlDotNet otherwise emits members in reflection
	/// order — the order JobDefinition happens to declare them — which put 'output' before 'from'
	/// and buried the routing keys among the engine controls.
	/// A property missing from this list still serializes; it sorts ahead of every listed one
	/// (Order defaults to 0), which is visible in the file rather than silent.
	/// </summary>
	private static readonly string[] KeyOrder =
	[
		nameof(JobDefinition.Input), nameof(JobDefinition.From), nameof(JobDefinition.Ref),
		nameof(JobDefinition.FromCheckpoint),
		nameof(JobDefinition.Transformers),
		nameof(JobDefinition.Output), nameof(JobDefinition.Checkpoint),
		nameof(JobDefinition.BatchSize), nameof(JobDefinition.MaxBatchBytes), nameof(JobDefinition.Limit),
		nameof(JobDefinition.SamplingRate), nameof(JobDefinition.SamplingSeed),
		nameof(JobDefinition.DryRunCount), nameof(JobDefinition.Prefix),
		nameof(JobDefinition.Cursor), nameof(JobDefinition.State), nameof(JobDefinition.Session),
		nameof(JobDefinition.MetricsPath), nameof(JobDefinition.LogPath),
		nameof(JobDefinition.ProviderOptions),
	];

#pragma warning disable CS8603
	private static readonly ISerializer Serializer = BuildSerializer();

	private static ISerializer BuildSerializer()
	{
		var builder = new SerializerBuilder()
			.WithNamingConvention(HyphenatedNamingConvention.Instance)
			// OmitDefaults compares against the TYPE default, so a collection left at its
			// Array.Empty<T>() initialiser is not a default and was written out as 'ref: []' —
			// anchored '&o0' and aliased on every later branch, because that initialiser is one
			// shared instance. An empty collection carries nothing an exported job needs.
			.ConfigureDefaultValuesHandling(DefaultValuesHandling.OmitNull | DefaultValuesHandling.OmitDefaults | DefaultValuesHandling.OmitEmptyCollections)
			.WithAttributeOverride<JobDefinition>(j => j.NoStats, new YamlIgnoreAttribute());

		for (int i = 0; i < KeyOrder.Length; i++)
			builder = builder.WithAttributeOverride(typeof(JobDefinition), KeyOrder[i], new YamlMemberAttribute { Order = i + 1 });

		return builder.Build();
	}
#pragma warning restore CS8603

	/// <summary>
	/// Writes a JobDefinition to a YAML file as a DAG with a 'main' branch.
	/// </summary>
	public static void Write(string filePath, DtPipe.Core.Models.JobDefinition job)
	{
		Write(filePath, new Dictionary<string, JobDefinition> { { "main", job } });
	}

	/// <summary>
	/// Serializes a dictionary of JobDefinitions (DAG) to YAML.
	/// </summary>
	public static string Serialize(Dictionary<string, DtPipe.Core.Models.JobDefinition> jobs)
		=> Serializer.Serialize(jobs);

	/// <summary>
	/// Writes a dictionary of JobDefinitions (DAG) to a YAML file.
	/// </summary>
	public static void Write(string filePath, Dictionary<string, DtPipe.Core.Models.JobDefinition> jobs)
	{
		var yaml = Serialize(jobs);
		File.WriteAllText(filePath, yaml);
		Console.Error.WriteLine($"DAG configuration exported to: {filePath}");
	}
}
