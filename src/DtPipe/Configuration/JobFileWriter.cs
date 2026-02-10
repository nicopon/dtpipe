using YamlDotNet.Serialization;
using YamlDotNet.Serialization.NamingConventions;

namespace DtPipe.Configuration;

/// <summary>
/// Writes JobDefinition to YAML file.
/// Used by --export-job to export CLI configuration to YAML.
/// </summary>
public static class JobFileWriter
{
	private static readonly ISerializer Serializer = new SerializerBuilder()
		.WithNamingConvention(HyphenatedNamingConvention.Instance)
		.ConfigureDefaultValuesHandling(DefaultValuesHandling.OmitNull | DefaultValuesHandling.OmitDefaults)
		.Build();

	/// <summary>
	/// Writes a JobDefinition to a YAML file.
	/// </summary>
	public static void Write(string filePath, JobDefinition job)
	{
		var yamlJob = new YamlJobOutput
		{
			Input = job.Input,
			Query = job.Query,
			Output = job.Output,
			BatchSize = job.BatchSize != 50_000 ? job.BatchSize : null,
			Limit = job.Limit != 0 ? job.Limit : null,
			DryRun = job.DryRun ? true : null,
			UnsafeQuery = job.UnsafeQuery ? true : null,
			ConnectionTimeout = job.ConnectionTimeout != 10 ? job.ConnectionTimeout : null,
			QueryTimeout = job.QueryTimeout != 0 ? job.QueryTimeout : null,
			SampleRate = Math.Abs(job.SampleRate - 1.0) > 0.0001 ? job.SampleRate : null,
			SampleSeed = job.SampleSeed,
			Transformers = ConvertTransformers(job.Transformers)
		};

		var yaml = Serializer.Serialize(yamlJob);
		File.WriteAllText(filePath, yaml);
		Console.Error.WriteLine($"Job configuration exported to: {filePath}");
	}

	private static List<Dictionary<string, object>>? ConvertTransformers(List<TransformerConfig>? configs)
	{
		if (configs == null || configs.Count == 0)
			return null;

		var result = new List<Dictionary<string, object>>();

		foreach (var config in configs)
		{
			var transformerDict = new Dictionary<string, object>();
			var content = new Dictionary<string, object>();

			if (config.Mappings != null && config.Mappings.Count > 0)
			{
				content["mappings"] = config.Mappings;
			}

			if (config.Options != null && config.Options.Count > 0)
			{
				content["options"] = config.Options;
			}

			transformerDict[config.Type] = content;
			result.Add(transformerDict);
		}

		return result;
	}

	/// <summary>
	/// Internal YAML output structure.
	/// </summary>
	private class YamlJobOutput
	{
		public string? Input { get; set; }
		public string? Query { get; set; }
		public string? Output { get; set; }
		public int? BatchSize { get; set; }
		public int? Limit { get; set; }
		public bool? DryRun { get; set; }
		public bool? UnsafeQuery { get; set; }
		public int? ConnectionTimeout { get; set; }
		public int? QueryTimeout { get; set; }
		public double? SampleRate { get; set; }
		public int? SampleSeed { get; set; }
		public List<Dictionary<string, object>>? Transformers { get; set; }
	}
}
