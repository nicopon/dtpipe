using System.Text.RegularExpressions;
using DtPipe.Core.Pipelines;
using YamlDotNet.Serialization;
using YamlDotNet.Serialization.NamingConventions;

namespace DtPipe.Configuration;

/// <summary>
/// Parses YAML job files into JobDefinition.
/// Supports ${{ENV_VAR}} interpolation for environment variables.
/// </summary>
public static partial class JobFileParser
{
	// Regex to match ${{ENV_VAR}} patterns (double braces to avoid collision with {COLUMN})
	[GeneratedRegex(@"\$\{\{([^}]+)\}\}", RegexOptions.Compiled)]
	private static partial Regex EnvVarPattern();

	private static readonly IDeserializer Deserializer = new DeserializerBuilder()
		.WithNamingConvention(HyphenatedNamingConvention.Instance)
		.IgnoreUnmatchedProperties()
		.Build();

	/// <summary>
	/// Parses a YAML job file into a JobDefinition.
	/// </summary>
	/// <param name="filePath">Path to the YAML file.</param>
	/// <returns>Parsed JobDefinition.</returns>
	public static JobDefinition Parse(string filePath)
	{
		if (!File.Exists(filePath))
		{
			throw new FileNotFoundException($"Job file not found: {filePath}");
		}

		var content = File.ReadAllText(filePath);

		// Interpolate environment variables
		content = InterpolateEnvVars(content);

		var yamlJob = Deserializer.Deserialize<YamlJobFile>(content);

		// Validate required fields
		if (string.IsNullOrWhiteSpace(yamlJob.Input))
			throw new InvalidOperationException("Job file missing required field: input");

		if (string.IsNullOrWhiteSpace(yamlJob.Output))
			throw new InvalidOperationException("Job file missing required field: output");

		return new JobDefinition
		{
			Input = yamlJob.Input,
			Query = yamlJob.Query,
			Output = yamlJob.Output,
			BatchSize = yamlJob.BatchSize ?? 50_000,
			Limit = yamlJob.Limit ?? 0,
			DryRun = yamlJob.DryRun ?? false,
			UnsafeQuery = yamlJob.UnsafeQuery ?? false,
			ConnectionTimeout = yamlJob.ConnectionTimeout ?? 10,
			QueryTimeout = yamlJob.QueryTimeout ?? 0,
			Strategy = yamlJob.Strategy,
			InsertMode = yamlJob.InsertMode,
			SamplingRate = yamlJob.SamplingRate ?? 1.0,
			SamplingSeed = yamlJob.SamplingSeed,
			Transformers = ParseTransformers(yamlJob.Transformers),
			ProviderOptions = yamlJob.ProviderOptions
		};
	}

	/// <summary>
	/// Interpolates ${{ENV_VAR}} patterns with environment variable values.
	/// </summary>
	private static string InterpolateEnvVars(string content)
	{
		return EnvVarPattern().Replace(content, match =>
		{
			var envVarName = match.Groups[1].Value.Trim();
			var envValue = Environment.GetEnvironmentVariable(envVarName);

			if (envValue is null)
			{
				Console.Error.WriteLine($"Warning: Environment variable '{envVarName}' is not set.");
				return match.Value; // Keep original if not found
			}

			return envValue;
		});
	}

	private static List<TransformerConfig>? ParseTransformers(List<Dictionary<object, object>>? transformers)
	{
		if (transformers is null || transformers.Count == 0)
			return null;

		var result = new List<TransformerConfig>();

		foreach (var transformerDict in transformers)
		{
			foreach (var kvp in transformerDict)
			{
				var type = kvp.Key?.ToString() ?? string.Empty;
				if (string.IsNullOrWhiteSpace(type))
				{
					throw new InvalidOperationException($"Transformer type cannot be null or empty.");
				}

				var value = kvp.Value;

				var config = new TransformerConfig { Type = type };

				if (value is Dictionary<object, object> dict)
				{
					// Check if this is a complex structure with explicit 'mappings' or 'options' keys
					// Example:
					// - type: complex-transformer
					//   mappings: { ... }
					//   options: { ... }
					object? mappingsObj = null;
					object? optionsObj = null;

					foreach (var subKvp in dict)
					{
						var keyStr = subKvp.Key.ToString() ?? string.Empty;
						if (string.Equals(keyStr, "mappings", StringComparison.OrdinalIgnoreCase))
							mappingsObj = subKvp.Value;
						else if (string.Equals(keyStr, "options", StringComparison.OrdinalIgnoreCase))
							optionsObj = subKvp.Value;
					}

					if (mappingsObj != null || optionsObj != null)
					{
						if (mappingsObj is Dictionary<object, object> mappingsDict)
							config = config with { Mappings = ParseStringDictionary(mappingsDict) };

						if (optionsObj is Dictionary<object, object> optionsDict)
							config = config with { Options = ParseStringDictionary(optionsDict) };
					}
					else
					{
						// Simple structure: the dictionary itself is the mappings
						// Example:
						// - type: simple-transformer
						//   col1: upper(col1)
						config = config with { Mappings = ParseStringDictionary(dict) };
					}
				}

				result.Add(config);
			}
		}

		return result;
	}

	private static Dictionary<string, string>? ParseStringDictionary(Dictionary<object, object>? dict)
	{
		if (dict is null) return null;

		var result = new Dictionary<string, string>();
		foreach (var kvp in dict)
		{
			if (kvp.Key != null)
				result[kvp.Key.ToString()!] = kvp.Value?.ToString() ?? string.Empty;
		}
		return result;
	}

	/// <summary>
	/// Internal YAML structure for flexible parsing.
	/// </summary>
	private class YamlJobFile
	{
		public string? Input { get; set; }
		public string? Query { get; set; }
		public string? Output { get; set; }
		public int? BatchSize { get; set; }
		public int? Limit { get; set; }
		public bool? DryRun { get; set; }
		public bool? UnsafeQuery { get; set; }
		public string? Strategy { get; set; }
		public string? InsertMode { get; set; }
		public int? ConnectionTimeout { get; set; }
		public int? QueryTimeout { get; set; }
		public double? SamplingRate { get; set; }
		public int? SamplingSeed { get; set; }
		public List<Dictionary<object, object>>? Transformers { get; set; }
		public Dictionary<string, Dictionary<string, object>>? ProviderOptions { get; set; }
	}
}
