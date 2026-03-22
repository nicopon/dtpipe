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
	/// Parses a YAML job file into a dictionary of JobDefinitions (DAG).
	/// Single-job files are automatically wrapped in a dictionary with key "main".
	/// </summary>
	/// <param name="filePath">Path to the YAML file.</param>
	/// <returns>Dictionary of JobDefinitions keyed by branch alias.</returns>
	public static Dictionary<string, DtPipe.Core.Models.JobDefinition> Parse(string filePath)
	{
		if (!File.Exists(filePath))
		{
			throw new FileNotFoundException($"Job file not found: {filePath}");
		}

		var content = File.ReadAllText(filePath);

		// Interpolate environment variables
		content = InterpolateEnvVars(content);

		// 1. Try to deserialize as a dictionary (DAG)
		try
		{
			var branches = Deserializer.Deserialize<Dictionary<string, DtPipe.Core.Models.JobDefinition>>(content);
			// Validate if it's really a DAG by checking if ANY branch has an input, sql processor, or from alias
			if (branches != null && branches.Count > 0 && branches.Values.Any(v => !string.IsNullOrEmpty(v.Input) || !string.IsNullOrEmpty(v.Sql) || !string.IsNullOrEmpty(v.From)))
			{
				// Successfully loaded as a DAG. Now handle transformers for each branch.
				var rootMap = Deserializer.Deserialize<Dictionary<string, object>>(content);
				foreach (var alias in branches.Keys)
				{
					if (rootMap != null && rootMap.TryGetValue(alias, out var branchObj) && branchObj is Dictionary<object, object> branchData)
					{
						if (branchData.TryGetValue("transformers", out var transObj) && transObj is List<object> transList)
						{
							var yamlTransformers = transList.OfType<Dictionary<object, object>>().ToList();
							branches[alias] = branches[alias] with { Transformers = ParseTransformers(yamlTransformers) };
						}
					}
				}
				return branches;
			}
		}
		catch { /* Fallback to single job */ }

		// 2. Fallback: Parse as a single JobDefinition (Standard)
		var job = Deserializer.Deserialize<DtPipe.Core.Models.JobDefinition>(content);
		var rawMap = Deserializer.Deserialize<Dictionary<string, object>>(content);
		if (rawMap != null && rawMap.TryGetValue("transformers", out var scalarTransObj) && scalarTransObj is List<object> scalarTransList)
		{
			var yamlTransformers = scalarTransList.OfType<Dictionary<object, object>>().ToList();
			job = job with { Transformers = ParseTransformers(yamlTransformers) };
		}

		return new Dictionary<string, DtPipe.Core.Models.JobDefinition> { { "main", job } };
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
			// Support new "sane" format: - type: fake, mappings: { ... }
			if (transformerDict.TryGetValue("type", out var typeObj))
			{
				var type = typeObj?.ToString() ?? string.Empty;
				var config = new TransformerConfig { Type = type };
				
				if (transformerDict.TryGetValue("mappings", out var m) && m is Dictionary<object, object> mDict)
					config = config with { Mappings = ParseStringDictionary(mDict) };
				
				if (transformerDict.TryGetValue("options", out var o) && o is Dictionary<object, object> oDict)
					config = config with { Options = ParseStringDictionary(oDict) };
					
				result.Add(config);
				continue;
			}

			// Support legacy "shortcut" format: - fake: { ... }
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
}
