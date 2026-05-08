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

		// 1. Deserialize as a dictionary (DAG)
		var branches = Deserializer.Deserialize<Dictionary<string, DtPipe.Core.Models.JobDefinition>>(content);
		if (branches == null || branches.Count == 0)
		{
			throw new InvalidOperationException("The job file is empty or invalid. A job file must define at least one named branch (DAG format).");
		}

		// Successfully loaded as a DAG. Now handle transformers for each branch.
		var rootMap = Deserializer.Deserialize<Dictionary<string, object>>(content);
		foreach (var alias in branches.Keys)
		{
			if (rootMap != null && rootMap.TryGetValue(alias, out var branchObj) && branchObj is System.Collections.IDictionary branchData)
			{
				if (branchData.Contains("transformers") && branchData["transformers"] is System.Collections.IEnumerable transList)
				{
					var yamlTransformers = transList.Cast<object>()
						.Select(t => t as System.Collections.IDictionary)
						.Where(t => t != null)
						.Cast<System.Collections.IDictionary>()
						.ToList();
					branches[alias] = branches[alias] with { Transformers = ParseTransformers(yamlTransformers) };
				}
			}
		}
		return branches;
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

	private static List<TransformerConfig>? ParseTransformers(List<System.Collections.IDictionary>? transformers)
	{
		if (transformers is null || transformers.Count == 0)
			return null;

		var result = new List<TransformerConfig>();

		foreach (var dict in transformers)
		{
			if (dict.Contains("type"))
			{
				var typeObj = dict["type"];
				var config = new TransformerConfig { Type = typeObj?.ToString() ?? string.Empty };
				
				if (dict.Contains("mappings") && dict["mappings"] is System.Collections.IDictionary mDict)
					config = config with { Mappings = ParseStringDictionary(mDict) };
				
				if (dict.Contains("options") && dict["options"] is System.Collections.IDictionary oDict)
					config = config with { Options = ParseStringDictionary(oDict) };
					
				result.Add(config);
			}
			else
			{
				Console.Error.WriteLine("Warning: Skipping transformer without 'type' property. The legacy 'shortcut' format is no longer supported.");
			}
		}

		return result;
	}

	private static Dictionary<string, string>? ParseStringDictionary(System.Collections.IDictionary? dict)
	{
		if (dict is null) return null;

		var result = new Dictionary<string, string>();
		foreach (System.Collections.DictionaryEntry kvp in dict)
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
