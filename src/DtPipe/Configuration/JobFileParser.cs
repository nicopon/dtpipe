using System.Text.RegularExpressions;
using DtPipe.Core.Pipelines;
using YamlDotNet.Serialization;
using YamlDotNet.Serialization.NamingConventions;
using YamlDotNet.Core;

namespace DtPipe.Configuration;

/// <summary>
/// Parses YAML job files into JobDefinition.
/// Supports ${{ENV_VAR}} and ${{keyring://alias}} interpolation.
/// </summary>
public static partial class JobFileParser
{
	// Regex to match ${{ENV_VAR}} or ${{keyring://alias}} patterns (double braces to avoid collision with {COLUMN})
	[GeneratedRegex(@"\$\{\{([^}]+)\}\}", RegexOptions.Compiled)]
	private static partial Regex EnvVarPattern();

	/// <summary>
	/// Parses a YAML job file into a dictionary of JobDefinitions (DAG).
	/// Single-job files are automatically wrapped in a dictionary with key "main".
	/// </summary>
	/// <param name="filePath">Path to the YAML file.</param>
	/// <param name="secretsManager">Optional secrets manager to resolve keyring:// references.</param>
	/// <returns>Dictionary of JobDefinitions keyed by branch alias.</returns>
	public static Dictionary<string, DtPipe.Core.Models.JobDefinition> Parse(string filePath, DtPipe.Cli.Security.ISecretsManager? secretsManager = null)
	{
		if (!File.Exists(filePath))
		{
			throw new FileNotFoundException($"Job file not found: {filePath}");
		}

		var content = File.ReadAllText(filePath);

		var deserializer = new DeserializerBuilder()
			.WithNamingConvention(HyphenatedNamingConvention.Instance)
			.IgnoreUnmatchedProperties()
			.WithNodeDeserializer(new InterpolatingNodeDeserializer(secretsManager), s => s.OnTop())
			.Build();

		// 1. Deserialize as a dictionary (DAG)
		var branches = deserializer.Deserialize<Dictionary<string, DtPipe.Core.Models.JobDefinition>>(content);
		if (branches == null || branches.Count == 0)
		{
			throw new InvalidOperationException("The job file is empty or invalid. A job file must define at least one named branch (DAG format).");
		}

		// Successfully loaded as a DAG. Now handle transformers for each branch.
		var rootMap = deserializer.Deserialize<Dictionary<string, object>>(content);
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
	/// Interpolates ${{VAR}} patterns with environment variable values or keyring secrets.
	/// </summary>
	internal static string InterpolateVariables(string content, DtPipe.Cli.Security.ISecretsManager? secretsManager)
	{
		return EnvVarPattern().Replace(content, match =>
		{
			var innerValue = match.Groups[1].Value.Trim();

			if (innerValue.StartsWith("keyring://", StringComparison.OrdinalIgnoreCase))
			{
				if (secretsManager != null)
				{
					try
					{
						var alias = innerValue["keyring://".Length..];
						var secretValue = secretsManager.GetSecret(alias);
						return secretValue ?? match.Value;
					}
					catch (Exception ex)
					{
						Console.Error.WriteLine($"Warning: Failed to resolve secret '{innerValue}': {ex.Message}");
						return match.Value;
					}
				}
				else
				{
					Console.Error.WriteLine($"Warning: Secret resolution requested for '{innerValue}' but no secrets manager is configured.");
					return match.Value;
				}
			}

			var envValue = Environment.GetEnvironmentVariable(innerValue);

			if (envValue is null)
			{
				Console.Error.WriteLine($"Warning: Environment variable '{innerValue}' is not set.");
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

	private sealed class InterpolatingNodeDeserializer : YamlDotNet.Serialization.INodeDeserializer
	{
		private readonly DtPipe.Cli.Security.ISecretsManager? _secretsManager;

		public InterpolatingNodeDeserializer(DtPipe.Cli.Security.ISecretsManager? secretsManager)
		{
			_secretsManager = secretsManager;
		}

		public bool Deserialize(YamlDotNet.Core.IParser parser, Type expectedType, Func<YamlDotNet.Core.IParser, Type, object?> nestedObjectDeserializer, out object? value, YamlDotNet.Serialization.ObjectDeserializer rootDeserializer)
		{
			if (expectedType == typeof(string) && parser.TryConsume<YamlDotNet.Core.Events.Scalar>(out var scalar))
			{
				value = InterpolateVariables(scalar.Value, _secretsManager);
				return true;
			}

			// For objects typed as object (like elements in Dictionary<string, object>), YamlDotNet's ObjectNodeDeserializer
			// normally reads scalars as strings. We want to intercept those too, but ONLY if the next event is a scalar.
			if (expectedType == typeof(object) && parser.TryConsume<YamlDotNet.Core.Events.Scalar>(out var objScalar))
			{
				value = InterpolateVariables(objScalar.Value, _secretsManager);
				return true;
			}

			value = null;
			return false;
		}
	}
}
