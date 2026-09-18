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
	/// <param name="filePath">Path to the YAML file or a memory:// job URL.</param>
	/// <param name="secretsManager">Optional secrets manager to resolve keyring:// references.</param>
	/// <param name="interpolate">
	/// False loads the job as written, leaving every <c>${{…}}</c> token in place. A caller that is
	/// going to WRITE the job back out passes false; one that is going to RUN it does not.
	/// </param>
	/// <returns>Dictionary of JobDefinitions keyed by branch alias.</returns>
	public static Dictionary<string, DtPipe.Core.Models.JobDefinition> Parse(string filePath, DtPipe.Cli.Security.ISecretsManager? secretsManager = null, bool interpolate = true)
	{
		string content;

		if (filePath.StartsWith("memory://", StringComparison.OrdinalIgnoreCase))
		{
			var name = filePath.Substring("memory://".Length).Trim();
			var tempPath = Path.Combine(Path.GetTempPath(), "dtpipe-job-" + name + ".yaml");
			try
			{
				if (!File.Exists(tempPath))
				{
					throw new FileNotFoundException($"Memory job file not found at: {tempPath}");
				}
				content = File.ReadAllText(tempPath);
			}
			catch (Exception ex)
			{
				throw new FileNotFoundException($"Failed to read memory job '{name}': {ex.Message}", ex);
			}
		}
		else
		{
			if (!File.Exists(filePath))
			{
				throw new FileNotFoundException($"Job file not found: {filePath}");
			}
			content = File.ReadAllText(filePath);
		}

		return ParseContent(content, secretsManager, interpolate);
	}

	/// <summary>
	/// Parses a YAML job content string into a dictionary of JobDefinitions (DAG).
	/// </summary>
	public static Dictionary<string, DtPipe.Core.Models.JobDefinition> ParseContent(string content, DtPipe.Cli.Security.ISecretsManager? secretsManager = null, bool interpolate = true)
	{
		// YamlDotNet wraps the real cause — the property it could not match — in a YamlException
		// whose own Message is the useless "Exception during deserialization". The unwrap belongs
		// here, on the one method both entry points reach: posted on the file path alone, it left
		// 'dry-run' and 'get-dag-topology' answering an unknown key with the bare property name
		// while 'validate-yaml-job' named the key, its line and the legal key list — the same YAML,
		// the same server, two answers.
		try
		{
			var jobs = Deserialize(content, secretsManager, interpolate);

			// Reported here because this is the last place that still holds the job as written: the
			// cursor:// token is replaced during deserialization, so afterwards there is no way to
			// tell a job that filters from one that only tracks.
			foreach (var advisory in DtPipe.Cli.Incremental.CursorAdvisory.Advise(jobs, content))
				Console.Error.WriteLine($"[dtpipe] Warning: {advisory}");

			return jobs;
		}
		catch (YamlException ex)
		{
			throw new InvalidOperationException(DtPipe.Cli.Mcp.ToolError.Describe(ex, content), ex);
		}
	}

	private static Dictionary<string, DtPipe.Core.Models.JobDefinition> Deserialize(string content, DtPipe.Cli.Security.ISecretsManager? secretsManager, bool interpolate)
	{
		// A key this loader does not know is refused, not dropped. Three defects of one shape have
		// been found this way — 'columns:' on a project transformer, an 'options:' block that built
		// nothing, and 'providers:' for 'provider-options:' — each silently discarded, each leaving
		// a job that validated and did not do what it said. The parser reports the line, which
		// ToolError then quotes back to the caller.
		var deserializer = new DeserializerBuilder()
			.WithNamingConvention(HyphenatedNamingConvention.Instance)
			.WithNodeDeserializer(new InterpolatingNodeDeserializer(secretsManager, interpolate), s => s.OnTop())
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
	/// Interpolates ${{VAR}} patterns through the canonical resolver chain (F11):
	/// env → keyring → cursor, exactly the engine used for CLI connection strings.
	/// The compiled regex stays only as the matcher for ${{...}} tokens.
	/// </summary>
	internal static string InterpolateVariables(string content, DtPipe.Cli.Security.ISecretsManager? secretsManager)
	{
		var interpolators = new List<DtPipe.Core.Expressions.IStringInterpolator>
		{
			new DtPipe.Cli.Incremental.CursorInterpolator(),
		};
		if (secretsManager != null)
			interpolators.Insert(0, new DtPipe.Cli.Security.KeyringInterpolator(secretsManager));
		interpolators.Add(new DtPipe.Cli.Expressions.EnvVarInterpolator());

		var resolver = new DtPipe.Cli.Expressions.CompositeStringContentResolver(interpolators);
		return resolver.ResolveAsync(content).GetAwaiter().GetResult() ?? content;
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
	/// Resolves <c>${{…}}</c> in every scalar as it is read.
	/// </summary>
	/// <remarks>
	/// It stays registered when <c>_interpolate</c> is false, and returns the scalar verbatim. It is
	/// also what forces every scalar to a string: dropping it would hand <c>object</c>-typed values
	/// — a provider-options block — to YamlDotNet's own inference, so the way to load a job as
	/// written is to keep this seam and neutralise it, not to remove it.
	/// </remarks>
	private sealed class InterpolatingNodeDeserializer : YamlDotNet.Serialization.INodeDeserializer
	{
		private readonly DtPipe.Cli.Security.ISecretsManager? _secretsManager;
		private readonly bool _interpolate;

		public InterpolatingNodeDeserializer(DtPipe.Cli.Security.ISecretsManager? secretsManager, bool interpolate)
		{
			_secretsManager = secretsManager;
			_interpolate = interpolate;
		}

		public bool Deserialize(YamlDotNet.Core.IParser parser, Type expectedType, Func<YamlDotNet.Core.IParser, Type, object?> nestedObjectDeserializer, out object? value, YamlDotNet.Serialization.ObjectDeserializer rootDeserializer)
		{
			if (expectedType == typeof(string) && parser.TryConsume<YamlDotNet.Core.Events.Scalar>(out var scalar))
			{
				value = _interpolate ? InterpolateVariables(scalar.Value, _secretsManager) : scalar.Value;
				return true;
			}

			// For objects typed as object (like elements in Dictionary<string, object>), YamlDotNet's ObjectNodeDeserializer
			// normally reads scalars as strings. We want to intercept those too, but ONLY if the next event is a scalar.
			if (expectedType == typeof(object) && parser.TryConsume<YamlDotNet.Core.Events.Scalar>(out var objScalar))
			{
				value = _interpolate ? InterpolateVariables(objScalar.Value, _secretsManager) : objScalar.Value;
				return true;
			}

			value = null;
			return false;
		}
	}
}
