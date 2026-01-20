using System.Text.RegularExpressions;
using YamlDotNet.Serialization;
using YamlDotNet.Serialization.NamingConventions;

namespace QueryDump.Configuration;

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
        if (string.IsNullOrWhiteSpace(yamlJob.Query))
            throw new InvalidOperationException("Job file missing required field: query");
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
            Transformers = ParseTransformers(yamlJob.Transformers)
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

    private static List<TransformerConfig>? ParseTransformers(List<Dictionary<string, object>>? transformers)
    {
        if (transformers is null || transformers.Count == 0)
            return null;

        var result = new List<TransformerConfig>();

        foreach (var transformer in transformers)
        {
            // Each transformer is a dict with a single key (the type)
            foreach (var (type, value) in transformer)
            {
                var config = new TransformerConfig
                {
                    Type = type,
                    Mappings = ParseMappings(value),
                    Options = null // Could be extended later
                };
                result.Add(config);
            }
        }

        return result;
    }

    private static Dictionary<string, string>? ParseMappings(object? value)
    {
        if (value is null) return null;

        if (value is Dictionary<object, object> dict)
        {
            return dict.ToDictionary(
                kvp => kvp.Key.ToString()!,
                kvp => kvp.Value?.ToString() ?? string.Empty
            );
        }

        return null;
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
        public int? ConnectionTimeout { get; set; }
        public int? QueryTimeout { get; set; }
        public List<Dictionary<string, object>>? Transformers { get; set; }
    }
}
