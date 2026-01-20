using System.Text.RegularExpressions;

namespace QueryDump.Transformers.Fake;

/// <summary>
/// Parses --fake option mappings in format COLUMN:faker.method or COLUMN:{OTHER_COLUMN}.
/// Validates faker paths against FakerRegistry.
/// </summary>
public sealed partial class FakeMappingParser
{
    private readonly FakerRegistry _registry;
    private readonly Dictionary<string, string> _mappings = new(StringComparer.OrdinalIgnoreCase);

    // Regex to match {COLUMN_NAME} patterns - unified syntax
    [GeneratedRegex(@"\{([^{}]+)\}", RegexOptions.Compiled)]
    private static partial Regex TemplatePattern();

    public FakeMappingParser(FakerRegistry registry)
    {
        _registry = registry;
    }

    /// <summary>
    /// Gets the parsed mappings (column name -> faker path or template).
    /// </summary>
    public IReadOnlyDictionary<string, string> Mappings => _mappings;

    /// <summary>
    /// Indicates whether any mappings are configured.
    /// </summary>
    public bool HasMappings => _mappings.Count > 0;

    /// <summary>
    /// Parses a mapping string in format COLUMN:value.
    /// </summary>
    public void Parse(string mapping)
    {
        // Format: COLUMN:dataset.method or COLUMN:{OTHER_COLUMN} template
        var separatorIndex = mapping.IndexOf(':');
        if (separatorIndex <= 0 || separatorIndex >= mapping.Length - 1)
        {
            Console.Error.WriteLine($"Warning: Invalid mapping format '{mapping}'. Expected 'COLUMN:value'");
            return;
        }

        var column = mapping[..separatorIndex].Trim();
        var value = mapping[(separatorIndex + 1)..].Trim();

        if (string.IsNullOrEmpty(column) || string.IsNullOrEmpty(value))
        {
            Console.Error.WriteLine($"Warning: Invalid mapping '{mapping}'. Column and value cannot be empty.");
            return;
        }

        // For templates, store as-is
        if (IsTemplate(value))
        {
            _mappings[column] = value;
            return;
        }

        // For fakers/strings, apply validation logic
        // Extract variant suffix (#xxx) if present - used for same-faker different values
        var hashIndex = value.IndexOf('#');
        var baseFakerPath = hashIndex >= 0 ? value[..hashIndex] : value;
        var variant = hashIndex >= 0 ? value[(hashIndex + 1)..] : null;
        
        var parts = baseFakerPath.Split('.', 2);
        var datasetName = parts.Length > 0 ? parts[0] : string.Empty;

        if (_registry.HasDataset(datasetName))
        {
            if (!_registry.HasGenerator(baseFakerPath))
            {
                throw new InvalidOperationException($"Unknown faker method '{baseFakerPath}' for dataset '{datasetName}'. Use --fake-list to see available options.");
            }
            // Store full path including variant for distinct hashing
            _mappings[column] = value;
        }
        else if (value.Contains(':'))
        {
             // Fallback: User might have used colon instead of dot (e.g. "finance:iban")
             var normalized = baseFakerPath.Replace(':', '.');
             if (_registry.HasGenerator(normalized))
             {
                 // Keep variant if present
                 _mappings[column] = variant is not null ? $"{normalized}#{variant}" : normalized;
             }
             else
             {
                 // Not a known faker even after normalization, treat as string
                 _mappings[column] = value;
             }
        }
        else
        {
            // Hardcoded string
            _mappings[column] = value;
        }
    }

    /// <summary>
    /// Parses multiple mappings.
    /// </summary>
    public void ParseAll(IEnumerable<string>? mappings)
    {
        if (mappings is null) return;
        foreach (var mapping in mappings)
        {
            Parse(mapping);
        }
    }

    /// <summary>
    /// Determines if a value is a template (contains {COLUMN} references).
    /// </summary>
    public static bool IsTemplate(string value) => TemplatePattern().IsMatch(value);

    /// <summary>
    /// Extracts referenced column names from a template.
    /// </summary>
    public static HashSet<string> ExtractReferencedColumns(string template)
    {
        var matches = TemplatePattern().Matches(template);
        var result = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (Match match in matches)
        {
            result.Add(match.Groups[1].Value);
        }
        return result;
    }
}
