using System.Globalization;
using System.Reflection;
using DtPipe.Core.Pipelines;

namespace DtPipe.Cli.Pipeline;

/// <summary>
/// Exports a CLI-bound options object back into the canonical YAML shapes consumed by
/// the job-file load path (TransformerConfig for transformers, provider-options
/// dictionaries for adapters). Only properties whose value differs from a fresh
/// default instance are emitted, so generated YAML stays minimal and stable.
/// </summary>
public static class OptionObjectExporter
{
    /// <summary>
    /// Options type → name of the property holding the transformer's primary values
    /// (emitted as <c>mappings:</c> entries by splitting each string on its first ':').
    /// All other changed properties go to <c>options:</c>.
    /// </summary>
    private static readonly Dictionary<Type, string> PrimaryMappingProperty = new()
    {
        [typeof(DtPipe.Transformers.Arrow.Fake.FakeOptions)] = "Fake",
        [typeof(DtPipe.Transformers.Arrow.Filter.FilterOptions)] = "Filters",
        [typeof(DtPipe.Transformers.Row.Compute.ComputeOptions)] = "Compute",
        [typeof(DtPipe.Transformers.Arrow.Null.NullOptions)] = "Columns",
        [typeof(DtPipe.Transformers.Arrow.Overwrite.OverwriteOptions)] = "Overwrite",
        [typeof(DtPipe.Transformers.Arrow.Format.FormatOptions)] = "Format",
        [typeof(DtPipe.Transformers.Arrow.Mask.MaskOptions)] = "Mask",
        [typeof(DtPipe.Transformers.Row.Expand.ExpandOptions)] = "Expand",
    };

    /// <summary>
    /// The property <paramref name="optionsType"/> encodes as <c>mappings:</c>, or null when it has
    /// none. The help asks so it does not list that property as an <c>options:</c> key: setting it
    /// there binds but changes nothing, because the factory decides it has no work before the
    /// options block is read. Both surfaces reading one map is what keeps them from disagreeing.
    /// </summary>
    public static string? PrimaryMappingPropertyFor(Type optionsType)
        => PrimaryMappingProperty.TryGetValue(optionsType, out var name) ? name : null;

    /// <summary>
    /// Builds a TransformerConfig from a bound transformer options instance.
    /// Returns null when nothing differs from defaults (nothing to export).
    /// </summary>
    public static TransformerConfig? ExportTransformerConfig(string componentName, object boundOptions)
    {
        var changed = CollectChanged(boundOptions);

        // The primary list property is re-encoded as mappings entries ("COL:value" split
        // on the first ':'), matching the shape every CreateFromYamlConfig consumes.
        Dictionary<string, string>? mappings = null;
        if (PrimaryMappingProperty.TryGetValue(boundOptions.GetType(), out var primaryName))
        {
            changed.Remove(primaryName.ToKebabCase());
            if (boundOptions.GetType().GetProperty(primaryName)?.GetValue(boundOptions) is System.Collections.IEnumerable list and not string)
            {
                mappings = new Dictionary<string, string>();
                foreach (var entry in list)
                {
                    var s = entry?.ToString() ?? "";
                    var sep = s.IndexOf(':');
                    mappings[sep > 0 ? s[..sep].Trim() : s] = sep > 0 ? s[(sep + 1)..].Trim() : "";
                }
            }
        }

        if (changed.Count == 0 && mappings is null or { Count: 0 }) return null;

        return new TransformerConfig
        {
            Type = componentName,
            Mappings = mappings is { Count: > 0 } ? mappings : null,
            Options = changed.Count > 0 ? changed : null
        };
    }

    /// <summary>
    /// Extracts the properties of <paramref name="boundOptions"/> that differ from a fresh
    /// default instance, keyed by kebab-case property name with invariant string values.
    /// </summary>
    public static Dictionary<string, string> CollectChanged(object boundOptions)
    {
        var result = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        var type = boundOptions.GetType();
        var defaults = TryCreateDefault(type);

        foreach (var prop in type.GetProperties(BindingFlags.Public | BindingFlags.Instance))
        {
            if (!prop.CanRead) continue;
            var value = prop.GetValue(boundOptions);
            var defaultValue = GetDefaultPropertyValue(prop, defaults);
            if (ValuesEqual(value, defaultValue)) continue;

            var serialized = Stringify(value);
            if (serialized is not null)
                result[prop.Name.ToKebabCase()] = serialized;
        }
        return result;
    }

    // Reflection helper: reads a property from the default instance by name.
    private static object? GetDefaultPropertyValue(PropertyInfo prop, object? defaults)
        => defaults?.GetType().GetProperty(prop.Name)?.GetValue(defaults);

    private static object? TryCreateDefault(Type type)
    {
        try { return Activator.CreateInstance(type); }
        catch { return null; }
    }

    private static bool ValuesEqual(object? a, object? b)
    {
        if (a is null && b is null) return true;
        if (a is null || b is null) return false;

        if (a is System.Collections.IEnumerable ea && a is not string
            && b is System.Collections.IEnumerable eb && b is not string)
        {
            return ea.Cast<object?>().SequenceEqual(eb.Cast<object?>());
        }
        return Equals(a, b);
    }

    private static string? Stringify(object? value)
    {
        switch (value)
        {
            case null: return null;
            case string s: return s;
            case bool b: return b ? "true" : "false";
            case Enum e: return e.ToString();
            // Before the IEnumerable case: a dictionary enumerates as KeyValuePair, whose ToString
            // renders "[k, v]" — a shape nothing reads back, so the option was lost on re-import.
            case System.Collections.IDictionary map:
                return string.Join(",", map.Keys.Cast<object?>().Select(k => $"{k}:{map[k!]}"));
            case System.Collections.IEnumerable list when value is not string:
                var items = list.Cast<object?>().Select(v => v?.ToString() ?? "");
                return string.Join(",", items);
            case IFormattable f: return f.ToString(null, CultureInfo.InvariantCulture);
            default: return value.ToString();
        }
    }
}
