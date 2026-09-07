using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using DtPipe.Core.Attributes;
using DtPipe.Core.Options;

namespace DtPipe.Cli.Pipeline;

/// <summary>
/// The single option binder (F8). One implementation serves all binding surfaces:
/// - <see cref="BindCli"/>: CLI tokens + FlagRegistry (replaces the legacy CLI binder)
/// - <see cref="BindPairs"/>: pre-extracted (flag, value) transformer groups (replaces the legacy transformer args binder)
/// - <see cref="BindYaml"/>: YAML provider-options dictionaries (replaces the legacy YAML configuration binder)
///
/// All surfaces resolve property names through <see cref="FlagNameDeriver"/>, share the
/// arity-driven value-token rule (<see cref="FlagDef.ConsumesNextToken"/>), and enforce
/// [ComponentOption(Required)] via <see cref="EnforceRequired"/> when asked.
/// </summary>
public static class OptionBinder
{
    // ─────────────────────────────────────────────────────────────────────────
    // CLI surface
    // ─────────────────────────────────────────────────────────────────────────

    public static void BindCli(object target, string[] args, FlagRegistry registry, string prefix = "", bool strict = false)
    {
        var type = target.GetType();
        var props = type.GetProperties(BindingFlags.Public | BindingFlags.Instance);
        // Inverse of flag-def generation: canonical name (+ explicit aliases) → property.
        var flagMap = BuildFlagToPropertyMap(props, type);

        for (int i = 0; i < args.Length; i++)
        {
            var token = args[i];
            if (!token.StartsWith('-')) continue;

            var def = registry.Lookup(token);
            if (def == null)
            {
                if (strict)
                    throw new InvalidOperationException(
                        $"Unrecognized flag '{token}' for component '{(string.IsNullOrEmpty(prefix) ? type.Name : prefix)}'. " +
                        "Check the provider prefix and flag spelling (see 'dtpipe --help'), or remove --strict-bindings to skip unknown flags.");
                continue;
            }

            string? value;
            if (!def.ConsumesNextToken)
            {
                value = "true";
            }
            else
            {
                // Arity-driven consumption (F8): a scalar/repeatable flag always consumes
                // the token that follows it as its value — no shape sniffing on the token.
                value = i + 1 < args.Length ? args[++i] : null;
                if (value == null) continue;
            }

            if (!flagMap.TryGetValue(def.Name, out var prop))
            {
                if (strict)
                    throw new InvalidOperationException(
                        $"Flag '{def.Name}' could not be bound to any property of '{type.Name}'. " +
                        "The flag exists in the registry but does not map to this options type.");
                continue;
            }

            SetValue(target, prop, value, strict);
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Transformer group surface — (flag, value) pairs already extracted by the group walker
    // ─────────────────────────────────────────────────────────────────────────

    public static void BindPairs(object target, IEnumerable<(string Option, string Value)> pairs, bool strict = false)
    {
        var type = target.GetType();
        var props = type.GetProperties(BindingFlags.Public | BindingFlags.Instance);
        var flagToProp = BuildFlagToPropertyMap(props, type);

        // Accumulate all values per property — required for repeatable flags.
        var accumulated = new Dictionary<PropertyInfo, List<string>>();
        foreach (var (option, value) in pairs)
        {
            if (!flagToProp.TryGetValue(option, out var prop))
            {
                if (strict)
                    throw new InvalidOperationException(
                        $"Unrecognized option '{option}' for options type '{type.Name}'.");
                continue;
            }
            if (!accumulated.TryGetValue(prop, out var list))
                accumulated[prop] = list = new List<string>();
            list.Add(value);
        }

        foreach (var (prop, values) in accumulated)
            SetProperty(target, prop, values, strict);
    }

    private static Dictionary<string, PropertyInfo> BuildFlagToPropertyMap(PropertyInfo[] props, Type type)
    {
        var metadata = GetMetadataMap(type);
        var result = new Dictionary<string, PropertyInfo>(StringComparer.OrdinalIgnoreCase);
        foreach (var prop in props)
        {
            var canonical = FlagNameDeriver.DeriveCanonical(prop, type, metadata);
            result[canonical] = prop;
            var attr = prop.GetCustomAttribute<ComponentOptionAttribute>();
            if (attr?.Aliases != null)
                foreach (var alias in attr.Aliases)
                    result[alias] = prop;
        }
        return result;
    }

    private static IReadOnlyDictionary<string, string>? GetMetadataMap(Type type)
    {
        try
        {
            var instance = Activator.CreateInstance(type);
            return (instance as ICliOptionMetadata)?.PropertyToFlag;
        }
        catch
        {
            return null;
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // YAML surface
    // ─────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Binds a provider-options dictionary to an existing instance. Keys are normalized
    /// (strip '-', '_', lowercase); unmapped keys warn (or throw in strict mode).
    /// When <paramref name="ignoreUnknownKeys"/> is set, unmapped keys are skipped silently —
    /// used for shared provider-options blocks (e.g. a plain "csv:" key feeding both the
    /// reader and the writer) where some keys legitimately target only one side.
    /// </summary>
    public static void BindYaml(object target, IReadOnlyDictionary<string, object?> config, bool strict = false, bool ignoreUnknownKeys = false)
    {
        if (config == null || config.Count == 0 || target == null) return;

        var properties = target.GetType().GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Where(p => p.CanWrite)
            .ToList();

        foreach (var kvp in config)
        {
            var key = kvp.Key;
            var value = kvp.Value;

            var prop = FindProperty(properties, key);

            if (prop == null)
            {
                if (ignoreUnknownKeys)
                    continue;
                var message = DescribeUnknownKey(target.GetType(), key);
                if (strict)
                    throw new InvalidOperationException(message);
                Console.Error.WriteLine($"[dtpipe] Warning: {message}");
                continue;
            }

            try
            {
                var convertedValue = ConvertValue(value, prop.PropertyType);
                prop.SetValue(target, convertedValue);
            }
            catch (Exception ex)
            {
                var message = $"Failed to bind provider option '{key}' to property '{prop.Name}': {ex.Message}";
                if (strict)
                    throw new InvalidOperationException(message, ex);
                Console.Error.WriteLine($"[dtpipe] Warning: {message}");
            }
        }
    }

    private static string NormalizeKey(string key)
        => key.Replace("-", "").Replace("_", "").ToLowerInvariant();

    private static PropertyInfo? FindProperty(IEnumerable<PropertyInfo> properties, string key)
    {
        var normalized = NormalizeKey(key);
        return properties.FirstOrDefault(p => NormalizeKey(p.Name) == normalized);
    }

    /// <summary>
    /// Whether <paramref name="key"/> names an option of <paramref name="optionsType"/> that would
    /// bind. Documented examples are checked against this: a key published in a component's help
    /// that binds nothing teaches a call that silently keeps every default.
    /// </summary>
    public static bool Binds(Type optionsType, string key)
        => FindProperty(optionsType.GetProperties(BindingFlags.Public | BindingFlags.Instance).Where(p => p.CanWrite), key) is not null;

    /// <summary>
    /// Why <paramref name="key"/> did not bind, and what to write instead.
    ///
    /// <para>
    /// A YAML provider-option key is the property name, which is not always the command-line flag:
    /// the generator's throttle is <c>--throttle</c> on the CLI and <c>rows-per-second</c> in YAML,
    /// and nineteen options across the catalogue diverge the same way. Writing the flag binds
    /// nothing, and a run that silently keeps a default is the one kind of mistake that leaves no
    /// trace to read afterwards — so the refusal names the key that would have worked, and says
    /// when the one that was written is the flag for it.
    /// </para>
    /// </summary>
    public static string DescribeUnknownKey(Type optionsType, string key)
    {
        var normalized = NormalizeKey(key);
        var properties = optionsType.GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Where(p => p.CanWrite).ToList();

        var byFlag = properties.FirstOrDefault(p =>
            p.GetCustomAttribute<ComponentOptionAttribute>() is { Name: { } flag }
            && NormalizeKey(flag.TrimStart('-')) == normalized);

        if (byFlag is not null)
            return $"Unrecognized provider option '{key}' for '{optionsType.Name}'. "
                 + $"'{key}' is the command-line flag; in YAML the key is '{ToYamlKey(byFlag.Name)}'.";

        var closest = properties
            .Select(p => (Key: ToYamlKey(p.Name), Distance: Distance(normalized, NormalizeKey(p.Name))))
            .Where(x => x.Distance <= Math.Max(2, normalized.Length / 3))
            .OrderBy(x => x.Distance)
            .Select(x => x.Key)
            .FirstOrDefault();

        var suffix = closest is not null
            ? $" Did you mean '{closest}'?"
            : $" Valid keys: {string.Join(", ", properties.Select(p => ToYamlKey(p.Name)).OrderBy(k => k, StringComparer.Ordinal))}.";

        return $"Unrecognized provider option '{key}' for '{optionsType.Name}'.{suffix}";
    }

    /// <summary>A property name as the YAML key it binds from — PascalCase to kebab-case.</summary>
    private static string ToYamlKey(string propertyName)
        => string.Concat(propertyName.Select((c, i) =>
            char.IsUpper(c) && i > 0 ? "-" + char.ToLowerInvariant(c) : char.ToLowerInvariant(c).ToString()));

    private static int Distance(string a, string b)
    {
        var previous = Enumerable.Range(0, b.Length + 1).ToArray();
        var current = new int[b.Length + 1];
        for (int i = 1; i <= a.Length; i++)
        {
            current[0] = i;
            for (int j = 1; j <= b.Length; j++)
                current[j] = Math.Min(Math.Min(previous[j] + 1, current[j - 1] + 1),
                                      previous[j - 1] + (a[i - 1] == b[j - 1] ? 0 : 1));
            (previous, current) = (current, previous);
        }
        return previous[b.Length];
    }

    private static object? ConvertValue(object? value, Type targetType)
    {
        if (value == null) return null;

        var underlyingType = Nullable.GetUnderlyingType(targetType) ?? targetType;

        if (underlyingType.IsInstanceOfType(value))
            return value;

        var stringValue = value.ToString();
        if (string.IsNullOrEmpty(stringValue)) return null;

        if (underlyingType.IsEnum)
            return Enum.Parse(underlyingType, stringValue, ignoreCase: true);

        if (underlyingType == typeof(Guid))
            return Guid.Parse(stringValue);

        if (underlyingType == typeof(TimeSpan))
            return TimeSpan.Parse(stringValue);

        // A YAML transformer option arrives flattened to a string (TransformerConfig.Options is
        // Dictionary<string,string>), so the two shapes the CLI already accepts must be readable
        // from one: a comma-separated list, and comma-separated "key:value" pairs.
        if (underlyingType == typeof(Dictionary<string, string>))
        {
            var pairs = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            foreach (var entry in SplitList(stringValue))
            {
                var sep = entry.IndexOf(':');
                if (sep > 0) pairs[entry[..sep].Trim()] = entry[(sep + 1)..].Trim();
                else pairs[entry] = string.Empty;
            }
            return pairs;
        }

        if (GetElementType(underlyingType) == typeof(string))
        {
            var items = SplitList(stringValue);
            return underlyingType.IsArray ? items.ToArray() : items;
        }

        return Convert.ChangeType(value, underlyingType);
    }

    private static List<string> SplitList(string value)
        => value.Split(',').Select(v => v.Trim()).Where(v => v.Length > 0).ToList();

    // ─────────────────────────────────────────────────────────────────────────
    // Required enforcement + value assignment
    // ─────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Checks every [ComponentOption(Required = true)] property of the target and reports
    /// unset ones: strict throws listing offenders, otherwise a warning is emitted.
    /// Not invoked automatically by the bind methods — call sites opt in where a missing
    /// required option is genuinely fatal at that point in the pipeline setup.
    /// </summary>
    public static void EnforceRequired(object target, bool strict)
    {
        var offenders = target.GetType().GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Where(p => p.GetCustomAttribute<ComponentOptionAttribute>() is { Required: true })
            .Where(p => IsUnset(p.GetValue(target)))
            .Select(p => p.Name)
            .ToList();

        if (offenders.Count == 0) return;

        var message = $"Required option(s) missing on {target.GetType().Name}: {string.Join(", ", offenders)}";
        if (strict)
            throw new InvalidOperationException(message);
        Console.Error.WriteLine($"[dtpipe] Warning: {message}");
    }

    private static bool IsUnset(object? value)
        => value is null
           || (value is string s && s.Length == 0)
           || (value is not string && !(value is System.Collections.IEnumerable) && Equals(value, Activator.CreateInstance(Nullable.GetUnderlyingType(value.GetType()) ?? value.GetType())));

    private static void SetValue(object target, PropertyInfo prop, string value, bool strict)
    {
        try
        {
            var type = Nullable.GetUnderlyingType(prop.PropertyType) ?? prop.PropertyType;
            if (type == typeof(string)) prop.SetValue(target, value);
            else if (type == typeof(bool)) prop.SetValue(target, bool.Parse(value));
            else if (type == typeof(int)) prop.SetValue(target, int.Parse(value));
            else if (type == typeof(double)) prop.SetValue(target, double.Parse(value));
            else if (type.IsEnum) prop.SetValue(target, Enum.Parse(type, value, true));
        }
        catch (Exception ex) when (ex is FormatException or InvalidCastException or OverflowException or ArgumentException)
        {
            if (strict)
                throw new InvalidOperationException(
                    $"Failed to bind value '{value}' to option '{prop.Name}': {ex.Message}", ex);
            Console.Error.WriteLine($"Warning: OptionBinder could not bind '{prop.Name}': {ex.Message}");
        }
    }

    private static void SetProperty(object instance, PropertyInfo prop, List<string> values, bool strict)
    {
        var propType = prop.PropertyType;
        var underlying = Nullable.GetUnderlyingType(propType) ?? propType;

        try
        {
            // Scalar types — use the last value (consistent with how flags override each other)
            if (underlying == typeof(string)) { prop.SetValue(instance, values.Last()); return; }
            if (underlying == typeof(bool)) { if (bool.TryParse(values.Last(), out var b)) prop.SetValue(instance, b); return; }
            if (underlying == typeof(int)) { if (int.TryParse(values.Last(), out var i)) prop.SetValue(instance, i); return; }
            if (underlying == typeof(double)) { if (double.TryParse(values.Last(), out var d)) prop.SetValue(instance, d); return; }
            if (underlying.IsEnum) { prop.SetValue(instance, Enum.Parse(underlying, values.Last(), ignoreCase: true)); return; }

            // Dictionary<string, string>: each value is "key:value"
            if (underlying == typeof(Dictionary<string, string>))
            {
                var dict = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                foreach (var v in values)
                {
                    var sep = v.IndexOf(':');
                    if (sep > 0) dict[v[..sep].Trim()] = v[(sep + 1)..].Trim();
                    else dict[v.Trim()] = string.Empty;
                }
                prop.SetValue(instance, dict);
                return;
            }

            // String collection types — use all values
            var elementType = GetElementType(propType);
            if (elementType == typeof(string))
            {
                if (propType == typeof(string[]) || propType.IsArray)
                    prop.SetValue(instance, values.ToArray());
                else
                    prop.SetValue(instance, values); // Assignable to IEnumerable<string>, IReadOnlyList<string>, List<string>
                return;
            }
        }
        catch (Exception ex) when (ex is FormatException or InvalidCastException or OverflowException or ArgumentException)
        {
            if (strict)
                throw new InvalidOperationException(
                    $"Failed to bind value '{values.Last()}' to option '{prop.Name}': {ex.Message}", ex);
            Console.Error.WriteLine($"Warning: OptionBinder could not bind '{prop.Name}': {ex.Message}");
        }
    }

    private static Type? GetElementType(Type type)
    {
        if (type.IsArray) return type.GetElementType();
        if (type.IsGenericType) return type.GetGenericArguments().FirstOrDefault();
        return null;
    }
}
