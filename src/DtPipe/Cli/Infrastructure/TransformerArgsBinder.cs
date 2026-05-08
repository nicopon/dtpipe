using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using DtPipe.Core.Attributes;
using DtPipe.Cli.Pipeline;

namespace DtPipe.Cli.Infrastructure;

/// <summary>
/// Binds CLI (Option, Value) pairs to a transformer options instance using attribute-driven
/// property matching. Handles repeatable flags and collection-typed properties correctly,
/// eliminating the need for manual flag-string matching in each factory.
/// </summary>
internal static class TransformerArgsBinder
{
    internal static void Bind(object instance, IEnumerable<(string Option, string Value)> configuration)
    {
        var type = instance.GetType();
        var props = type.GetProperties(BindingFlags.Public | BindingFlags.Instance);
        var flagDefs = CliOptionBuilder.GenerateFlagDefsForType(type);

        var flagToProp = BuildFlagToPropertyMap(props, flagDefs, type);

        // Accumulate all values per property — required for repeatable flags (IEnumerable<string> etc.)
        var accumulated = new Dictionary<PropertyInfo, List<string>>();
        foreach (var (option, value) in configuration)
        {
            if (flagToProp.TryGetValue(option, out var prop))
            {
                if (!accumulated.TryGetValue(prop, out var list))
                    accumulated[prop] = list = new List<string>();
                list.Add(value);
            }
        }

        foreach (var (prop, values) in accumulated)
            SetProperty(instance, prop, values);
    }

    private static Dictionary<string, PropertyInfo> BuildFlagToPropertyMap(
        PropertyInfo[] props, IEnumerable<FlagDef> flagDefs, Type type)
    {
        var result = new Dictionary<string, PropertyInfo>(StringComparer.OrdinalIgnoreCase);
        foreach (var flag in flagDefs)
        {
            var prop = FindProperty(props, flag, type);
            if (prop == null) continue;
            result[flag.Name] = prop;
            foreach (var alias in flag.Aliases) result[alias] = prop;
        }
        return result;
    }

    // Resolves which property a FlagDef maps to, respecting explicit [ComponentOption] names.
    private static PropertyInfo? FindProperty(PropertyInfo[] props, FlagDef flag, Type type)
    {
        // 1. Exact match via [ComponentOption(name)] attribute
        foreach (var prop in props)
        {
            var attr = prop.GetCustomAttribute<ComponentOptionAttribute>();
            if (attr?.Name != null && string.Equals(attr.Name, flag.Name, StringComparison.OrdinalIgnoreCase))
                return prop;
        }

        // 2. Auto-generated name: prefix + kebab(propertyName)
        var prefixProp = type.GetProperty("Prefix", BindingFlags.Public | BindingFlags.Static | BindingFlags.FlattenHierarchy);
        var prefix = prefixProp?.GetValue(null) as string ?? "";

        foreach (var prop in props)
        {
            var kebab = prop.Name.ToKebabCase();
            var autoName = kebab == prefix.ToLowerInvariant()
                ? $"--{prefix.ToLowerInvariant()}"
                : string.IsNullOrEmpty(prefix)
                    ? $"--{kebab}"
                    : $"--{prefix.ToLowerInvariant()}-{kebab}";

            if (string.Equals(autoName, flag.Name, StringComparison.OrdinalIgnoreCase))
                return prop;
        }
        return null;
    }

    private static void SetProperty(object instance, PropertyInfo prop, List<string> values)
    {
        var propType = prop.PropertyType;
        var underlying = Nullable.GetUnderlyingType(propType) ?? propType;

        try
        {
            // Scalar types — use the last value (consistent with how flags override each other)
            if (underlying == typeof(string)) { prop.SetValue(instance, values.Last()); return; }
            if (underlying == typeof(bool)) { if (bool.TryParse(values.Last(), out var b)) prop.SetValue(instance, b); return; }
            if (underlying == typeof(int)) { if (int.TryParse(values.Last(), out var i)) prop.SetValue(instance, i); return; }
            if (underlying == typeof(double)) { if (double.TryParse(values.Last(), System.Globalization.CultureInfo.InvariantCulture, out var d)) prop.SetValue(instance, d); return; }
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
            Console.Error.WriteLine($"Warning: TransformerArgsBinder could not bind '{prop.Name}': {ex.Message}");
        }
    }

    private static Type? GetElementType(Type type)
    {
        if (type.IsArray) return type.GetElementType();
        if (type.IsGenericType) return type.GetGenericArguments().FirstOrDefault();
        return null;
    }
}
