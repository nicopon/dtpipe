using System;
using System.Collections.Generic;
using System.Reflection;
using System.Linq;

namespace DtPipe.Cli.Pipeline;

public static class FlagBinder
{
    public static void Bind(object target, string[] args, FlagRegistry registry, string prefix = "")
    {
        var type = target.GetType();
        var props = type.GetProperties(BindingFlags.Public | BindingFlags.Instance);
        
        for (int i = 0; i < args.Length; i++)
        {
            var token = args[i];
            if (!token.StartsWith('-')) continue;

            var def = registry.Lookup(token);
            if (def == null) continue;

            string? value = null;
            if (def.Arity != FlagArity.Boolean)
            {
                if (i + 1 < args.Length && !args[i+1].StartsWith('-'))
                {
                    value = args[++i];
                }
            }
            else
            {
                value = "true";
            }

            if (value == null) continue;

            foreach (var prop in props)
            {
                if (Match(prop, def.Name, def.Aliases, prefix))
                {
                    SetValue(target, prop, value);
                }
            }
        }
    }

    private static bool Match(PropertyInfo prop, string flagName, string[] aliases, string prefix)
    {
        var kebab = prop.Name.ToKebabCase();
        var names = new List<string> { $"--{kebab}" };
        if (!string.IsNullOrEmpty(prefix))
        {
            names.Add($"--{prefix.ToLowerInvariant()}-{kebab}");
            if (kebab == prefix.ToLowerInvariant()) names.Add($"--{prefix.ToLowerInvariant()}");
        }
        
        return names.Any(n => n.Equals(flagName, StringComparison.OrdinalIgnoreCase)) ||
               aliases.Any(a => names.Any(n => n.Equals(a, StringComparison.OrdinalIgnoreCase)));
    }

    private static void SetValue(object target, PropertyInfo prop, string value)
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
        catch { /* skip errors */ }
    }
}

public static class StringExtensions
{
    public static string ToKebabCase(this string str)
    {
        return string.Concat(str.Select((x, i) => i > 0 && char.IsUpper(x) ? "-" + x.ToString() : x.ToString())).ToLower();
    }
}
