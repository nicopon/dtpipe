using System.CommandLine;
using System.CommandLine.Parsing;
using System.ComponentModel;
using System.Diagnostics.CodeAnalysis;
using System.Reflection;
using QueryDump.Core.Options;

namespace QueryDump.Cli;

public static class CliOptionBuilder
{
    /// <summary>
    /// Generates a list of System.CommandLine Options for the given option set type.
    /// </summary>
    public static IEnumerable<Option> GenerateOptions<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] T>() where T : class, IOptionSet, new()
    {
        var options = new List<Option>();
        var prefix = T.Prefix;
        var defaultInstance = new T(); // Used to get default values

        foreach (var property in typeof(T).GetProperties(BindingFlags.Public | BindingFlags.Instance))
        {
            var flagName = $"--{prefix}-{property.Name.ToKebabCase()}";
            var description = property.GetCustomAttribute<DescriptionAttribute>()?.Description ?? "";
            
            // Check for list/array types (repeatable options)
            var propType = property.PropertyType;
            var isList = propType != typeof(string) && typeof(System.Collections.IEnumerable).IsAssignableFrom(propType);

            if (isList)
            {
                // Handle List<string> etc.
                // Assuming currently only List<string> is used (FakeMappings)
                if (propType == typeof(IReadOnlyList<string>) || propType == typeof(List<string>) || propType == typeof(string[]))
                {
                    var option = new Option<string[]>(flagName, description)
                    {
                        Arity = ArgumentArity.ZeroOrMore, 
                        AllowMultipleArgumentsPerToken = true
                    };
                    options.Add(option);
                }
            }
            else
            {
                // Scalar types
                var defaultValue = property.GetValue(defaultInstance);
                
                // Create generic option dynamically
                var optionType = typeof(Option<>).MakeGenericType(GetUnderlyingType(propType));
                // Use constructor: Option(string name, string? description = null)
                var option = (Option)Activator.CreateInstance(optionType, flagName, description)!;

                // Set default value if not null
                if (defaultValue != null)
                {
                    option.SetDefaultValue(defaultValue);
                }
                
                options.Add(option);
            }
        }

        return options;
    }
    
    /// <summary>
    /// Binds parsed results back to an instance of T, using the provided list of options.
    /// </summary>
    public static T Bind<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] T>(ParseResult result, IEnumerable<Option> options) where T : class, IOptionSet, new()
    {
        var instance = new T();
        var prefix = T.Prefix;
        var optionsList = options.ToList();

        foreach (var property in typeof(T).GetProperties(BindingFlags.Public | BindingFlags.Instance))
        {
            var flagName = $"--{prefix}-{property.Name.ToKebabCase()}";
            var option = optionsList.FirstOrDefault(o => o.Name == flagName); 

            if (option != null)
            {
                // We use GetValueForOption using the non-generic Option base, which returns object?
                var value = result.GetValueForOption(option);
                
                if (value != null)
                {
                     // Convert list to IReadOnlyList if needed
                     // System.CommandLine returns Array for AllowMultipleArgumentsPerToken
                     if (property.PropertyType == typeof(IReadOnlyList<string>) && value is string[] arr)
                     {
                         property.SetValue(instance, arr);
                     }
                     else
                     {
                         property.SetValue(instance, value);
                     }
                }
            }
        }

        return instance;
    }

    private static Type GetUnderlyingType(Type t)
    {
        return Nullable.GetUnderlyingType(t) ?? t;
    }

    private static string ToKebabCase(this string str)
    {
        return string.Concat(str.Select((x, i) => i > 0 && char.IsUpper(x) ? "-" + x.ToString() : x.ToString())).ToLower();
    }
}
