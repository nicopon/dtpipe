using System.CommandLine;
using System.CommandLine.Parsing;
using System.ComponentModel;
using System.Reflection;
using QueryDump.Core.Options;
using QueryDump.Core.Attributes;

namespace QueryDump.Cli;

public static class CliOptionBuilder
{
    /// <summary>
    /// Generates a list of System.CommandLine Options for the given option set type.
    /// </summary>
    public static IEnumerable<Option> GenerateOptions<T>() where T : class, IOptionSet, new()
    {
        var options = new List<Option>();
        var prefix = T.Prefix;
        var defaultInstance = new T(); // Used to get default values

        foreach (var property in typeof(T).GetProperties(BindingFlags.Public | BindingFlags.Instance))
        {
            var cliOptionAttr = property.GetCustomAttribute<CliOptionAttribute>();
            var descriptionAttr = property.GetCustomAttribute<DescriptionAttribute>();
            
            // Skip properties without [Description] OR [CliOption] attribute
            if (cliOptionAttr is null && descriptionAttr is null)
            {
                continue;
            }
            
            var flagName = cliOptionAttr?.Name ?? $"--{prefix}-{property.Name.ToKebabCase()}";
            var description = cliOptionAttr?.Description ?? descriptionAttr?.Description ?? string.Empty;
            
            // Check for list/array types (repeatable options)
            var propType = property.PropertyType;
            var isList = propType != typeof(string) && typeof(System.Collections.IEnumerable).IsAssignableFrom(propType);

            if (isList)
            {
                // Handle List<string> etc.
                if (propType == typeof(IReadOnlyList<string>) || 
                    propType == typeof(List<string>) || 
                    propType == typeof(string[]) || 
                    propType == typeof(IEnumerable<string>))
                {
                    var option = new Option<string[]>(flagName)
                    {
                        Description = description,
                        Arity = ArgumentArity.ZeroOrMore,
                        AllowMultipleArgumentsPerToken = true
                    };
                    options.Add(option);

                    // If manually named (via Attribute), consider adding legacy alias if needed, or just rely on the new name.
                    // For now, simple override.
                }
            }
            else
            {
                // Scalar types
                var defaultValue = property.GetValue(defaultInstance);
                var underlyingType = GetUnderlyingType(propType);
                
                // Create generic option dynamically
                var optionType = typeof(Option<>).MakeGenericType(underlyingType);
                var option = (Option)Activator.CreateInstance(optionType, flagName)!;
                
                // Set description
                option.Description = description;

                // Force boolean options to be flags (Arity 0) to prevent consuming next tokens
                if (underlyingType == typeof(bool))
                {
                    option.Arity = ArgumentArity.Zero;
                }

                // Set default value if not null using DefaultValueFactory
                if (defaultValue != null)
                {
                    SetDefaultValue(option, optionType, underlyingType, defaultValue);
                }
                
                options.Add(option);
            }
        }

        return options;
    }

    /// <summary>
    /// Generates options and returns a mapping from each Option to its source property name.
    /// This allows factories to identify options without hardcoding CLI strings.
    /// </summary>
    public static (IEnumerable<Option> Options, Dictionary<string, string> AliasToProperty) GenerateOptionsWithMetadata<T>() 
        where T : class, IOptionSet, new()
    {
        var options = new List<Option>();
        var aliasToProperty = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        var prefix = T.Prefix;
        var defaultInstance = new T();

        foreach (var property in typeof(T).GetProperties(BindingFlags.Public | BindingFlags.Instance))
        {
            var cliOptionAttr = property.GetCustomAttribute<CliOptionAttribute>();
            var descriptionAttr = property.GetCustomAttribute<DescriptionAttribute>();
            
            if (cliOptionAttr is null && descriptionAttr is null)
            {
                continue;
            }
            
            var flagName = cliOptionAttr?.Name ?? $"--{prefix}-{property.Name.ToKebabCase()}";
            var description = cliOptionAttr?.Description ?? descriptionAttr?.Description ?? string.Empty;
            
            var propType = property.PropertyType;
            var isList = propType != typeof(string) && typeof(System.Collections.IEnumerable).IsAssignableFrom(propType);

            Option option;
            if (isList && (propType == typeof(IReadOnlyList<string>) || 
                          propType == typeof(List<string>) || 
                          propType == typeof(string[]) || 
                          propType == typeof(IEnumerable<string>)))
            {
                option = new Option<string[]>(flagName)
                {
                    Description = description,
                    Arity = ArgumentArity.ZeroOrMore,
                    AllowMultipleArgumentsPerToken = true
                };
            }
            else
            {
                var defaultValue = property.GetValue(defaultInstance);
                var underlyingType = GetUnderlyingType(propType);
                var optionType = typeof(Option<>).MakeGenericType(underlyingType);
                option = (Option)Activator.CreateInstance(optionType, flagName)!;
                option.Description = description;
                
                // Force boolean options to be flags (Arity 0) to prevent consuming next tokens
                if (underlyingType == typeof(bool))
                {
                    option.Arity = ArgumentArity.Zero;
                }

                if (defaultValue != null)
                {
                    SetDefaultValue(option, optionType, underlyingType, defaultValue);
                }
            }
            
            options.Add(option);
            
            // Map the option name and all aliases to the property name
            aliasToProperty[flagName] = property.Name;
            foreach (var alias in option.Aliases)
            {
                aliasToProperty[alias] = property.Name;
            }
        }

        return (options, aliasToProperty);
    }

    /// <summary>
    /// Non-generic version of GenerateOptionsWithMetadata for runtime type handling.
    /// </summary>
    public static (IEnumerable<Option> Options, Dictionary<string, string> AliasToProperty) GenerateOptionsWithMetadataForType(Type optionSetType)
    {
        var method = typeof(CliOptionBuilder).GetMethod(nameof(GenerateOptionsWithMetadata), BindingFlags.Public | BindingFlags.Static)!;
        var genericMethod = method.MakeGenericMethod(optionSetType);
        return ((IEnumerable<Option>, Dictionary<string, string>))genericMethod.Invoke(null, null)!;
    }
    
    private static void SetDefaultValue(Option option, Type optionType, Type valueType, object defaultValue)
    {
        // Get the DefaultValueFactory property
        var defaultValueFactoryProperty = optionType.GetProperty("DefaultValueFactory");
        if (defaultValueFactoryProperty == null) return;
        
        // Create a Func<ArgumentResult, T> delegate
        var method = typeof(CliOptionBuilder).GetMethod(nameof(CreateDefaultFactory), BindingFlags.NonPublic | BindingFlags.Static)!;
        var genericMethod = method.MakeGenericMethod(valueType);
        var factory = genericMethod.Invoke(null, new[] { defaultValue });
        
        defaultValueFactoryProperty.SetValue(option, factory);
    }
    
    private static Func<ArgumentResult, T> CreateDefaultFactory<T>(T value)
    {
        return _ => value;
    }
    
    /// <summary>
    /// Binds parsed results back to an instance of T, using the provided list of options.
    /// </summary>
    public static T Bind<T>(ParseResult result, IEnumerable<Option> options) where T : class, IOptionSet, new()
    {
        var instance = new T();
        var prefix = T.Prefix;
        var optionsList = options.ToList();

        foreach (var property in typeof(T).GetProperties(BindingFlags.Public | BindingFlags.Instance))
        {
            var cliOptionAttr = property.GetCustomAttribute<CliOptionAttribute>();
            var flagName = cliOptionAttr?.Name ?? $"--{prefix}-{property.Name.ToKebabCase()}";
            
            // Match by Name - System.CommandLine stores the full name including dashes in Name property
            var matchedOption = optionsList.FirstOrDefault(o => o.Name == flagName);
            
            if (matchedOption is not null)
            {
                var propType = property.PropertyType;
                var isList = propType != typeof(string) && typeof(System.Collections.IEnumerable).IsAssignableFrom(propType);

                if (isList && (
                    propType == typeof(IReadOnlyList<string>) || 
                    propType == typeof(List<string>) || 
                    propType == typeof(string[]) || 
                    propType == typeof(IEnumerable<string>)))
                {
                    var values = result.GetValue((Option<string[]>)matchedOption);
                    if (values is not null && values.Length > 0)
                    {
                        if (propType == typeof(IReadOnlyList<string>) || propType == typeof(List<string>))
                        {
                            property.SetValue(instance, values.ToList());
                        }
                        else
                        {
                            property.SetValue(instance, values);
                        }
                    }
                }
                else
                {
                    // Scalar value - use reflection to call GetValue<T>
                    var underlyingType = GetUnderlyingType(propType);
                    var genericGetValue = typeof(CliOptionBuilder).GetMethod(nameof(GetTypedValue), BindingFlags.NonPublic | BindingFlags.Static)!;
                    var typedMethod = genericGetValue.MakeGenericMethod(underlyingType);
                    var value = typedMethod.Invoke(null, new object[] { result, matchedOption });
                    
                    // For value types (bool, int, etc.), always set - they can't be null
                    // For reference types, only set if not null
                    if (value is not null || underlyingType.IsValueType)
                    {
                        property.SetValue(instance, value);
                    }
                }
            }
        }

        return instance;
    }
    
    private static T? GetTypedValue<T>(ParseResult result, Option option)
    {
        try
        {
            return result.GetValue((Option<T>)option);
        }
        catch
        {
            return default;
        }
    }

    private static Type GetUnderlyingType(Type type)
    {
        return Nullable.GetUnderlyingType(type) ?? type;
    }
    
    /// <summary>
    /// Non-generic version of GenerateOptions for runtime type handling.
    /// </summary>

    public static IEnumerable<Option> GenerateOptionsForType(Type optionSetType)
    {
        var method = typeof(CliOptionBuilder).GetMethod(nameof(GenerateOptions), BindingFlags.Public | BindingFlags.Static)!;
        var genericMethod = method.MakeGenericMethod(optionSetType);
        return (IEnumerable<Option>)genericMethod.Invoke(null, null)!;
    }
    
    /// <summary>
    /// Non-generic version of Bind for runtime type handling.
    /// </summary>

    public static object BindForType(Type optionSetType, ParseResult result, IEnumerable<Option> options)
    {
        var method = typeof(CliOptionBuilder).GetMethod(nameof(Bind), BindingFlags.Public | BindingFlags.Static)!;
        var genericMethod = method.MakeGenericMethod(optionSetType);
        return genericMethod.Invoke(null, new object[] { result, options })!;
    }

    /// <summary>
    /// Converts PascalCase to kebab-case.
    /// </summary>
    public static string ToKebabCase(this string value)
    {
        if (string.IsNullOrEmpty(value))
            return value;

        var result = new System.Text.StringBuilder();
        for (var i = 0; i < value.Length; i++)
        {
            var c = value[i];
            if (char.IsUpper(c))
            {
                if (i > 0) result.Append('-');
                result.Append(char.ToLowerInvariant(c));
            }
            else
            {
                result.Append(c);
            }
        }
        return result.ToString();
    }
}
