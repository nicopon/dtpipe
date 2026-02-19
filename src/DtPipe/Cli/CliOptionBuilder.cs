using System.CommandLine;
using System.CommandLine.Parsing;
using System.ComponentModel;
using System.Linq.Expressions;
using System.Reflection;
using DtPipe.Core.Attributes;
using DtPipe.Core.Options;

namespace DtPipe.Cli;

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

			var flagName = cliOptionAttr?.Name;
			if (string.IsNullOrEmpty(flagName))
			{
				var kebabProp = property.Name.ToKebabCase();
				flagName = kebabProp == prefix.ToLowerInvariant()
					? $"--{prefix}"
					: $"--{prefix}-{kebabProp}";
			}

			var description = cliOptionAttr?.Description ?? descriptionAttr?.Description ?? string.Empty;

			// Check for list/array types (repeatable options)
			var propType = property.PropertyType;
			var isList = propType != typeof(string) && typeof(System.Collections.IEnumerable).IsAssignableFrom(propType);

			Option option;

			if (isList)
			{
				// Handle List<string> etc.
				if (propType == typeof(IReadOnlyList<string>) ||
					propType == typeof(List<string>) ||
					propType == typeof(string[]) ||
					propType == typeof(IEnumerable<string>))
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
					continue;
				}
			}
			else
			{
				// Scalar types
				var defaultValue = property.GetValue(defaultInstance);
				var underlyingType = GetUnderlyingType(propType);

				// Create generic option dynamically
				var optionType = typeof(Option<>).MakeGenericType(underlyingType);
				option = (Option)Activator.CreateInstance(optionType, new object[] { flagName })!;

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
			}

			// Add Aliases if defined
			if (cliOptionAttr?.Aliases != null)
			{
				foreach (var alias in cliOptionAttr.Aliases)
				{
					option.Aliases.Add(alias);
				}
			}

			// Set Hidden status (workaround for older System.CommandLine)
			if (cliOptionAttr?.Hidden == true)
			{
				option.Description = "[HIDDEN] " + (option.Description ?? "");
			}

			options.Add(option);
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

			var flagName = cliOptionAttr?.Name;
			if (string.IsNullOrEmpty(flagName))
			{
				var kebabProp = property.Name.ToKebabCase();
				flagName = kebabProp == prefix.ToLowerInvariant()
					? $"--{prefix}"
					: $"--{prefix}-{kebabProp}";
			}

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
				option = (Option)Activator.CreateInstance(optionType, new object[] { flagName })!;
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

			// Add Aliases if defined
			if (cliOptionAttr?.Aliases != null)
			{
				foreach (var alias in cliOptionAttr.Aliases)
				{
					option.Aliases.Add(alias);
				}
			}

			// Set Hidden status (workaround for older System.CommandLine)
			if (cliOptionAttr?.Hidden == true)
			{
				option.Description = "[HIDDEN] " + (option.Description ?? "");
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
		// For System.CommandLine 2.0.2, use DefaultValueFactory property
		var prop = optionType.GetProperty("DefaultValueFactory");
		if (prop != null)
		{
			try
			{
				// Create Func<ParseResult, T> that returns the constant defaultValue
				var parseResultParam = Expression.Parameter(typeof(ParseResult), "p");
				var body = Expression.Constant(defaultValue, valueType);
				var lambda = Expression.Lambda(typeof(Func<,>).MakeGenericType(typeof(ParseResult), valueType), body, parseResultParam);
				var delegateInstance = lambda.Compile();
				prop.SetValue(option, delegateInstance);
				return;
			}
			catch
			{
				// Fallback if Expression fails
			}
		}

		// Fallback for older versions or if property is missing/fails
		var method = optionType.GetMethod("SetDefaultValue", new[] { valueType });
		if (method != null)
		{
			method.Invoke(option, new[] { defaultValue });
		}
		else
		{
			// Try setting the property if it exists
			var defaultValueProp = optionType.GetProperty("DefaultValue");
			if (defaultValueProp != null && defaultValueProp.CanWrite)
			{
				defaultValueProp.SetValue(option, defaultValue);
			}
		}
	}

	/// <summary>
	/// Binds parsed results back to an instance of T, using the provided list of options.
	/// </summary>
	public static T Bind<T>(ParseResult result, IEnumerable<Option> options) where T : class, IOptionSet, new()
	{
		var instance = new T();
		Bind(instance, result, options);
		return instance;
	}

	/// <summary>
	/// Binds parsed results to an existing instance of T.
	/// </summary>
	public static void Bind<T>(T instance, ParseResult result, IEnumerable<Option> options) where T : class, IOptionSet
	{
		var prefix = T.Prefix;
		var optionsList = options.ToList();

		foreach (var property in typeof(T).GetProperties(BindingFlags.Public | BindingFlags.Instance))
		{
			var cliOptionAttr = property.GetCustomAttribute<CliOptionAttribute>();
			var flagName = cliOptionAttr?.Name;
			if (string.IsNullOrEmpty(flagName))
			{
				var kebabProp = property.Name.ToKebabCase();
				flagName = kebabProp == prefix.ToLowerInvariant()
					? $"--{prefix}"
					: $"--{prefix}-{kebabProp}";
			}

			// Match by Alias - System.CommandLine stripping dashes from Name, but Aliases keeps them.
			var matchedOption = optionsList.FirstOrDefault(o => o.Name == flagName || o.Aliases.Contains(flagName));

			if (matchedOption is not null)
			{
				// Robust check for option presence in the parse result.
				// 1. Try direct instance match (standard/fastest)
				var optionResult = result.GetResult(matchedOption);

				// 2. Fallback: if instance match fails (cloned symbols), check by alias presence in tokens.
				if (optionResult is null)
				{
					bool presentInTokens = false;
					foreach (var token in result.Tokens)
					{
						if (token.Type == TokenType.Option &&
						   (token.Value == matchedOption.Name || matchedOption.Aliases.Contains(token.Value)))
						{
							presentInTokens = true;
							break;
						}
					}
					if (!presentInTokens) continue;
				}
				else
				{
					// If GetResult returns a result, we should check if it was provided or just contains the default.
					// System.CommandLine 2.0.2 OptionResult doesn't have an easy "WasProvided" but usually
					// we can check if it has any tokens.
					if (!optionResult.Tokens.Any())
					{
						// Check if it's a bool flag (Arity 0). If it's a flag and not in tokens, keep POCO default.
						bool presentInTokens = false;
						foreach (var token in result.Tokens)
						{
							if (token.Type == TokenType.Option &&
							   (token.Value == matchedOption.Name || matchedOption.Aliases.Contains(token.Value)))
							{
								presentInTokens = true;
								break;
							}
						}
						if (!presentInTokens) continue;
					}
				}

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

	public static object BindForType(Type optionSetType, ParseResult result, IEnumerable<Option> options)
	{
		var method = typeof(CliOptionBuilder).GetMethods(BindingFlags.Public | BindingFlags.Static)
			.First(m => m.Name == nameof(Bind) && m.IsGenericMethod && m.GetParameters().Length == 2);
		var genericMethod = method.MakeGenericMethod(optionSetType);
		return genericMethod.Invoke(null, new object[] { result, options })!;
	}

	public static void BindForType(Type optionSetType, object instance, ParseResult result, IEnumerable<Option> options)
	{
		var method = typeof(CliOptionBuilder).GetMethods(BindingFlags.Public | BindingFlags.Static)
			.First(m => m.Name == nameof(Bind) && m.IsGenericMethod && m.GetParameters().Length == 3);

		var genericMethod = method.MakeGenericMethod(optionSetType);
		genericMethod.Invoke(null, new object[] { instance, result, options });
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
