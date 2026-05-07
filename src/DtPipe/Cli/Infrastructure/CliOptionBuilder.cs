using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Reflection;
using DtPipe.Core.Attributes;
using DtPipe.Core.Options;
using DtPipe.Cli.Pipeline;

namespace DtPipe.Cli.Infrastructure;

public static class CliOptionBuilder
{
	public static IEnumerable<FlagDef> GenerateFlagDefsForType(Type t, FlagScope scope = FlagScope.PerBranch)
	{
		var flags = new List<FlagDef>();
		var prefixProp = t.GetProperty("Prefix", BindingFlags.Public | BindingFlags.Static | BindingFlags.FlattenHierarchy);
		var prefix = prefixProp?.GetValue(null) as string ?? "";

		var defaultInstance = Activator.CreateInstance(t);
		IReadOnlyDictionary<string, string>? cliMetadata = null;
		if (defaultInstance is ICliOptionMetadata meta)
		{
			cliMetadata = meta.PropertyToFlag;
		}

		foreach (var property in t.GetProperties(BindingFlags.Public | BindingFlags.Instance))
		{
			var cliOptionAttr = property.GetCustomAttribute<ComponentOptionAttribute>();
			var descriptionAttr = property.GetCustomAttribute<DescriptionAttribute>();

			string? metadataFlag = null;
			if (cliMetadata != null && cliMetadata.TryGetValue(property.Name, out var mappedFlag))
			{
				metadataFlag = mappedFlag;
			}

			if (cliOptionAttr is null && descriptionAttr is null && metadataFlag is null) continue;

			var flagName = metadataFlag ?? cliOptionAttr?.Name;
			if (string.IsNullOrEmpty(flagName))
			{
				var kebabProp = property.Name.ToKebabCase();
				flagName = kebabProp == prefix.ToLowerInvariant()
					? $"--{prefix.ToLowerInvariant()}"
					: (string.IsNullOrEmpty(prefix) ? $"--{kebabProp}" : $"--{prefix.ToLowerInvariant()}-{kebabProp}");
			}

			var description = cliOptionAttr?.Description ?? descriptionAttr?.Description ?? string.Empty;
			var propType = property.PropertyType;
			var isList = propType != typeof(string) && typeof(System.Collections.IEnumerable).IsAssignableFrom(propType);

			var arity = FlagArity.Scalar;
			if (isList) arity = FlagArity.Repeatable;
			else if (GetUnderlyingType(propType) == typeof(bool)) arity = FlagArity.Boolean;

			flags.Add(new FlagDef(flagName, cliOptionAttr?.Aliases ?? Array.Empty<string>(), arity, scope, description));
		}
		return flags;
	}

	private static Type GetUnderlyingType(Type type)
	{
		return Nullable.GetUnderlyingType(type) ?? type;
	}

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
