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
	// F17: warn once per (type, property) when a property is skipped for lack of any
	// CLI metadata — a silent skip usually means a forgotten [ComponentOption].
	private static readonly HashSet<string> WarnedSkippedProperties = new(StringComparer.Ordinal);

	public static IEnumerable<FlagDef> GenerateFlagDefsForType(Type t, FlagScope scope = FlagScope.PerBranch)
	{
		var flags = new List<FlagDef>();

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

			if (cliOptionAttr is null && descriptionAttr is null && metadataFlag is null)
			{
				WarnSkippedProperty(t, property);
				continue;
			}

			// F8 single derivation source — see FlagNameDeriver.
			var flagName = FlagNameDeriver.DeriveCanonical(property, t, cliMetadata);

			var description = ResolveDescription(cliOptionAttr, descriptionAttr);
			var propType = property.PropertyType;
			var isList = propType != typeof(string) && typeof(System.Collections.IEnumerable).IsAssignableFrom(propType);

			var arity = FlagArity.Scalar;
			var booleanValued = false;
			if (isList) arity = FlagArity.Repeatable;
			else if (GetUnderlyingType(propType) == typeof(bool))
			{
				// A switch turns an option on, so it only means something for an option that is
				// off by default. One that starts on takes true|false, or nothing could turn it off.
				booleanValued = defaultInstance is not null && property.GetValue(defaultInstance) is true;
				arity = booleanValued ? FlagArity.Scalar : FlagArity.Boolean;
			}

			flags.Add(new FlagDef(flagName, cliOptionAttr?.Aliases ?? Array.Empty<string>(), arity, scope, description,
				BooleanValued: booleanValued));
		}
		return flags;
	}

	private static Type GetUnderlyingType(Type type)
	{
		return Nullable.GetUnderlyingType(type) ?? type;
	}

	private static void WarnSkippedProperty(Type declaringType, PropertyInfo property)
	{
		// F17 signal is aimed at development time: a forgotten [ComponentOption] on a NEW
		// property. Curated internal properties (engine options, schema injection slots…)
		// legitimately carry no CLI metadata, so the reminder only fires under DEBUG=1.
		if (Environment.GetEnvironmentVariable("DEBUG") != "1") return;

		var key = $"{declaringType.FullName}.{property.Name}";
		lock (WarnedSkippedProperties)
		{
			if (!WarnedSkippedProperties.Add(key)) return;
		}
		Console.Error.WriteLine(
			$"[dtpipe] Warning: property '{property.Name}' on options type '{declaringType.Name}' has no [ComponentOption], [Description] or metadata mapping — it is not exposed as a CLI flag. Add [ComponentOption] if it should be user-facing.");
	}

	/// <summary>Resolves a property's help description, preferring the CLI-specific override over the general one.</summary>
	public static string ResolveDescription(ComponentOptionAttribute? cliOptionAttr, DescriptionAttribute? descriptionAttr)
		=> cliOptionAttr?.Description ?? descriptionAttr?.Description ?? string.Empty;
}
