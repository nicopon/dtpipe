using System.ComponentModel;
using System.Reflection;

namespace DtPipe.Configuration;

/// <summary>
/// Helper to bind configuration dictionaries (e.g. from YAML) to strongly-typed objects.
/// </summary>
public static class ConfigurationBinder
{
	/// <summary>
	/// Binds a dictionary of key-values to properties of type T.
	/// Case-insensitive matching. Supports primitive types and enums.
	/// </summary>
	public static T Bind<T>(Dictionary<string, object> config) where T : new()
	{
		var instance = new T();
		Bind(instance, config);
		return instance;
	}

	/// <summary>
	/// Binds a dictionary to an existing instance.
	/// </summary>
	public static void Bind<T>(T instance, Dictionary<string, object> config)
	{
		if (config == null || config.Count == 0 || instance == null) return;

		// Build map of properties for case-insensitive lookup (kebab-case aware?)
		// Strategy: Normalize keys to simple string (remove dashes, lower case) for matching
		var properties = instance.GetType().GetProperties(BindingFlags.Public | BindingFlags.Instance)
			.Where(p => p.CanWrite)
			.ToList();

		// dictionary keys might be kebab-case "fetch-size" or "FetchSize"
		// Target property: FetchSize.
		// Normalize func: s => s.Replace("-", "").ToLowerInvariant()

		foreach (var kvp in config)
		{
			var key = kvp.Key;
			var value = kvp.Value;

			var normalizedKey = key.Replace("-", "").Replace("_", "").ToLowerInvariant();

			var prop = properties.FirstOrDefault(p =>
				p.Name.Replace("_", "").ToLowerInvariant() == normalizedKey);

			if (prop != null)
			{
				try
				{
					var convertedValue = ConvertValue(value, prop.PropertyType);
					prop.SetValue(instance, convertedValue);
				}
				catch (Exception ex)
				{
					Console.Error.WriteLine($"Warning: Failed to bind config '{key}' to property '{prop.Name}': {ex.Message}");
				}
			}
		}
	}

	private static object? ConvertValue(object value, Type targetType)
	{
		if (value == null) return null;

		var underlyingType = Nullable.GetUnderlyingType(targetType) ?? targetType;

		// If direct assignment works
		if (underlyingType.IsInstanceOfType(value))
		{
			return value;
		}

		// String conversions
		var stringValue = value.ToString();
		if (string.IsNullOrEmpty(stringValue)) return null;

		if (underlyingType.IsEnum)
		{
			return Enum.Parse(underlyingType, stringValue, ignoreCase: true);
		}

		if (underlyingType == typeof(Guid))
		{
			return Guid.Parse(stringValue);
		}

		if (underlyingType == typeof(TimeSpan))
		{
			return TimeSpan.Parse(stringValue);
		}

		return Convert.ChangeType(value, underlyingType);
	}
}
