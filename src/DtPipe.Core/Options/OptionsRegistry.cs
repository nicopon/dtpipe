using System.Collections.Concurrent;

namespace DtPipe.Core.Options;

/// <summary>
/// Registry to hold specific option instances, populated from CLI or configuration.
/// </summary>
public class OptionsRegistry
{
	private readonly AsyncLocal<Dictionary<Type, object>> _options = new();

	private Dictionary<Type, object> CurrentOptions
	{
		get
		{
			if (_options.Value == null)
			{
				_options.Value = new Dictionary<Type, object>();
			}
			return _options.Value;
		}
	}

	/// <summary>
	/// Forks the current registry state into an isolated asynchronous scope.
	/// </summary>
	public void BeginScope()
	{
		var newDict = new Dictionary<Type, object>();
		if (_options.Value != null)
		{
			foreach (var kvp in _options.Value)
			{
				newDict[kvp.Key] = kvp.Value;
			}
		}
		_options.Value = newDict;
	}

	/// <summary>
	/// Registers an options instance.
	/// </summary>
	/// <summary>
	/// Registers an options instance and returns it.
	/// </summary>
	public T Register<T>(T options) where T : class, IOptionSet
	{
		CurrentOptions[typeof(T)] = options;
		return options;
	}

	/// <summary>
	/// Retrieves options of a specific type. Returns a default instance if not found.
	/// </summary>
	public T Get<T>() where T : class, IOptionSet, new()
	{
		if (CurrentOptions.TryGetValue(typeof(T), out var value))
		{
			return (T)value;
		}

		// Return default option if not registered (fallback)
		return new T();
	}

	/// <summary>
	/// Retrieves options of a specific type by runtime Type.
	/// </summary>
	public object Get(Type optionType)
	{
		if (CurrentOptions.TryGetValue(optionType, out var value))
		{
			return value;
		}

		// Return default instance if not registered
		try
		{
			return Activator.CreateInstance(optionType) ?? throw new InvalidOperationException($"Could not create instance of {optionType.Name}");
		}
		catch (Exception ex)
		{
			throw new InvalidOperationException($"Could not create default instance for option type {optionType.Name}. Ensure it has a parameterless constructor.", ex);
		}
	}

	/// <summary>
	/// Checks if options of a specific type are registered.
	/// </summary>
	public bool Has<T>() where T : class, IOptionSet
	{
		return CurrentOptions.ContainsKey(typeof(T));
	}

	/// <summary>
	/// Registers options by runtime type.
	/// </summary>
	public void RegisterByType(Type optionType, object options)
	{
		CurrentOptions[optionType] = options;
	}
}
