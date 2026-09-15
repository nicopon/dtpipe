using System.Collections.Concurrent;

namespace DtPipe.Core.Options;

/// <summary>
/// Registry to hold specific option instances, populated from CLI or configuration.
///
/// <para>
/// A miss is not a diagnostic and must not be reported as one. <c>ProviderConfigurationService</c>
/// registers every <c>IDataFactory</c>'s options type unconditionally, so after a configuration
/// pass every type is present whatever became of the individual keys — a miss therefore means
/// "no configuration pass reached this flow", a wiring condition, and can never mean "a value was
/// lost". A warning here told whoever passed nothing that their values had been skipped, on the
/// one path where there were none, and stayed silent on the path where there would be. The
/// invariant is worth keeping and is kept, as a test over the whole catalogue; a caller that
/// genuinely must not default has <see cref="Require{T}"/>.
/// </para>
/// </summary>
public class OptionsRegistry : IOptionsProvider
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

		return new T();
	}

	/// <summary>
	/// Retrieves options of a specific type by runtime Type. Returns a default instance if not found.
	/// </summary>
	public object Get(Type optionType)
	{
		if (CurrentOptions.TryGetValue(optionType, out var value))
		{
			return value;
		}

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
	/// Attempts to retrieve registered options of a specific type without side effects.
	/// </summary>
	public bool TryGet<T>(out T value) where T : class, IOptionSet, new()
	{
		if (CurrentOptions.TryGetValue(typeof(T), out var raw) && raw is T typed)
		{
			value = typed;
			return true;
		}
		value = new T();
		return false;
	}

	/// <summary>
	/// Attempts to retrieve registered options by runtime type without side effects
	/// (no warning, no default materialization). For capability probes over factories
	/// whose <c>OptionsType</c> is only known at runtime.
	/// </summary>
	public bool TryGetByType(Type optionType, out object? value)
	{
		if (CurrentOptions.TryGetValue(optionType, out var raw))
		{
			value = raw;
			return true;
		}
		value = null;
		return false;
	}

	/// <summary>
	/// Requires registered options of a specific type, throwing when they were never bound.
	/// Use instead of <see cref="Get{T}"/> in code paths where a silent default would hide
	/// a binding failure.
	/// </summary>
	public T Require<T>() where T : class, IOptionSet, new()
	{
		if (CurrentOptions.TryGetValue(typeof(T), out var raw) && raw is T typed)
		{
			return typed;
		}

		throw new InvalidOperationException(
			$"Options of type '{typeof(T).Name}' were required but never bound. " +
			"Ensure the component's flags were provided and the options were registered before use.");
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
