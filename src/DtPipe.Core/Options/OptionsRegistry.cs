using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;

namespace DtPipe.Core.Options;

/// <summary>
/// Registry to hold specific option instances, populated from CLI or configuration.
/// </summary>
public class OptionsRegistry : IOptionsProvider
{
	private readonly AsyncLocal<Dictionary<Type, object>> _options = new();
	private readonly ILogger? _logger;

	public OptionsRegistry(ILogger? logger = null)
	{
		_logger = logger;
	}

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
	/// F17: a miss previously fell back to <c>new T()</c> silently — it now emits a
	/// warning naming the type so silently-unbound options are visible.
	/// </summary>
	public T Get<T>() where T : class, IOptionSet, new()
	{
		if (CurrentOptions.TryGetValue(typeof(T), out var value))
		{
			return (T)value;
		}

		WarnMissing(typeof(T));
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

		WarnMissing(optionType);

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
	/// Materializes a default instance for <paramref name="optionType"/> WITHOUT the
	/// missing-options warning. For bulk registration passes (e.g. provider configuration
	/// binding iterates every contributor and intentionally materializes defaults for
	/// inactive providers) — genuine consumers should use <see cref="Get(Type)"/>.
	/// </summary>
	public object GetOrNew(Type optionType)
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
	/// Materializes a default instance of <typeparamref name="T"/> WITHOUT the missing-options
	/// warning, for consumers whose default is a legitimate outcome rather than a lost binding.
	/// </summary>
	public T GetOrNew<T>() where T : class, IOptionSet, new()
		=> CurrentOptions.TryGetValue(typeof(T), out var value) ? (T)value : new T();

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

	private void WarnMissing(Type optionType)
		=> Warn($"[dtpipe] Warning: no options of type '{optionType.Name}' were registered; using a fresh default instance. If this component expected CLI/config values, they were silently skipped.");

	private void Warn(string message)
	{
		if (_logger != null)
		{
			_logger.LogWarning("{Message}", message);
		}
		else
		{
			Console.Error.WriteLine(message);
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
