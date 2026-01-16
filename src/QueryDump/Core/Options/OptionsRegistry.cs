using System.Collections.Concurrent;

namespace QueryDump.Core.Options;

/// <summary>
/// Registry to hold specific option instances, populated from CLI or configuration.
/// </summary>
public class OptionsRegistry
{
    private readonly ConcurrentDictionary<Type, object> _options = new();

    /// <summary>
    /// Registers an options instance.
    /// </summary>
    /// <summary>
    /// Registers an options instance and returns it.
    /// </summary>
    public T Register<T>(T options) where T : class, IOptionSet
    {
        _options[typeof(T)] = options;
        return options;
    }

    /// <summary>
    /// Retrieves options of a specific type. Returns a default instance if not found.
    /// </summary>
    public T Get<T>() where T : class, IOptionSet, new()
    {
        if (_options.TryGetValue(typeof(T), out var value))
        {
            return (T)value;
        }

        // Return default option if not registered (fallback)
        return new T();
    }
    
    /// <summary>
    /// Checks if options of a specific type are registered.
    /// </summary>
    public bool Has<T>() where T : class, IOptionSet
    {
        return _options.ContainsKey(typeof(T));
    }
    
    /// <summary>
    /// Registers options by runtime type.
    /// </summary>
    public void RegisterByType(Type optionType, object options)
    {
        _options[optionType] = options;
    }
}
