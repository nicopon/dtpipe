using System.Collections.Concurrent;

namespace DtPipe.Coordinator.Server.Tests;

/// <summary>
/// <see cref="IIgnoredHostStore"/> in memory, for tests that are not about Redis: the same contract (what expired is forgotten, the
/// soonest to be let back in goes first past the bound). A store lives as long as the test process, found again by a name: a second
/// <see cref="EnrollmentQueue"/> over the same name is a restart.
/// </summary>
public sealed class MemoryIgnoredHostStore : IIgnoredHostStore
{
    private static readonly ConcurrentDictionary<string, MemoryIgnoredHostStore> Stores = new();

    private readonly object _lock = new();
    private readonly Dictionary<string, DateTimeOffset> _until = new(StringComparer.Ordinal);

    /// <summary>The store a test names; the same one every time it is asked for the same name.</summary>
    public static MemoryIgnoredHostStore At(string name) => Stores.GetOrAdd(name, _ => new MemoryIgnoredHostStore());

    /// <summary>A queue over a store of its own.</summary>
    public static EnrollmentQueue Queue(TimeProvider? time = null) => new(new MemoryIgnoredHostStore(), time);

    public IReadOnlyDictionary<string, DateTimeOffset> Load(DateTimeOffset now)
    {
        lock (_lock) return _until.Where(e => e.Value > now).ToDictionary(e => e.Key, e => e.Value, StringComparer.Ordinal);
    }

    public void Add(string subject, DateTimeOffset until, DateTimeOffset now, int max)
    {
        lock (_lock)
        {
            foreach (var expired in _until.Where(e => e.Value <= now).Select(e => e.Key).ToList()) _until.Remove(expired);
            _until[subject] = until;
            while (_until.Count > max) _until.Remove(_until.MinBy(e => e.Value).Key);
        }
    }
}
