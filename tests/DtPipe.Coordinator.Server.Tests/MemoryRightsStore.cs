using System.Collections.Concurrent;

namespace DtPipe.Coordinator.Server.Tests;

/// <summary>
/// <see cref="IRightsStore"/> in memory, for tests that are not about Redis: the same contract (a document, a version a write must
/// name, a create that happens once). A store lives as long as the test process, found again by the name of the state directory a test
/// gives it: a second <see cref="RightsStore"/> over the same name is a restart.
/// </summary>
public sealed class MemoryRightsStore : IRightsStore
{
    private static readonly ConcurrentDictionary<string, MemoryRightsStore> Stores = new();

    private readonly object _lock = new();
    private StoredRights? _stored;

    /// <summary>The store a test names; the same one every time it is asked for the same name.</summary>
    public static MemoryRightsStore At(string name) => Stores.GetOrAdd(name, _ => new MemoryRightsStore());

    /// <summary>The rights over the store of <paramref name="options"/>' state directory.</summary>
    public static RightsStore Rights(CoordinatorServerOptions options) => new(At(options.StateDir), options);

    public StoredRights? Load()
    {
        lock (_lock) return _stored;
    }

    public bool Create(string document)
    {
        lock (_lock)
        {
            if (_stored is not null) return false;
            _stored = new StoredRights(document, 1);
            return true;
        }
    }

    public long? Replace(string document, long expectedVersion)
    {
        lock (_lock)
        {
            if (_stored is null || _stored.Version != expectedVersion) return null;
            _stored = new StoredRights(document, expectedVersion + 1);
            return _stored.Version;
        }
    }
}
