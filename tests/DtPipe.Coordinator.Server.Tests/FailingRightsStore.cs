namespace DtPipe.Coordinator.Server.Tests;

/// <summary>A rights store that can be taken down: every call then fails as a store that cannot be reached does.</summary>
internal sealed class FailingRightsStore(IRightsStore inner) : IRightsStore
{
    public bool Down { get; set; }

    private T Guard<T>(Func<T> call) => Down ? throw new StoreUnavailableException("down") : call();

    public StoredRights? Load() => Guard(inner.Load);
    public bool Create(string document) => Guard(() => inner.Create(document));
    public long? Replace(string document, long expectedVersion) => Guard(() => inner.Replace(document, expectedVersion));
}
