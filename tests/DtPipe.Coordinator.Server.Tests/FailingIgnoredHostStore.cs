namespace DtPipe.Coordinator.Server.Tests;

/// <summary>An ignored-hosts store that can be taken down: every call then fails as a store that cannot be reached does.</summary>
internal sealed class FailingIgnoredHostStore(IIgnoredHostStore inner) : IIgnoredHostStore
{
    public bool Down { get; set; }

    public IReadOnlyDictionary<string, DateTimeOffset> Load(DateTimeOffset now) =>
        Down ? throw new StoreUnavailableException("down") : inner.Load(now);

    public void Add(string subject, DateTimeOffset until, DateTimeOffset now, int max)
    {
        if (Down) throw new StoreUnavailableException("down");
        inner.Add(subject, until, now, max);
    }
}
