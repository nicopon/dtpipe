namespace DtPipe.Coordinator.Server.Tests;

/// <summary>A store that can be taken down: every call then fails as a store that cannot be reached does.</summary>
internal sealed class FailingRunStore(IRunStore inner) : IRunStore
{
    public bool Down { get; set; }

    private T Guard<T>(Func<T> call) => Down ? throw new StoreUnavailableException("down") : call();

    public long NextSequence() => Guard(inner.NextSequence);
    public void RaiseSequenceTo(long floor) => Guard(() => { inner.RaiseSequenceTo(floor); return 0; });
    public void Put(StoredRun run) => Guard(() => { inner.Put(run); return 0; });
    public StoredRuns Load() => Guard(inner.Load);
}
