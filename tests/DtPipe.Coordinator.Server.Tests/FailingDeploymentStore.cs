namespace DtPipe.Coordinator.Server.Tests;

/// <summary>A deployment store that can be taken down: every call then fails as a store that cannot be reached does.</summary>
internal sealed class FailingDeploymentStore(IDeploymentStore inner) : IDeploymentStore
{
    public bool Down { get; set; }

    private T Guard<T>(Func<T> call) => Down ? throw new StoreUnavailableException("down") : call();

    public IReadOnlyList<StoredDeployment> Load() => Guard(inner.Load);

    public void Save(string pipelineId, string document) => Guard(() =>
    {
        inner.Save(pipelineId, document);
        return 0;
    });

    public void Delete(string pipelineId) => Guard(() =>
    {
        inner.Delete(pipelineId);
        return 0;
    });
}
