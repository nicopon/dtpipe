using System.Collections.Concurrent;

namespace DtPipe.Coordinator.Server.Tests;

/// <summary>
/// <see cref="IDeploymentStore"/> in memory, for tests that are not about Redis: a document per pipeline. A store lives as long as the
/// test process, found again by the name of the state directory a test gives it: a second <see cref="DeploymentManager"/> over the
/// same name is a restart.
/// </summary>
public sealed class MemoryDeploymentStore : IDeploymentStore
{
    private static readonly ConcurrentDictionary<string, MemoryDeploymentStore> Stores = new();

    private readonly object _lock = new();
    private readonly Dictionary<string, string> _documents = new(StringComparer.Ordinal);

    /// <summary>The store a test names; the same one every time it is asked for the same name.</summary>
    public static MemoryDeploymentStore At(string name) => Stores.GetOrAdd(name, _ => new MemoryDeploymentStore());

    /// <summary>The pipelines that have a document.</summary>
    public IReadOnlyList<string> Pipelines()
    {
        lock (_lock) return [.. _documents.Keys.Order(StringComparer.Ordinal)];
    }

    public IReadOnlyList<StoredDeployment> Load()
    {
        lock (_lock) return [.. _documents.Select(d => new StoredDeployment(d.Key, d.Value))];
    }

    public void Save(string pipelineId, string document)
    {
        lock (_lock) _documents[pipelineId] = document;
    }

    public void Delete(string pipelineId)
    {
        lock (_lock) _documents.Remove(pipelineId);
    }
}
