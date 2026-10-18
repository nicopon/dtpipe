namespace DtPipe.Coordinator.Server;

/// <summary>An armed deployment as the store keeps it: the pipeline it belongs to and its serialized document.</summary>
public sealed record StoredDeployment(string PipelineId, string Document);

/// <summary>
/// What <see cref="DeploymentManager"/> keeps beyond the process of the armed deployments: one serialized document per library
/// pipeline. One coordinator writes it, and the deployment's gate already serializes the writers of one pipeline, so a write names no
/// version. A store holds the text, never the shape of a deployment. A call that cannot reach the store throws
/// <see cref="StoreUnavailableException"/> having changed nothing the caller can rely on: a call that timed out may have been applied.
/// </summary>
public interface IDeploymentStore
{
    /// <summary>Every document the store holds, in no order.</summary>
    IReadOnlyList<StoredDeployment> Load();

    /// <summary>Keeps <paramref name="document"/> as the pipeline's, replacing the one it held.</summary>
    void Save(string pipelineId, string document);

    /// <summary>Forgets the pipeline's document; nothing happens when it has none.</summary>
    void Delete(string pipelineId);
}
