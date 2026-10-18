using System.Text.Json;
using System.Text.Json.Serialization;
using DtPipe.Coordinator.Contracts;
using DtPipe.Jobs.Coordinator;

namespace DtPipe.Coordinator.Server;

/// <summary>
/// A library pipeline's deployment: the intention to run it, armed at a commit. A run plans from the job at
/// <see cref="Commit"/> and from <see cref="Bindings"/> as they were when it was armed, never from the library as it is now.
/// <see cref="JobSha"/> and <see cref="BindingsSha"/> say, by content, whether the library still holds what is armed
/// (<see cref="PipelineLibrary.Sha"/>, <see cref="Bindings.Sha"/>): a commit that put the same text back is the same job.
/// </summary>
public sealed record ArmedDeployment(
    string PipelineId, string Commit, string JobSha, string? BindingsSha, IReadOnlyDictionary<string, Binding> Bindings,
    DateTimeOffset ArmedAt, string By);

/// <summary>Where a pipeline's job is read at a commit: the library, or a script in a test.</summary>
public interface IJobSource
{
    /// <summary>The pipeline's job as <paramref name="commit"/> holds it, or null when the library has no such commit or file.</summary>
    Task<string?> JobAtAsync(string pipelineId, string commit, CancellationToken ct);
}

/// <summary>
/// The armed deployments as <see cref="DeploymentService"/> sees them: each serialized as one JSON document in an
/// <see cref="IDeploymentStore"/>, read back at start. A document that cannot be read, or that names another pipeline than its
/// own, is skipped with a warning, and its pipeline is then not armed. An id that is no library id has no document, and the store
/// is never asked for it.
/// </summary>
internal sealed class DeploymentStore(IDeploymentStore store, ILogger logger)
{
    private static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web);

    public IReadOnlyList<ArmedDeployment> Load()
    {
        var armed = new List<ArmedDeployment>();
        foreach (var stored in store.Load())
        {
            try
            {
                var record = JsonSerializer.Deserialize<ArmedDeployment>(stored.Document, Json);
                if (record is null || !PipelineLibrary.IsValidId(record.PipelineId) || record.PipelineId != stored.PipelineId)
                    throw new JsonException("it does not name the pipeline it is kept for");
                armed.Add(record);
            }
            catch (JsonException ex)
            {
                logger.LogWarning("Deployment {Pipeline} skipped: {Error}", stored.PipelineId, ex.Message);
            }
        }
        return armed;
    }

    public void Save(ArmedDeployment armed) => store.Save(armed.PipelineId, JsonSerializer.Serialize(armed, Json));

    /// <summary>Forgets the pipeline's armed deployment, if it has one; a lab plan's id may be no library id and has no document.</summary>
    public void Delete(string pipelineId)
    {
        if (PipelineLibrary.IsValidId(pipelineId)) store.Delete(pipelineId);
    }
}
