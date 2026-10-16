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
/// The armed deployments, one JSON file each under <c>deployments/</c> in the state directory, outside git like the run
/// journal, read back at start. A file that cannot be read is skipped with a warning.
/// </summary>
internal sealed class DeploymentStore
{
    private static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web) { WriteIndented = true };

    private readonly string _dir;
    private readonly ILogger _logger;

    public DeploymentStore(string stateDir, ILogger logger)
    {
        _dir = Path.Combine(stateDir, "deployments");
        _logger = logger;
        Directory.CreateDirectory(_dir);
    }

    public IReadOnlyList<ArmedDeployment> Load()
    {
        var armed = new List<ArmedDeployment>();
        foreach (var file in Directory.EnumerateFiles(_dir, "*.json"))
        {
            try
            {
                var record = JsonSerializer.Deserialize<ArmedDeployment>(File.ReadAllText(file), Json);
                if (record is null || !PipelineLibrary.IsValidId(record.PipelineId) || Path.GetFileNameWithoutExtension(file) != record.PipelineId)
                    throw new JsonException("it does not name the pipeline of its file");
                armed.Add(record);
            }
            catch (Exception ex) when (ex is JsonException or IOException)
            {
                _logger.LogWarning("Deployment {File} skipped: {Error}", file, ex.Message);
            }
        }
        return armed;
    }

    public void Save(ArmedDeployment armed)
    {
        var path = PathOf(armed.PipelineId);
        var temporary = path + ".tmp";
        File.WriteAllText(temporary, JsonSerializer.Serialize(armed, Json));
        File.Move(temporary, path, overwrite: true);
    }

    /// <summary>Forgets the pipeline's armed deployment, if it has one; a lab plan's id may be no library id and has no file.</summary>
    public void Delete(string pipelineId)
    {
        if (PipelineLibrary.IsValidId(pipelineId)) File.Delete(PathOf(pipelineId));
    }

    private string PathOf(string pipelineId) => Path.Combine(_dir, pipelineId + ".json");
}
