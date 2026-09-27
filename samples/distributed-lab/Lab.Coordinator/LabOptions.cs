namespace DtPipe.Lab.Coordinator;

public sealed class LabOptions
{
    /// <summary>The <c>samples/distributed-lab</c> directory: holds <c>pipelines/</c> and <c>flow-matrix.json</c>.</summary>
    public required string LabRoot { get; init; }

    /// <summary>Scratch space for plans (split halves, assembled fragments). Never read by a node.</summary>
    public required string StateDir { get; init; }

    public required string DtPipeExecutable { get; init; }

    /// <summary>
    /// The TransportR group every pipeline node lands in. <c>PipelineNode</c> connects without
    /// declaring groups, so the dev identity provider gives them all this one; the per-node groups
    /// of <c>flow-matrix.json</c> are enforced on the plan instead (<c>IPlanRegistry</c>).
    /// </summary>
    public const string RuntimeGroup = "lab";

    public static LabOptions FromConfiguration(IConfiguration configuration)
    {
        var labRoot = Path.GetFullPath(configuration["lab-root"] ?? Directory.GetCurrentDirectory());
        return new LabOptions
        {
            LabRoot = labRoot,
            StateDir = Path.GetFullPath(configuration["state"] ?? Path.Combine(labRoot, ".state")),
            DtPipeExecutable = Path.GetFullPath(configuration["dtpipe"]
                ?? Path.Combine(labRoot, "..", "..", "dist", "release", OperatingSystem.IsWindows() ? "dtpipe.exe" : "dtpipe")),
        };
    }
}
