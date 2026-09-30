namespace DtPipe.Lab.Coordinator;

public sealed class LabOptions
{
    /// <summary>The <c>samples/distributed-lab</c> directory: holds <c>pipelines/</c> and <c>flow-matrix.json</c>.</summary>
    public required string LabRoot { get; init; }

    /// <summary>Scratch space for plans (split halves, assembled fragments). Never read by a node.</summary>
    public required string StateDir { get; init; }

    public required string DtPipeExecutable { get; init; }

    /// <summary>The URL node hosts reach the coordinator at: the issuer of every token.</summary>
    public required string PublicUrl { get; init; }

    /// <summary>
    /// How long a token lasts. A pipeline node keeps the token it connected with for as long as it
    /// is deployed; every re-arm fetches a new one.
    /// </summary>
    public TimeSpan TokenLifetime { get; init; } = TimeSpan.FromHours(12);

    public static LabOptions FromConfiguration(IConfiguration configuration)
    {
        var labRoot = Path.GetFullPath(configuration["lab-root"] ?? Directory.GetCurrentDirectory());
        return new LabOptions
        {
            LabRoot = labRoot,
            PublicUrl = (configuration["urls"] ?? "http://127.0.0.1:5180").Split(';')[0].TrimEnd('/'),
            StateDir = Path.GetFullPath(configuration["state"] ?? Path.Combine(labRoot, ".state")),
            TokenLifetime = int.TryParse(configuration["token-lifetime-seconds"], out var seconds) && seconds > 0
                ? TimeSpan.FromSeconds(seconds)
                : TimeSpan.FromHours(12),
            DtPipeExecutable = Path.GetFullPath(configuration["dtpipe"]
                ?? Path.Combine(labRoot, "..", "..", "dist", "release", OperatingSystem.IsWindows() ? "dtpipe.exe" : "dtpipe")),
        };
    }
}
