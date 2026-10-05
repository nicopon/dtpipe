namespace DtPipe.Lab.Coordinator;

public sealed class LabOptions
{
    /// <summary>The <c>samples/distributed-lab</c> directory: holds <c>pipelines/</c> and <c>rights-seed.json</c>.</summary>
    public required string LabRoot { get; init; }

    /// <summary>Scratch space for plans (split halves, assembled fragments). Never read by a node.</summary>
    public required string StateDir { get; init; }

    public required string DtPipeExecutable { get; init; }

    /// <summary>The URL node hosts reach the coordinator at.</summary>
    public required string PublicUrl { get; init; }

    /// <summary>The issuer of every token the coordinator accepts: an OpenID Connect provider it trusts.</summary>
    public required string Authority { get; init; }

    /// <summary>The audience a token must name to be accepted here.</summary>
    public required string Audience { get; init; }

    public static LabOptions FromConfiguration(IConfiguration configuration)
    {
        var labRoot = Path.GetFullPath(configuration["lab-root"] ?? Directory.GetCurrentDirectory());
        return new LabOptions
        {
            LabRoot = labRoot,
            PublicUrl = (configuration["urls"] ?? "http://127.0.0.1:5180").Split(';')[0].TrimEnd('/'),
            StateDir = Path.GetFullPath(configuration["state"] ?? Path.Combine(labRoot, ".state")),
            Authority = (configuration["authority"] ?? "http://127.0.0.1:5190").TrimEnd('/'),
            Audience = configuration["audience"] ?? "dtpipe-coordinator",
            DtPipeExecutable = Path.GetFullPath(configuration["dtpipe"]
                ?? Path.Combine(labRoot, "..", "..", "dist", "release", OperatingSystem.IsWindows() ? "dtpipe.exe" : "dtpipe")),
        };
    }
}
