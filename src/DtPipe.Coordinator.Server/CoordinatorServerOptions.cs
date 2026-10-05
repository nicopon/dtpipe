namespace DtPipe.Coordinator.Server;

/// <summary>
/// The server's settings, from the <c>Coordinator</c> configuration section: environment variables
/// (<c>Coordinator__StateDirectory</c>, …) or command-line arguments (<c>--Coordinator:StateDirectory</c>).
/// Paths are resolved against the working directory.
/// </summary>
public sealed class CoordinatorServerOptions
{
    /// <summary>Where the library, the run journal, the rights document and every plan's scratch files live.</summary>
    public required string StateDir { get; init; }

    /// <summary>The dtpipe binary the server runs itself: <c>split</c>, <c>--export-job</c>, <c>--dry-run</c>.</summary>
    public required string DtPipeExecutable { get; init; }

    /// <summary>The issuer of every token the server accepts: an OpenID Connect provider it trusts.</summary>
    public required string Authority { get; init; }

    /// <summary>The audience a token must name to be accepted here.</summary>
    public required string Audience { get; init; }

    /// <summary>Pipelines a new library starts with, one <c>.yaml</c> each; none when unset.</summary>
    public string? LibrarySeed { get; init; }

    /// <summary>The rights document a new state directory starts with; an empty one when unset.</summary>
    public string? RightsSeed { get; init; }

    /// <summary>
    /// Turns on what only a test bench needs: the Lab view and its manual planning (<c>/api/plan</c>,
    /// <c>/api/deploy</c>, the scenario catalog), fault injection, and <c>/api/query</c>, which opens
    /// a node's database from the server's own process. Off by default.
    /// </summary>
    public bool LabMode { get; init; }

    /// <summary>The Lab view's scenarios, one job file each; read only in <see cref="LabMode"/>.</summary>
    public string? Scenarios { get; init; }

    public static CoordinatorServerOptions FromConfiguration(IConfiguration configuration)
    {
        var section = configuration.GetSection("Coordinator");
        string? PathOf(string key) => section[key] is { Length: > 0 } value ? Path.GetFullPath(value) : null;
        return new CoordinatorServerOptions
        {
            StateDir = PathOf("StateDirectory") ?? Path.GetFullPath("state"),
            DtPipeExecutable = section["DtPipe"] is { Length: > 0 } dtpipe && (dtpipe.Contains('/') || dtpipe.Contains('\\'))
                ? Path.GetFullPath(dtpipe)
                : section["DtPipe"] is { Length: > 0 } name ? name : "dtpipe",
            Authority = (section["Authority"] ?? throw new InvalidOperationException("Coordinator:Authority is required: the issuer this server trusts.")).TrimEnd('/'),
            Audience = section["Audience"] ?? "dtpipe-coordinator",
            LibrarySeed = PathOf("LibrarySeed"),
            RightsSeed = PathOf("RightsSeed"),
            LabMode = section.GetValue<bool>("LabMode"),
            Scenarios = PathOf("Scenarios"),
        };
    }
}
