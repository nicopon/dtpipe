namespace DtPipe.Coordinator.Server;

/// <summary>
/// The Redis that keeps the coordinator's state: the run journal, the rights and the session keys (<c>Coordinator:Redis</c>).
/// <see cref="ConnectionString"/> is a secret when it holds a password: it is read from the environment
/// (<c>Coordinator__Redis__ConnectionString</c>) and written in no log or message.
/// <see cref="KeyPrefix"/> lets several coordinators share one Redis. <see cref="Initialize"/> allows the journal and the rights to
/// begin where Redis holds none: the first start, or after Redis lost its data.
/// </summary>
public sealed record RedisSettings(string? ConnectionString = null, string KeyPrefix = "dtpipe-coordinator:", bool Initialize = false);

/// <summary>
/// The server's settings, from the <c>Coordinator</c> configuration section: environment variables
/// (<c>Coordinator__StateDirectory</c>, …) or command-line arguments (<c>--Coordinator:StateDirectory</c>).
/// Paths are resolved against the working directory.
/// </summary>
public sealed class CoordinatorServerOptions
{
    /// <summary>Where the library, the rights document and every plan's scratch files live.</summary>
    public required string StateDir { get; init; }

    /// <summary>The issuer of every token the server accepts: an OpenID Connect provider it trusts.</summary>
    public required string Authority { get; init; }

    /// <summary>The audience a service's token must name to be accepted here.</summary>
    public required string Audience { get; init; }

    /// <summary>The provider's client the pages sign people in with: a confidential authorization-code client.</summary>
    public required string UiClientId { get; init; }
    public required string UiClientSecret { get; init; }

    /// <summary>
    /// The provider's client <c>dtpipe studio</c> signs people in with (<c>Coordinator:Studio:ClientId</c>,
    /// a public authorization-code client): a person's bearer token obtained through it reaches <c>/api</c>.
    /// None when unset, and then no bearer token reaches <c>/api</c>.
    /// </summary>
    public string? StudioClientId { get; init; }

    /// <summary>Pipelines a new library starts with, one <c>.yaml</c> each; none when unset.</summary>
    public string? LibrarySeed { get; init; }

    /// <summary>The rights document a new state directory starts with; an empty one when unset.</summary>
    public string? RightsSeed { get; init; }

    /// <summary>
    /// The claims of a token shown beside a host waiting for approval, to help an administrator tell
    /// who it is (<c>Coordinator:Identity:LabelClaims</c>, a list). Only a label: nothing grants anything. None when unset.
    /// </summary>
    public IReadOnlyList<string> IdentityLabelClaims { get; init; } = [];

    /// <summary>
    /// Turns on what only a test bench needs: the Lab view and its manual planning (<c>/api/plan</c>,
    /// <c>/api/deploy</c>, the scenario catalog), and fault injection. Off by default.
    /// </summary>
    public bool LabMode { get; init; }

    /// <summary>
    /// How long a health read is kept while nothing the server publishes moves (<c>Coordinator:Health:MaxAge</c>, a time span). The state of
    /// TransportR's registrations changes without an event, so a read is never kept longer than this.
    /// </summary>
    public TimeSpan HealthMaxAge { get; init; } = TimeSpan.FromMinutes(5);

    /// <summary>Where the run journal lives beyond the process; the server does not start without a connection string.</summary>
    public RedisSettings Redis { get; init; } = new();

    /// <summary>The Lab view's scenarios, one job file each; read only in <see cref="LabMode"/>.</summary>
    public string? Scenarios { get; init; }

    public static CoordinatorServerOptions FromConfiguration(IConfiguration configuration)
    {
        var section = configuration.GetSection("Coordinator");
        string? PathOf(string key) => section[key] is { Length: > 0 } value ? Path.GetFullPath(value) : null;
        return new CoordinatorServerOptions
        {
            StateDir = PathOf("StateDirectory") ?? Path.GetFullPath("state"),
            Authority = (section["Authority"] ?? throw new InvalidOperationException("Coordinator:Authority is required: the issuer this server trusts.")).TrimEnd('/'),
            Audience = section["Audience"] ?? "dtpipe-coordinator",
            UiClientId = section["Ui:ClientId"] ?? "coordinator-ui",
            UiClientSecret = section["Ui:ClientSecret"] ?? throw new InvalidOperationException("Coordinator:Ui:ClientSecret is required: the secret of the pages' client at the provider."),
            StudioClientId = section["Studio:ClientId"] is { Length: > 0 } studio ? studio : null,
            LibrarySeed = PathOf("LibrarySeed"),
            RightsSeed = PathOf("RightsSeed"),
            IdentityLabelClaims = section.GetSection("Identity:LabelClaims").Get<string[]>() ?? [],
            LabMode = section.GetValue<bool>("LabMode"),
            Scenarios = PathOf("Scenarios"),
            Redis = new RedisSettings(
                section["Redis:ConnectionString"] is { Length: > 0 } redis ? redis : null,
                section["Redis:KeyPrefix"] is { Length: > 0 } prefix ? prefix : "dtpipe-coordinator:",
                section.GetValue<bool>("Redis:Initialize")),
            HealthMaxAge = section.GetValue<TimeSpan?>("Health:MaxAge") is { } age && age > TimeSpan.Zero ? age : TimeSpan.FromMinutes(5),
        };
    }
}
