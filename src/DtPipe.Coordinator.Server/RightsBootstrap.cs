namespace DtPipe.Coordinator.Server;

/// <summary>
/// An identity a deployment gives the rights to start with, verified by the key it gives: what the rights keep of the key, its hash and its
/// last four characters, never its text. <see cref="ToString"/> names the identity alone.
/// </summary>
public sealed record BootstrapIdentity(
    string ClientId, string Kind, string Name, IReadOnlyList<string> Groups, IReadOnlyList<string> Pipelines, string? Role, string KeyHash, string KeyHint)
{
    /// <summary>The identity as the rights add it, enabled, with its one key.</summary>
    public Identity ToIdentity(DateTimeOffset now) => new(ClientId, Name, "", Groups, true, Kind, Pipelines)
    {
        Auth = new IdentityAuth(AuthModes.Key, [new IdentityKey("1", KeyHash, KeyHint, now, RightsBootstrap.KeyLabel)]),
        Role = Role,
    };

    public override string ToString() => $"{Kind} {ClientId}";
}

/// <summary>
/// What <c>Coordinator:Bootstrap</c> gives the rights when the server starts (<see cref="RightsStore.Bootstrap"/>). <c>AdminKey</c> is the
/// key of the first administrator: a person, admin, whose id is the one the key names. <c>Identities</c> is a list of identities verified
/// by a key (<c>Id</c>, <c>Kind</c>, <c>Name</c>, <c>Groups</c>, <c>Pipelines</c>, <c>Role</c>, <c>Key</c>), a runner's for one. Each key is
/// a <see cref="SecretReference"/> and names its identity; one that is malformed, or names another, stops the start without being written.
/// The server makes no key: an identity of the rights that no bootstrap names starts with nothing.
/// </summary>
public sealed record RightsBootstrap(BootstrapIdentity? Admin, IReadOnlyList<BootstrapIdentity> Identities)
{
    public static readonly RightsBootstrap None = new(null, []);

    /// <summary>The author the rights' audit names for what the bootstrap adds: no identity's id, no email.</summary>
    public const string Author = "Coordinator:Bootstrap";

    /// <summary>The label of a key the bootstrap gave.</summary>
    public const string KeyLabel = "bootstrap";

    private const string Section = "Coordinator:Bootstrap";

    public bool IsEmpty => Admin is null && Identities.Count == 0;

    /// <summary>The ids of the people it names, the administrator included.</summary>
    public IReadOnlyList<string> People => [.. All.Where(i => i.Kind == IdentityKinds.Person).Select(i => i.ClientId)];

    private IEnumerable<BootstrapIdentity> All => new[] { Admin }.OfType<BootstrapIdentity>().Concat(Identities);

    public static RightsBootstrap FromConfiguration(IConfiguration configuration)
    {
        BootstrapIdentity? admin = null;
        if (SecretReference.Read(configuration, $"{Section}:AdminKey") is { } text)
        {
            var key = KeyOf(text, $"{Section}:AdminKey");
            admin = new(key.ClientId, IdentityKinds.Person, key.ClientId, [], [], CoordinatorRoles.Admin, key.Hash, key.Hint);
        }
        var identities = new List<BootstrapIdentity>();
        foreach (var entry in configuration.GetSection($"{Section}:Identities").GetChildren().OrderBy(c => int.TryParse(c.Key, out var n) ? n : int.MaxValue))
        {
            var setting = $"{Section}:Identities:{entry.Key}";
            var id = entry["Id"]?.Trim() is { } given && PipelineLibrary.IsValidId(given)
                ? given
                : throw new InvalidOperationException($"{setting}:Id must be an identity's id: lowercase letters, digits and '-'.");
            var key = KeyOf(SecretReference.Read(configuration, $"{setting}:Key")
                ?? throw new InvalidOperationException($"{setting}:Key is required: an identity the bootstrap adds is verified by the key the deployment gives it."), $"{setting}:Key");
            if (key.ClientId != id) throw new InvalidOperationException($"{setting}:Key opens another identity than '{id}': a key names the identity it opens.");
            identities.Add(new(id, entry["Kind"]?.Trim() is { Length: > 0 } kind ? kind : IdentityKinds.Node, entry["Name"]?.Trim() is { Length: > 0 } name ? name : id,
                entry.GetSection("Groups").Get<string[]>() ?? [], entry.GetSection("Pipelines").Get<string[]>() ?? [], entry["Role"], key.Hash, key.Hint));
        }
        var bootstrap = new RightsBootstrap(admin, identities);
        if (bootstrap.All.GroupBy(i => i.ClientId).FirstOrDefault(g => g.Count() > 1) is { } twice)
            throw new InvalidOperationException($"{Section} names '{twice.Key}' more than once.");
        return bootstrap;
    }

    private static ClearKey KeyOf(string text, string setting) =>
        ClearKey.From(text) ?? throw new InvalidOperationException($"{setting} is not a key of this coordinator: dtp_<id>_<43 characters of base64url>.");
}
