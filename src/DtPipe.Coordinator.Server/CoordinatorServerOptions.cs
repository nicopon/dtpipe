namespace DtPipe.Coordinator.Server;

/// <summary>Where the coordinator keeps its state beyond the process (<c>Coordinator:Store</c>).</summary>
public enum StoreKind
{
    /// <summary>A SQLite database file in a directory of the host: one coordinator, no other service. The default.</summary>
    Sqlite,

    /// <summary>A PostgreSQL database the deployment provides, named by <see cref="CoordinatorServerOptions.ConnectionString"/>.</summary>
    PostgreSql,

    /// <summary>A SQL Server database the deployment provides, named by <see cref="CoordinatorServerOptions.ConnectionString"/>.</summary>
    SqlServer,

    /// <summary>A Redis the deployment provides.</summary>
    Redis,
}

/// <summary>
/// The Redis that keeps the coordinator's state when <see cref="StoreKind.Redis"/> is chosen: the run journal, the rights, the deployments and the session keys (<c>Coordinator:Redis</c>).
/// <see cref="ConnectionString"/> is a secret when it holds a password: it is read from the environment
/// (<c>Coordinator__Redis__ConnectionString</c>) and written in no log or message.
/// <see cref="KeyPrefix"/> lets several coordinators share one Redis. <see cref="Initialize"/> allows the journal, the rights and the library
/// to begin where the state holds none: the first start, or after Redis (or the library's remote) lost its data. The connection string is
/// set through the environment only: a command-line argument can be listed by any process of the host.
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

    /// <summary>
    /// The OpenID Connect provider the server trusts for people and for the identities it verifies. The tokens of the identities
    /// verified by a key are <see cref="KeyIssuer"/>'s. None when <c>Coordinator:Authority</c> is unset: then every identity is verified
    /// by a key, the key issuer is the only authorization server, and <see cref="Bootstrap"/> must name the first administrator.
    /// </summary>
    public string? Authority { get; init; }

    /// <summary>The server trusts an identity provider: an identity may be verified by it.</summary>
    public bool HasProvider => Authority is not null;

    /// <summary>The audience a service's token must name to be accepted here.</summary>
    public required string Audience { get; init; }

    /// <summary>The provider's client the pages sign people in with: a confidential authorization-code client. Its secret is none without a provider.</summary>
    public required string UiClientId { get; init; }
    public string? UiClientSecret { get; init; }

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
    /// How long a health read is kept while nothing the server publishes moves (<c>Coordinator:Health:MaxAge</c>, a time span). The state of
    /// TransportR's registrations changes without an event, so a read is never kept longer than this.
    /// </summary>
    public TimeSpan HealthMaxAge { get; init; } = TimeSpan.FromMinutes(5);

    /// <summary>
    /// Where the state lives beyond the process (<c>Coordinator:Store</c>: <c>sqlite</c>, <c>postgresql</c>, <c>sqlserver</c> or
    /// <c>redis</c>); SQLite when unset.
    /// </summary>
    public StoreKind Store { get; init; } = StoreKind.Sqlite;

    /// <summary>
    /// The database of a PostgreSQL or SQL Server store (<c>Coordinator:ConnectionString</c>), required by those two and refused by the
    /// others. A secret, as <see cref="SecretReference"/> reads one: no log or message carries a part of it, the host it names included.
    /// </summary>
    public string? ConnectionString { get; init; }

    /// <summary>
    /// How long a start waits for another coordinator to leave the database before it refuses (<c>Coordinator:StoreLockWait</c>, a time
    /// span, 30 seconds unless set): the earlier instance of a rolling update. Never without bound.
    /// </summary>
    public TimeSpan StoreLockWait { get; init; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Whether the state may begin where none is kept (<c>Coordinator:Initialize</c>): the first start, or after the store (or the library's
    /// remote) lost its data. Off, a store that holds nothing stops the start, since rights begun again could grant what was revoked
    /// and run ids would be given twice.
    /// </summary>
    public bool Initialize { get; init; }

    /// <summary>The Redis the state lives in when <see cref="Store"/> is Redis; the server does not start without a connection string.</summary>
    public RedisSettings Redis { get; init; } = new();

    /// <summary>
    /// The directory of the SQLite database when <see cref="Store"/> is SQLite (<c>Coordinator:Sqlite:Directory</c>): the state directory
    /// unless set, so that a volume mounted for the state holds the database too, and a volume of its own can hold it alone.
    /// </summary>
    public string? SqliteDirectory { get; init; }

    /// <summary>
    /// Where the library's git repository lives (<c>Coordinator:Library:Directory</c>): <c>library</c> under the state directory unless set,
    /// so that a deployment can keep the repository on a volume of its own, apart from the state directory's scratch files.
    /// </summary>
    public string? LibraryDirectory { get; init; }

    /// <summary>The git repository the library is pushed to (<c>Coordinator:Library:Remote</c>); none when unset, and the library then stays in the state directory.</summary>
    public LibraryRemoteSettings? LibraryRemote { get; init; }

    /// <summary>
    /// The address people reach the server at (<c>Coordinator:PublicUrl</c>, <c>https://coordinator.example</c>): a scheme, a host and
    /// a port, no path. The server listens in plain HTTP behind a proxy that terminates TLS, so what the request says of its scheme and
    /// host is not what a person's browser used. When set, every request is read as made to this address, whatever its headers say.
    /// None when unset, and then the request's own scheme and host stand.
    /// </summary>
    public Uri? PublicUrl { get; init; }

    /// <summary>
    /// The issuer of the tokens this server makes for identities verified by a key: <see cref="PublicUrl"/>. None without it, and then no
    /// identity may be verified by a key.
    /// </summary>
    public string? KeyIssuer { get; init; }

    /// <summary>
    /// Whether a key may be exchanged while <see cref="PublicUrl"/> is plain HTTP (<c>Coordinator:Auth:AllowHttpKeys</c>): the key crosses
    /// the network at every renewal, so the exchange is refused there unless this is set, for a network that is private.
    /// </summary>
    public bool AllowHttpKeys { get; init; }

    /// <summary>Keys are exchanged here: there is an issuer, and it is reached over HTTPS or HTTP is allowed.</summary>
    public bool ExchangesKeys => KeyIssuer is not null && (PublicUrl!.Scheme == Uri.UriSchemeHttps || AllowHttpKeys);

    /// <summary>Keys are exchanged over plain HTTP because <see cref="AllowHttpKeys"/> says so: the log and every page say it.</summary>
    public bool ExchangesKeysOverHttp => KeyIssuer is not null && PublicUrl!.Scheme == Uri.UriSchemeHttp && AllowHttpKeys;

    /// <summary>
    /// How long a token the issuer makes for a service lasts (<c>Coordinator:Auth:ServiceTokenLifetime</c>, a time span, 10 minutes unless
    /// set, <see cref="MinServiceTokenLifetime"/> at least): a revoked key opens nothing after it, and a node asks for its next token two
    /// minutes before it ends.
    /// </summary>
    public TimeSpan ServiceTokenLifetime { get; init; } = TimeSpan.FromMinutes(10);

    public static readonly TimeSpan MinServiceTokenLifetime = TimeSpan.FromMinutes(5);

    /// <summary>
    /// Whether a person may be verified by a key (<c>Coordinator:Auth:PersonKeys</c>, true unless set false): a key has neither MFA
    /// nor SSO, so an organisation that has an identity provider may admit people through it alone. False, an identity of kind person
    /// is refused when it is added, a start whose rights hold one stops naming it, the sign-in form offers no key, the issuer gives a
    /// person no token, and the metadata does not name <see cref="DtPipe.Jobs.Coordinator.PersonAuth.Key"/>.
    /// </summary>
    public bool PersonKeys { get; init; } = true;

    /// <summary>
    /// A person signs in here with a key: the issuer exchanges keys (<see cref="ExchangesKeys"/>: over HTTPS, or over HTTP where
    /// <see cref="AllowHttpKeys"/> says so), and <see cref="PersonKeys"/> lets people be verified by it. Otherwise no form offers a key
    /// and the metadata names none, since no key could be taken.
    /// </summary>
    public bool SignsPeopleInByKey => ExchangesKeys && PersonKeys;

    /// <summary>
    /// How long a token the issuer makes for a person lasts (<c>Coordinator:Auth:PersonTokenLifetime</c>, a time span, an hour unless
    /// set, <see cref="MinServiceTokenLifetime"/> at least, since the studio asks for the next one two minutes before it ends). It is
    /// judged against the rights on every request whatever its lifetime, so a revoked key opens nothing after the next request.
    /// </summary>
    public TimeSpan PersonTokenLifetime { get; init; } = TimeSpan.FromHours(1);

    /// <summary>The ways a person signs in here, as the metadata lists them: the provider when there is one, and a key when <see cref="SignsPeopleInByKey"/>.</summary>
    public IReadOnlyList<string> PersonAuth
    {
        get
        {
            var ways = new List<string>();
            if (HasProvider) ways.Add(DtPipe.Jobs.Coordinator.PersonAuth.Idp);
            if (SignsPeopleInByKey) ways.Add(DtPipe.Jobs.Coordinator.PersonAuth.Key);
            return ways;
        }
    }

    /// <summary>
    /// The issuers the metadata names, which the studio and a node with no <c>Oidc:Authority</c> read first: the provider, then the key
    /// issuer; the key issuer alone without a provider.
    /// </summary>
    public IReadOnlyList<string> AuthorizationServers => new[] { Authority, KeyIssuer }.OfType<string>().ToList();

    /// <summary>The identities a deployment gives the rights to start with (<c>Coordinator:Bootstrap</c>); none unless set.</summary>
    public RightsBootstrap Bootstrap { get; init; } = RightsBootstrap.None;

    private static Uri? PublicUrlOf(string? value)
    {
        if (string.IsNullOrWhiteSpace(value)) return null;
        if (!Uri.TryCreate(value, UriKind.Absolute, out var uri) || uri.Scheme is not ("http" or "https")
            || uri.AbsolutePath != "/" || uri.Query.Length > 0 || uri.Fragment.Length > 0 || uri.UserInfo.Length > 0)
            throw new InvalidOperationException("Coordinator:PublicUrl must be an http or https address with a host and an optional port, and no user, path, query or fragment (https://coordinator.example).");
        return uri;
    }

    // A table, not Enum.TryParse, which takes "1" for a value.
    private static readonly (string Name, StoreKind Kind)[] StoreNames =
        [("sqlite", StoreKind.Sqlite), ("postgresql", StoreKind.PostgreSql), ("sqlserver", StoreKind.SqlServer), ("redis", StoreKind.Redis)];

    public static CoordinatorServerOptions FromConfiguration(IConfiguration configuration)
    {
        var section = configuration.GetSection("Coordinator");
        var publicUrl = PublicUrlOf(section["PublicUrl"]);
        string? PathOf(string key) => section[key] is { Length: > 0 } value ? Path.GetFullPath(value) : null;
        var store = section["Store"] is { Length: > 0 } kind
            ? StoreNames.FirstOrDefault(n => string.Equals(n.Name, kind, StringComparison.OrdinalIgnoreCase)) is { Name: not null } named ? named.Kind
                : throw new InvalidOperationException("Coordinator:Store must be 'sqlite', 'postgresql', 'sqlserver' or 'redis'.")
            : StoreKind.Sqlite;
        var storeName = StoreNames.First(n => n.Kind == store).Name;
        // A setting of another store would be ignored in silence, and a coordinator would keep its state elsewhere than meant.
        if (store != StoreKind.Redis && (section["Redis:ConnectionString"] is { Length: > 0 } || section["Redis:KeyPrefix"] is { Length: > 0 }))
            throw new InvalidOperationException($"Coordinator:Redis settings are given but Coordinator:Store is '{storeName}': set Coordinator:Store to 'redis', or remove them.");
        if (store != StoreKind.Sqlite && section["Sqlite:Directory"] is { Length: > 0 })
            throw new InvalidOperationException($"Coordinator:Sqlite:Directory is given but Coordinator:Store is '{storeName}': remove it, or set Coordinator:Store to 'sqlite'.");
        var connectionString = SecretReference.Read(configuration, "Coordinator:ConnectionString");
        if (store is StoreKind.PostgreSql or StoreKind.SqlServer && connectionString is null)
            throw new InvalidOperationException($"Coordinator:ConnectionString is required when Coordinator:Store is '{storeName}': the database the state is kept in, given through the environment or as @path to a file.");
        if (store is not (StoreKind.PostgreSql or StoreKind.SqlServer) && connectionString is not null)
            throw new InvalidOperationException($"Coordinator:ConnectionString is given but Coordinator:Store is '{storeName}': it names the database of a 'postgresql' or 'sqlserver' store.");
        var lockWait = section.GetValue<TimeSpan?>("StoreLockWait") ?? TimeSpan.FromSeconds(30);
        if (lockWait < TimeSpan.Zero)
            throw new InvalidOperationException("Coordinator:StoreLockWait must not be negative.");
        var authority = section["Authority"] is { Length: > 0 } external ? external.TrimEnd('/') : null;
        var keyIssuer = publicUrl?.GetLeftPart(UriPartial.Authority);
        // The issuer is what tells the two providers' tokens apart: one address for both would let either sign for the other.
        if (keyIssuer is not null && string.Equals(authority, keyIssuer, StringComparison.OrdinalIgnoreCase))
            throw new InvalidOperationException("Coordinator:Authority is Coordinator:PublicUrl, where this server issues the tokens of the identities verified by a key: an external identity provider has an issuer of its own.");
        var allowHttpKeys = section.GetValue<bool>("Auth:AllowHttpKeys");
        var personKeys = section.GetValue<bool?>("Auth:PersonKeys") ?? true;
        // Without a provider every identity is verified by a key, a person's included: the key issuer must exist and take a person's key.
        if (authority is null)
        {
            if (publicUrl is null)
                throw new InvalidOperationException("Coordinator:Authority is not set, so every identity is verified by a key whose tokens this server issues at Coordinator:PublicUrl, which is not set either: set Coordinator:PublicUrl, and Coordinator:Authority for an external identity provider.");
            if (section["Ui:ClientSecret"] is { Length: > 0 })
                throw new InvalidOperationException("Coordinator:Ui:ClientSecret is set but Coordinator:Authority is not: the pages' client is an identity provider's, and this server trusts none.");
            if (!personKeys)
                throw new InvalidOperationException("Coordinator:Auth:PersonKeys is false and Coordinator:Authority is not set: a person signs in with a key or through an identity provider, so nobody could sign in.");
            if (publicUrl.Scheme == Uri.UriSchemeHttp && !allowHttpKeys)
                throw new InvalidOperationException("Coordinator:PublicUrl is http, Coordinator:Auth:AllowHttpKeys is not set and Coordinator:Authority is not set: no key could be exchanged here, so nobody could sign in. Reach this server over HTTPS, or set Coordinator:Auth:AllowHttpKeys for a private network.");
        }
        var lifetime = section.GetValue<TimeSpan?>("Auth:ServiceTokenLifetime") ?? TimeSpan.FromMinutes(10);
        if (lifetime < MinServiceTokenLifetime)
            throw new InvalidOperationException($"Coordinator:Auth:ServiceTokenLifetime must be {MinServiceTokenLifetime.TotalMinutes} minutes at least: a node asks for its next token two minutes before its token ends.");
        var personLifetime = section.GetValue<TimeSpan?>("Auth:PersonTokenLifetime") ?? TimeSpan.FromHours(1);
        if (personLifetime < MinServiceTokenLifetime)
            throw new InvalidOperationException($"Coordinator:Auth:PersonTokenLifetime must be {MinServiceTokenLifetime.TotalMinutes} minutes at least: the studio asks for its next token two minutes before its token ends.");
        var options = new CoordinatorServerOptions
        {
            StateDir = PathOf("StateDirectory") ?? Path.GetFullPath("state"),
            Authority = authority,
            KeyIssuer = keyIssuer,
            AllowHttpKeys = allowHttpKeys,
            ServiceTokenLifetime = lifetime,
            PersonTokenLifetime = personLifetime,
            PersonKeys = personKeys,
            Audience = section["Audience"] ?? "dtpipe-coordinator",
            UiClientId = section["Ui:ClientId"] ?? "coordinator-ui",
            UiClientSecret = authority is null ? null
                : section["Ui:ClientSecret"] ?? throw new InvalidOperationException("Coordinator:Ui:ClientSecret is required: the secret of the pages' client at the provider."),
            Bootstrap = RightsBootstrap.FromConfiguration(configuration),
            StudioClientId = section["Studio:ClientId"] is { Length: > 0 } studio ? studio : null,
            LibrarySeed = PathOf("LibrarySeed"),
            LibraryDirectory = PathOf("Library:Directory"),
            RightsSeed = PathOf("RightsSeed"),
            IdentityLabelClaims = section.GetSection("Identity:LabelClaims").Get<string[]>() ?? [],
            PublicUrl = publicUrl,
            Store = store,
            ConnectionString = connectionString,
            StoreLockWait = lockWait,
            Initialize = section.GetValue<bool>("Initialize"),
            SqliteDirectory = PathOf("Sqlite:Directory"),
            Redis = new RedisSettings(
                section["Redis:ConnectionString"] is { Length: > 0 } redis ? redis : null,
                section["Redis:KeyPrefix"] is { Length: > 0 } prefix ? prefix : "dtpipe-coordinator:",
                section.GetValue<bool>("Initialize")),
            LibraryRemote = LibraryRemoteSettings.FromConfiguration(section),
            HealthMaxAge = section.GetValue<TimeSpan?>("Health:MaxAge") is { } age && age > TimeSpan.Zero ? age : TimeSpan.FromMinutes(5),
        };
        // The first administrator of a server with no provider signs in with the key the deployment gave it: the server makes none.
        if (authority is null && options.Bootstrap.Admin is null)
            throw new InvalidOperationException("Coordinator:Bootstrap:AdminKey is required when Coordinator:Authority is not set: it is the key of the first administrator (dtp_<id>_<secret>, given through the environment or as @path to a file), and this server makes none.");
        if (!options.Bootstrap.IsEmpty && Server.KeyIssuer.Unavailable(options) is { } reason)
            throw new InvalidOperationException($"Coordinator:Bootstrap names identities verified by a key, but {reason}.");
        if (options.Bootstrap.People is { Count: > 0 } people && Server.KeyIssuer.PersonUnavailable(options) is { } refusal)
            throw new InvalidOperationException($"Coordinator:Bootstrap names people ({string.Join(", ", people)}), but {refusal}.");
        return options;
    }
}
