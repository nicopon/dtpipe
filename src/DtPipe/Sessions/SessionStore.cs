using System.Text.Json;
using System.Text.Json.Serialization;

namespace DtPipe.Sessions;

/// <summary>Session metadata, as persisted. Kept small: it is bookkeeping, not a cache index.</summary>
public sealed class SessionMetadata
{
    public int Version { get; set; } = 1;
    public string? CreatedAt { get; set; }
    public string? LastUsedAt { get; set; }
    public int TtlDays { get; set; } = SessionStore.DefaultTtlDays;
    /// <summary>Whether the one-time opt-in notice has been shown for this session.</summary>
    public bool NoticeShown { get; set; }
}

/// <summary>
/// The on-disk store for one session's artefacts, under the .dtpipe/ convention SchemaStore
/// already established — the same directory, extended, not a second one beside it.
///
/// Layout:
/// <code>
///   .dtpipe/.gitignore              "*" — written at creation
///   .dtpipe/sessions/&lt;name&gt;/session.json
///   .dtpipe/sessions/&lt;name&gt;/&lt;checkpoint-key&gt;/…
/// </code>
///
/// Nothing here is created by resolving a session. The store comes into being the first time
/// something is actually materialised, which is what keeps an ordinary run traceless.
/// </summary>
public sealed class SessionStore
{
    public const int DefaultTtlDays = 7;
    public const string TtlEnvironmentVariable = "DTPIPE_SESSION_TTL_DAYS";

    private static readonly JsonSerializerOptions SerializerOptions = new()
    {
        WriteIndented = true,
        PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
    };

    public SessionIdentity Identity { get; }

    /// <summary>The .dtpipe/ root this session lives under.</summary>
    public string RootPath => Identity.RootPath;

    /// <summary>This session's own directory.</summary>
    public string SessionPath => Path.Combine(RootPath, "sessions", Identity.Name);

    public string MetadataPath => Path.Combine(SessionPath, "session.json");

    public SessionStore(SessionIdentity identity) => Identity = identity;

    public static SessionStore Resolve(string? explicitName = null, string? workingDirectory = null)
        => new(SessionResolver.Resolve(explicitName, workingDirectory));

    /// <summary>True once anything has been materialised for this session.</summary>
    public bool Exists => Directory.Exists(SessionPath);

    /// <summary>
    /// Creates the store if needed and stamps the session as used. Call this at the moment of
    /// materialisation, never before.
    /// </summary>
    public SessionMetadata EnsureCreated()
    {
        EnsureIgnored(RootPath);
        Directory.CreateDirectory(SessionPath);

        var meta = ReadMetadata() ?? new SessionMetadata
        {
            CreatedAt = DateTime.UtcNow.ToString("O"),
            TtlDays = ConfiguredTtlDays()
        };

        meta.LastUsedAt = DateTime.UtcNow.ToString("O");
        WriteMetadata(meta);
        return meta;
    }

    /// <summary>
    /// Writes <c>.dtpipe/.gitignore</c> containing <c>*</c> when the root is created, so the store
    /// ignores itself whatever the project's own .gitignore says. Verified on 2026-08-28 that
    /// .dtpipe/ is not ignored in this repository — leaving that to each project's discipline is
    /// how a checkpoint ends up in a commit.
    /// </summary>
    public static void EnsureIgnored(string rootPath)
    {
        Directory.CreateDirectory(rootPath);
        var gitignore = Path.Combine(rootPath, ".gitignore");
        if (!File.Exists(gitignore)) File.WriteAllText(gitignore, "*\n");
    }

    public SessionMetadata? ReadMetadata()
    {
        if (!File.Exists(MetadataPath)) return null;
        try
        {
            return JsonSerializer.Deserialize<SessionMetadata>(File.ReadAllText(MetadataPath), SerializerOptions);
        }
        catch
        {
            // A corrupt metadata file must not fail a run: the session is treated as new and
            // rewritten. There is nothing here that cannot be regenerated.
            return null;
        }
    }

    public void WriteMetadata(SessionMetadata meta)
    {
        Directory.CreateDirectory(SessionPath);
        File.WriteAllText(MetadataPath, JsonSerializer.Serialize(meta, SerializerOptions));
    }

    /// <summary>Where one checkpoint's artefacts live, addressed by content.</summary>
    public string CheckpointPath(string checkpointKey) => Path.Combine(SessionPath, checkpointKey);

    internal static int ConfiguredTtlDays()
    {
        var raw = Environment.GetEnvironmentVariable(TtlEnvironmentVariable);
        return int.TryParse(raw, out var days) && days > 0 ? days : DefaultTtlDays;
    }

    /// <summary>Every session directory under a given .dtpipe/ root.</summary>
    public static IEnumerable<string> EnumerateSessionPaths(string rootPath)
    {
        var sessions = Path.Combine(rootPath, "sessions");
        return Directory.Exists(sessions)
            ? Directory.EnumerateDirectories(sessions)
            : Enumerable.Empty<string>();
    }
}
