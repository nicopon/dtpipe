using System.Text.Json;
using DtPipe.Cli.Security;
using DtPipe.Sessions;

namespace DtPipe.Cli.Studio;

/// <summary>The agent's connection as <c>studio.json</c> keeps it: no secret, ever.</summary>
public sealed record AgentSettings(string Provider, string Url, string? Model, int? Timeout);

/// <summary>The coordinator's connection as <c>studio.json</c> keeps it: its origin and how the person signs in (<c>browser</c> or <c>key</c>), never the key.</summary>
public sealed record CoordinatorSettings(string Origin, string Auth);

/// <summary>What <c>studio.json</c> holds, section by section; a section that cannot be read is null.</summary>
public sealed record RememberedSettings(AgentSettings? Agent, CoordinatorSettings? Coordinator);

/// <summary>
/// What the studio remembers for the person on this machine: <c>studio.json</c> under <see cref="UserStatePaths.Root"/>, holding
/// no secret, and the secrets in the keyring's blob under aliases derived from the address they belong to
/// (<c>studio.llm:&lt;address&gt;</c>, <c>studio.coordinator:&lt;origin&gt;</c>), so a key is never offered to another address. A file that cannot be read, or that a
/// newer studio wrote, is ignored with a warning and never stops the start. Nothing the command line gives (a flag, the folder)
/// is written here.
/// </summary>
public sealed class StudioSettings(string path, ISecretsManager secrets)
{
    public const int Version = 1;

    private static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web) { WriteIndented = true };

    public static string DefaultPath() => Path.Combine(UserStatePaths.Root(), "studio.json");

    /// <summary>Why the file was ignored the last time it was read, or null.</summary>
    public string? Warning { get; private set; }

    /// <summary>False once the keyring refused a read or a write: a key typed after that lasts the session only.</summary>
    public bool KeyringUsable { get; private set; } = true;

    /// <summary>
    /// What the file holds: the agent, the coordinators known (an origin each, by address), the one that is active and whether it
    /// is connected again at start, and the folders the page opened, the most recent first. A folder is never opened at start.
    /// </summary>
    private sealed record FileModel(
        int Version, AgentSettings? Agent, CoordinatorSettings[]? Coordinators = null, string? ActiveCoordinator = null, bool Reconnect = false,
        string[]? Folders = null);

    /// <summary>How many recent folders the file keeps.</summary>
    public const int MaxFolders = 10;

    /// <summary>The remembered agent, or null: nothing remembered, or a file that is ignored (see <see cref="Warning"/>).</summary>
    public AgentSettings? ReadAgent() => Read().Agent;

    /// <summary>
    /// The coordinator to connect at start: the active one when the file asks for the reconnection, or null (nothing remembered,
    /// no reconnection, or a file that is ignored: see <see cref="Warning"/>).
    /// </summary>
    public CoordinatorSettings? ReadCoordinator() => Read().Coordinator;

    /// <summary>Every section the file holds and this studio reads; one that is invalid is left out, and <see cref="Warning"/> says why.</summary>
    public RememberedSettings Read()
    {
        Warning = null;
        var file = ReadFile();
        if (file is null) return new RememberedSettings(null, null);
        var coordinators = (file.Coordinators ?? []).Select(ValidCoordinator).OfType<CoordinatorSettings>().ToList();
        var active = file is { Reconnect: true, ActiveCoordinator: { } origin } && IsOrigin(origin)
            ? coordinators.FirstOrDefault(c => SameAddress(c.Origin, origin))
            : null;
        return new RememberedSettings(ValidAgent(file.Agent), active);
    }

    private static bool SameAddress(string a, string b) => NormalizeUrl(a) == NormalizeUrl(b);

    /// <summary>
    /// The folders the page opened and the person asked to remember, the most recent first, each once, as full paths. One that is
    /// not a full path is left out, and <see cref="Warning"/> says so.
    /// </summary>
    public IReadOnlyList<string> ReadFolders()
    {
        Warning = null;
        var folders = new List<string>();
        foreach (var folder in ReadFile()?.Folders ?? [])
        {
            if (string.IsNullOrWhiteSpace(folder) || !Path.IsPathFullyQualified(folder)) { Ignore<string>("a recent folder is not a full path"); continue; }
            var normalized = StudioFolder.Normalize(folder);
            if (!folders.Any(f => string.Equals(f, normalized, StudioFolder.PathComparison))) folders.Add(normalized);
        }
        return folders.Take(MaxFolders).ToList();
    }

    private FileModel? ReadFile()
    {
        if (!File.Exists(path)) return null;
        try
        {
            var file = JsonSerializer.Deserialize<FileModel>(File.ReadAllText(path), Json);
            if (file is null) return Ignore<FileModel>("it is empty");
            if (file.Version != Version) return Ignore<FileModel>($"it is of version {file.Version}, this studio reads version {Version}");
            return file;
        }
        catch (Exception ex) when (ex is JsonException or IOException or UnauthorizedAccessException)
        {
            return Ignore<FileModel>("it cannot be read");
        }
    }

    private AgentSettings? ValidAgent(AgentSettings? agent)
    {
        if (agent is null) return null;
        if (agent.Provider is not ("ollama" or "openai")) return Ignore<AgentSettings>("its agent names no known provider");
        if (!IsAddress(agent.Url)) return Ignore<AgentSettings>("its agent has no http or https address");
        return agent;
    }

    private CoordinatorSettings? ValidCoordinator(CoordinatorSettings? coordinator)
    {
        if (coordinator is null) return null;
        if (coordinator.Auth is not ("browser" or "key")) return Ignore<CoordinatorSettings>("its coordinator signs in neither through the browser nor with a key");
        if (!IsOrigin(coordinator.Origin)) return Ignore<CoordinatorSettings>("its coordinator has no http or https origin");
        return coordinator;
    }

    /// <summary>Says why a part of the file is left out, and answers null for the caller to return.</summary>
    private T? Ignore<T>(string reason) where T : class
    {
        Warning = $"The studio's settings ({path}) are ignored: {reason}.";
        return null;
    }

    public void WriteAgent(AgentSettings agent) => Update(file => file with { Agent = agent });

    /// <summary>Forgets the remembered agent; the file stays only for what else it holds.</summary>
    public void ClearAgent() => Clear(file => file with { Agent = null });

    /// <summary>Keeps the coordinator among those known, by its address, makes it the active one and asks for its reconnection at start.</summary>
    public void WriteCoordinator(CoordinatorSettings coordinator) => Update(file => file with
    {
        Coordinators = [.. (file.Coordinators ?? []).Where(c => !SameAddress(c.Origin, coordinator.Origin)), coordinator],
        ActiveCoordinator = coordinator.Origin,
        Reconnect = true,
    });

    /// <summary>Forgets the active coordinator: its entry goes, and nothing is connected at the next start.</summary>
    public void ClearCoordinator() => Clear(file => file with
    {
        Coordinators = (file.Coordinators ?? []).Where(c => file.ActiveCoordinator is null || !SameAddress(c.Origin, file.ActiveCoordinator)).ToArray(),
        ActiveCoordinator = null,
        Reconnect = false,
    });

    /// <summary>Puts the folder first among the recent ones, once, and keeps the most recent <see cref="MaxFolders"/>.</summary>
    public void RememberFolder(string folder)
    {
        var normalized = StudioFolder.Normalize(folder);
        var kept = ReadFolders().Where(f => !string.Equals(f, normalized, StudioFolder.PathComparison));
        Update(file => file with { Folders = [.. new[] { normalized }.Concat(kept).Take(MaxFolders)] });
    }

    /// <summary>Changes one section and keeps the others as they are; a file that cannot be read is replaced, since the person asked to write.</summary>
    private void Update(Func<FileModel, FileModel> change) => Write(change(ReadFile() ?? new FileModel(Version, null)));

    private void Clear(Func<FileModel, FileModel> change)
    {
        if (File.Exists(path)) Update(change);
    }

    /// <summary>The file is written beside itself and moved over the old one, with the person's rights alone where the system has them.</summary>
    private void Write(FileModel model)
    {
        var directory = Path.GetDirectoryName(path);
        if (!string.IsNullOrEmpty(directory)) Directory.CreateDirectory(directory);
        var temporary = $"{path}.{Environment.ProcessId}.tmp";
        var options = new FileStreamOptions { Mode = FileMode.Create, Access = FileAccess.Write };
        if (!OperatingSystem.IsWindows()) options.UnixCreateMode = UnixFileMode.UserRead | UnixFileMode.UserWrite;
        try
        {
            using (var stream = new FileStream(temporary, options))
            using (var writer = new StreamWriter(stream))
                writer.Write(JsonSerializer.Serialize(model, Json));
            File.Move(temporary, path, overwrite: true);
        }
        finally
        {
            if (File.Exists(temporary)) File.Delete(temporary);
        }
    }

    private static bool IsAddress(string? url) =>
        Uri.TryCreate(url, UriKind.Absolute, out var uri) && uri.Scheme is "http" or "https";

    /// <summary>An origin is an address with no path, no query, no fragment and no credential, as <see cref="CoordinatorLink"/> reads one.</summary>
    private static bool IsOrigin(string? url) =>
        Uri.TryCreate(url, UriKind.Absolute, out var uri) && uri.Scheme is "http" or "https"
        && uri.UserInfo.Length == 0 && uri.AbsolutePath == "/" && uri.Query.Length == 0 && uri.Fragment.Length == 0;

    /// <summary>
    /// The address as an alias reads it: scheme and host in lower case, the default port dropped, the trailing slash of the path
    /// dropped, the path and the query kept, no credential and no fragment.
    /// </summary>
    public static string NormalizeUrl(string url)
    {
        var uri = new Uri(url, UriKind.Absolute);
        var port = uri.IsDefaultPort ? "" : $":{uri.Port}";
        var path = uri.AbsolutePath.TrimEnd('/');
        return $"{uri.Scheme.ToLowerInvariant()}://{uri.Host.ToLowerInvariant()}{port}{path}{uri.Query}";
    }

    /// <summary>The address as a person is shown it: neither the credential, nor the query, nor the fragment is part of it.</summary>
    public static string Shown(string url) =>
        Uri.TryCreate(url, UriKind.Absolute, out var uri) && uri.Scheme is "http" or "https" ? NormalizeUrl(url).Split('?')[0] : "";

    public static string LlmAlias(string url) => ReservedAliases.StudioPrefix + "llm:" + NormalizeUrl(url);

    public static string CoordinatorAlias(string origin) => ReservedAliases.StudioPrefix + "coordinator:" + NormalizeUrl(origin);

    /// <summary>The key kept for this model address, or null when there is none or the keyring cannot be read.</summary>
    public string? ReadLlmKey(string url) => ReadKey(LlmAlias(url));

    /// <summary>Keeps the key for this model address; false when the keyring refuses, and the key then lasts the session alone.</summary>
    public bool StoreLlmKey(string url, string key) => StoreKey(LlmAlias(url), key);

    public void ForgetLlmKey(string url) => ForgetKey(LlmAlias(url));

    /// <summary>The key kept for this coordinator, or null when there is none or the keyring cannot be read.</summary>
    public string? ReadCoordinatorKey(string origin) => ReadKey(CoordinatorAlias(origin));

    /// <summary>Keeps the key for this coordinator; false when the keyring refuses, and the key then lasts the session alone.</summary>
    public bool StoreCoordinatorKey(string origin, string key) => StoreKey(CoordinatorAlias(origin), key);

    public void ForgetCoordinatorKey(string origin) => ForgetKey(CoordinatorAlias(origin));

    private string? ReadKey(string alias)
    {
        try
        {
            var key = secrets.GetSecret(alias);
            KeyringUsable = true;
            return key;
        }
        catch (Exception)
        {
            KeyringUsable = false;
            return null;
        }
    }

    private bool StoreKey(string alias, string key)
    {
        try
        {
            secrets.SetSecret(alias, key);
            KeyringUsable = true;
            return true;
        }
        catch (Exception)
        {
            KeyringUsable = false;
            return false;
        }
    }

    private void ForgetKey(string alias)
    {
        try { secrets.DeleteSecret(alias); }
        catch (Exception) { KeyringUsable = false; }
    }
}
