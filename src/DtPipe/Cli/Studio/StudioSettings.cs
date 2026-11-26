using System.Text.Json;
using DtPipe.Cli.Security;
using DtPipe.Sessions;

namespace DtPipe.Cli.Studio;

/// <summary>The agent's connection as <c>studio.json</c> keeps it: no secret, ever.</summary>
public sealed record AgentSettings(string Provider, string Url, string? Model, int? Timeout, int? NumCtx = null);

/// <summary>The coordinator's connection as <c>studio.json</c> keeps it: its origin and how the person signs in (<c>browser</c> or <c>key</c>), never the key.</summary>
public sealed record CoordinatorSettings(string Origin, string Auth);

/// <summary>What <c>studio.json</c> holds as the connections to make at start, section by section; a section that cannot be read is null.</summary>
public sealed record RememberedSettings(AgentSettings? Agent, CoordinatorSettings? Coordinator);

/// <summary>
/// What the studio remembers for the person on this machine: <c>studio.json</c> under <see cref="UserStatePaths.Root"/>, holding
/// no secret, with keyring operations delegated to <see cref="StudioSecretStore"/>. A file that cannot be read, or that a
/// newer studio wrote, is ignored with a warning and never stops the start. Nothing the command line gives (a flag, the folder)
/// is written here.
/// </summary>
public sealed class StudioSettings(string path, ISecretsManager secrets)
{
    public const int Version = 1;

    private static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web) { WriteIndented = true };

    private readonly object _fileLock = new();
    private readonly StudioSecretStore _secrets = new(secrets);

    private delegate T? Validator<T>(T? item, ref string? warning) where T : class;

    public StudioSecretStore Secrets => _secrets;

    public static string DefaultPath() => Path.Combine(UserStatePaths.Root(), "studio.json");

    /// <summary>
    /// What the file holds: the agent and the coordinator to connect again at start, and three lists of what the page used with
    /// success and was asked to remember, the most recent first, each identity once: the agents, the coordinators and the
    /// folders. A list is a history the person edits; it never decides what is connected at start, and a folder is never opened
    /// at start.
    /// </summary>
    private sealed record FileModel(
        int Version, AgentSettings? Agent, CoordinatorSettings? Coordinator = null, AgentSettings[]? Agents = null,
        CoordinatorSettings[]? Coordinators = null, string[]? Folders = null);

    /// <summary>
    /// Every section the file holds and this studio reads: the agent and the coordinator to connect at start, each null when
    /// nothing is remembered or the section is ignored. One that is invalid is left out, and <paramref name="warning"/> says why.
    /// </summary>
    public RememberedSettings Read(out string? warning)
    {
        lock (_fileLock)
        {
            warning = null;
            var file = ReadFile(ref warning);
            return file is null
                ? new RememberedSettings(null, null)
                : new RememberedSettings(ValidAgent(file.Agent, ref warning), ValidCoordinator(file.Coordinator, ref warning));
        }
    }

    public RememberedSettings Read() => Read(out _);

    private static bool SameAddress(string a, string b) => NormalizeUrl(a) == NormalizeUrl(b);

    /// <summary>The identity of a recent agent: its provider and its normalized address.</summary>
    public static bool SameAgent(AgentSettings a, AgentSettings b) => a.Provider == b.Provider && SameAddress(a.Url, b.Url);

    /// <summary>The identity of a recent coordinator: its normalized origin.</summary>
    public static bool SameCoordinator(CoordinatorSettings a, CoordinatorSettings b) => SameAddress(a.Origin, b.Origin);

    /// <summary>The identity of a recent folder: its full path.</summary>
    public static bool SameFolder(string a, string b) => string.Equals(a, b, StudioFolder.PathComparison);

    /// <summary>The agents the page connected and the person asked to remember, the most recent first, each provider and address once.</summary>
    public IReadOnlyList<AgentSettings> ReadAgents(out string? warning) =>
        ReadRecent(file => file.Agents, ValidAgent, SameAgent, out warning);

    public IReadOnlyList<AgentSettings> ReadAgents() => ReadAgents(out _);

    /// <summary>The coordinators the page connected and the person asked to remember, the most recent first, each origin once.</summary>
    public IReadOnlyList<CoordinatorSettings> ReadCoordinators(out string? warning) =>
        ReadRecent(file => file.Coordinators, ValidCoordinator, SameCoordinator, out warning);

    public IReadOnlyList<CoordinatorSettings> ReadCoordinators() => ReadCoordinators(out _);

    /// <summary>
    /// The folders the page opened and the person asked to remember, the most recent first, each once, as full paths. One that is
    /// not a full path is left out, and <paramref name="warning"/> says so.
    /// </summary>
    public IReadOnlyList<string> ReadFolders(out string? warning) =>
        ReadRecent(file => file.Folders, ValidFolder, SameFolder, out warning);

    public IReadOnlyList<string> ReadFolders() => ReadFolders(out _);

    private IReadOnlyList<T> ReadRecent<T>(Func<FileModel, T?[]?> list, Validator<T> valid, Func<T, T, bool> same, out string? warning) where T : class
    {
        lock (_fileLock)
        {
            warning = null;
            return Distinct(ReadFile(ref warning) is { } file ? list(file) : null, valid, same, ref warning);
        }
    }

    private List<T> RemoveRecent<T>(
        Func<FileModel, T?[]?> list,
        Func<FileModel, T?>? activeSelector,
        Validator<T> valid,
        Func<T, T, bool> same,
        Func<T, bool> match,
        Func<FileModel, T?, T[], FileModel> apply) where T : class
    {
        var removed = new List<T>();
        Clear(file =>
        {
            string? ignored = null;
            var recent = Distinct(list(file), valid, same, ref ignored);
            removed.AddRange(recent.Where(match));
            var active = activeSelector is not null ? valid(activeSelector(file), ref ignored) : null;
            var activeGoes = active is not null && match(active);
            if (activeGoes) removed.Add(active!);
            var nextActive = activeSelector is null ? null : activeGoes ? null : activeSelector(file);
            return apply(file, nextActive, recent.Where(item => !match(item)).ToArray());
        });
        return removed;
    }

    /// <summary>The valid entries of a list, each identity once, the first (the most recent) kept. A list has no bound.</summary>
    private static List<T> Distinct<T>(IEnumerable<T?>? stored, Validator<T> valid, Func<T, T, bool> same, ref string? warning) where T : class
    {
        var kept = new List<T>();
        foreach (var item in stored ?? [])
            if (valid(item, ref warning) is { } entry && !kept.Any(k => same(k, entry))) kept.Add(entry);
        return kept;
    }

    private static List<T> Distinct<T>(IEnumerable<T?>? stored, Validator<T> valid, Func<T, T, bool> same) where T : class
    {
        string? ignored = null;
        return Distinct(stored, valid, same, ref ignored);
    }

    /// <summary>The list with the entry put first, once.</summary>
    private static T[] Recent<T>(IReadOnlyList<T> current, T entry, Func<T, T, bool> same) =>
        [.. new[] { entry }.Concat(current.Where(c => !same(c, entry)))];

    private FileModel? ReadFile(ref string? warning)
    {
        if (!File.Exists(path)) return null;
        try
        {
            var file = JsonSerializer.Deserialize<FileModel>(File.ReadAllText(path), Json);
            if (file is null) return Ignore<FileModel>("it is empty", ref warning);
            if (file.Version != Version) return Ignore<FileModel>($"it is of version {file.Version}, this studio reads version {Version}", ref warning);
            return file;
        }
        catch (Exception ex) when (ex is JsonException or IOException or UnauthorizedAccessException)
        {
            return Ignore<FileModel>("it cannot be read", ref warning);
        }
    }

    private AgentSettings? ValidAgent(AgentSettings? agent, ref string? warning)
    {
        if (agent is null) return null;
        if (agent.Provider is not ("ollama" or "openai")) return Ignore<AgentSettings>("its agent names no known provider", ref warning);
        if (!IsAddress(agent.Url)) return Ignore<AgentSettings>("its agent has no http or https address", ref warning);
        if (agent.NumCtx is { } window && !StudioAgentClients.IsNumCtx(window)) return Ignore<AgentSettings>("its agent's context size is out of bounds", ref warning);
        return agent;
    }

    private CoordinatorSettings? ValidCoordinator(CoordinatorSettings? coordinator, ref string? warning)
    {
        if (coordinator is null) return null;
        if (coordinator.Auth is not ("browser" or "key")) return Ignore<CoordinatorSettings>("its coordinator signs in neither through the browser nor with a key", ref warning);
        if (!IsOrigin(coordinator.Origin)) return Ignore<CoordinatorSettings>("its coordinator has no http or https origin", ref warning);
        return coordinator;
    }

    private string? ValidFolder(string? folder, ref string? warning)
    {
        if (string.IsNullOrWhiteSpace(folder) || !Path.IsPathFullyQualified(folder)) return Ignore<string>("a recent folder is not a full path", ref warning);
        return StudioFolder.Normalize(folder);
    }

    /// <summary>Says why a part of the file is left out, and answers null for the caller to return.</summary>
    private T? Ignore<T>(string reason, ref string? warning) where T : class
    {
        warning = $"The studio's settings ({path}) are ignored: {reason}.";
        return null;
    }

    /// <summary>Makes the agent the one connected again at start, and puts it first among the recent agents, once, with the model, delay and context it was last used with.</summary>
    public void WriteAgent(AgentSettings agent) => Update(file => file with
    {
        Agent = agent,
        Agents = Recent(Distinct(file.Agents, ValidAgent, SameAgent), agent, SameAgent),
    });

    /// <summary>
    /// Removes the recent agents the page shows at this provider and address, the remembered agent too when it is one of them, and
    /// forgets the key kept for each address. The key is deleted by its alias, never read.
    /// </summary>
    public void RemoveAgent(string provider, string url)
    {
        var shown = Shown(url);
        var removed = RemoveRecent(
            file => file.Agents,
            file => file.Agent,
            ValidAgent,
            SameAgent,
            agent => agent.Provider == provider && Shown(agent.Url) == shown,
            (file, active, remaining) => file with { Agent = active, Agents = remaining });
        foreach (var address in removed.Select(a => a.Url).Distinct()) _secrets.ForgetLlmKey(address);
    }

    /// <summary>Makes the coordinator the one connected again at start, optionally putting it first among the recent ones in the same write.</summary>
    public void WriteCoordinator(CoordinatorSettings coordinator, bool remember = false) => Update(file => file with
    {
        Coordinator = coordinator,
        Coordinators = remember ? Recent(Distinct(file.Coordinators, ValidCoordinator, SameCoordinator), coordinator, SameCoordinator) : file.Coordinators,
    });

    /// <summary>Puts the coordinator first among the recent ones, once, with how the person last signed in to it.</summary>
    public void RememberCoordinator(CoordinatorSettings coordinator) => Update(file => file with
    {
        Coordinators = Recent(Distinct(file.Coordinators, ValidCoordinator, SameCoordinator), coordinator, SameCoordinator),
    });

    /// <summary>
    /// Removes the recent coordinator at this origin, the remembered coordinator too when it is that one, and forgets the key kept
    /// for the origin. The key is deleted by its alias, never read.
    /// </summary>
    public void RemoveCoordinator(string origin)
    {
        var shown = Shown(origin);
        var removed = RemoveRecent(
            file => file.Coordinators,
            file => file.Coordinator,
            ValidCoordinator,
            SameCoordinator,
            coordinator => Shown(coordinator.Origin) == shown,
            (file, active, remaining) => file with { Coordinator = active, Coordinators = remaining });
        foreach (var address in removed.Select(c => c.Origin).Distinct()) _secrets.ForgetCoordinatorKey(address);
    }

    /// <summary>Puts the folder first among the recent ones, once.</summary>
    public void RememberFolder(string folder)
    {
        var normalized = StudioFolder.Normalize(folder);
        Update(file => file with { Folders = Recent(Distinct(file.Folders, ValidFolder, SameFolder), normalized, SameFolder) });
    }

    /// <summary>Removes the folder from the recent ones, whether or not it is still on the disk.</summary>
    public void RemoveFolder(string folder)
    {
        var normalized = StudioFolder.Normalize(folder);
        RemoveRecent(
            file => file.Folders,
            null,
            ValidFolder,
            SameFolder,
            f => SameFolder(f, normalized),
            (file, _, remaining) => file with { Folders = remaining });
    }

    /// <summary>Changes one section and keeps the others as they are; a file that cannot be read is replaced, since the person asked to write.</summary>
    private void Update(Func<FileModel, FileModel> change)
    {
        lock (_fileLock) UpdateLocked(change);
    }

    private void Clear(Func<FileModel, FileModel> change)
    {
        lock (_fileLock)
        {
            if (File.Exists(path)) UpdateLocked(change);
        }
    }

    private void UpdateLocked(Func<FileModel, FileModel> change)
    {
        string? ignored = null;
        Write(change(ReadFile(ref ignored) ?? new FileModel(Version, null)));
    }

    /// <summary>The file is written beside itself and moved over the old one, with the person's rights alone where the system has them.</summary>
    private void Write(FileModel model)
    {
        var directory = Path.GetDirectoryName(path);
        if (!string.IsNullOrEmpty(directory)) Directory.CreateDirectory(directory);
        var temporary = $"{path}.{Guid.NewGuid():N}.tmp";
        var options = new FileStreamOptions { Mode = FileMode.CreateNew, Access = FileAccess.Write };
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

    public static string LlmAlias(string url) => StudioSecretStore.LlmAlias(url);

    public static string CoordinatorAlias(string origin) => StudioSecretStore.CoordinatorAlias(origin);

    public string? ReadLlmKey(string url) => _secrets.ReadLlmKey(url);

    public bool StoreLlmKey(string url, string key) => _secrets.StoreLlmKey(url, key);

    public void ForgetLlmKey(string url) => _secrets.ForgetLlmKey(url);

    public string? ReadCoordinatorKey(string origin) => _secrets.ReadCoordinatorKey(origin);

    public bool StoreCoordinatorKey(string origin, string key) => _secrets.StoreCoordinatorKey(origin, key);

    public void ForgetCoordinatorKey(string origin) => _secrets.ForgetCoordinatorKey(origin);

    public IReadOnlySet<string> KeptAliases(out bool keyringUsable) => _secrets.KeptAliases(out keyringUsable);

    public IReadOnlySet<string> KeptAliases() => _secrets.KeptAliases();
}
