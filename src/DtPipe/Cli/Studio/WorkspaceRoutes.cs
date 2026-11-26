using DtPipe.Cli.Security;
using Microsoft.AspNetCore.Http;

namespace DtPipe.Cli.Studio;

/// <summary>
/// The folder the studio works on, which the page may change: a path typed or a recent one, tested before it is opened and
/// remembered among the recent folders only when the person asks. Every request that uses the folder reads it in the slot,
/// never a copy; a change is refused while the page holds unsaved changes, a request uses the folder, an engine action or a
/// run goes on, or the agent's turn runs or a card waits. It moves the process's current directory, starts a new conversation
/// with the agent, and forgets the run the old folder ended; the page then reads everything again.
/// </summary>
internal sealed class WorkspaceRoutes(
    WorkspaceSlot workspace,
    AgentSlot agentSlot,
    EngineSlot engineSlot,
    StudioSettings? settings,
    ISecretsManager secrets,
    string startFolder,
    Func<bool> runInProgress,
    Action onWorkspaceChanged,
    Func<StudioAgentSetup, StudioAgent> createAgent)
{
    private sealed record WorkspaceRequest(string? Path, bool Remember, bool Unsaved);

    public void Register(StudioRouteTable routes)
    {
        routes.Map("GET", "/api/workspace", context => StudioHttp.JsonAsync(context, View()), RouteLocks.None);
        routes.Map("POST", "/api/workspace/test", TestAsync, RouteLocks.None);
        routes.Map("DELETE", "/api/workspace/recents", ForgetAsync, RouteLocks.None);
        routes.Map("PUT", "/api/workspace", OpenAsync, RouteLocks.None);
    }

    private async Task TestAsync(HttpContext context)
    {
        var tested = FolderAt((await StudioHttp.BodyAsync<WorkspaceRequest>(context)).Path);
        await StudioHttp.JsonAsync(context, new { root = tested.Root, name = NameOf(tested.Root), jobs = tested.Jobs().Count });
    }

    private async Task ForgetAsync(HttpContext context) =>
        await StudioHttp.JsonAsync(context, ForgetRecent(await StudioHttp.BodyAsync<WorkspaceRequest>(context)));

    private async Task OpenAsync(HttpContext context) =>
        await StudioHttp.JsonAsync(context, await OpenWorkspaceAsync(await StudioHttp.BodyAsync<WorkspaceRequest>(context), context.RequestAborted));

    /// <summary>
    /// The folder as the page reads it: where it is, how it was chosen, the one the studio started on and the recent ones, the
    /// folder in place left out (a display rule: the file keeps its entry).
    /// </summary>
    private object View(string? warning = null)
    {
        var root = workspace.Current.Folder.Root;
        string? settingsWarning = null;
        var all = settings is { } s ? s.ReadFolders(out settingsWarning) : [];
        var currentRemembered = all.Any(f => StudioSettings.SameFolder(f, root));
        var recents = all.Where(f => !StudioSettings.SameFolder(f, root)).ToList();
        return new
        {
            root,
            name = NameOf(root),
            source = workspace.Source,
            start = startFolder,
            recents,
            currentRemembered,
            canRemember = settings is not null,
            settingsWarning,
            warning,
        };
    }

    private static string NameOf(string root) => Path.GetFileName(root) is { Length: > 0 } name ? name : root;

    /// <summary>Expands a leading <c>~</c> against <paramref name="home"/> (or the user profile), refusing when none is configured.</summary>
    public static string ExpandHome(string path, string? home = null)
    {
        if (path != "~" && !path.StartsWith("~/", StringComparison.Ordinal) && !path.StartsWith("~" + Path.DirectorySeparatorChar, StringComparison.Ordinal))
            return path;
        var profile = home ?? Environment.GetFolderPath(Environment.SpecialFolder.UserProfile);
        if (string.IsNullOrWhiteSpace(profile))
            throw new StudioRefusal(400, "Cannot expand '~': the home directory is not set. Type the folder's full path.");
        if (path.Length <= 2) return profile;
        return Path.Join(profile, path[2..]);
    }

    /// <summary>The folder at a path the person typed, opened as the studio opens one, or refused with the reason: nothing changes.</summary>
    private static StudioFolder FolderAt(string? typed)
    {
        var path = typed?.Trim() ?? "";
        if (path.Length == 0) throw new StudioRefusal(400, "Type the folder's full path.");
        path = ExpandHome(path);
        if (!Path.IsPathFullyQualified(path)) throw new StudioRefusal(400, $"'{path}' is not a full path: type the folder's whole path.");
        StudioFolder folder;
        try { folder = new StudioFolder(path); }
        catch (DirectoryNotFoundException ex) { throw new StudioRefusal(File.Exists(path) ? 400 : 404, ex.Message); }
        try
        {
            using var entries = Directory.EnumerateFileSystemEntries(folder.Root).GetEnumerator();
            entries.MoveNext();
        }
        catch (Exception ex) when (ex is UnauthorizedAccessException or IOException)
        {
            throw new StudioRefusal(400, $"{folder.Root} cannot be read: {ex.Message}");
        }
        return folder;
    }

    /// <summary>
    /// Opens the folder the person chose, in place of the one open. The page alone knows whether its steps are saved: it says so,
    /// and the change waits for them to be saved or discarded. The folder joins the recent ones only when the person asks.
    /// </summary>
    private async Task<object> OpenWorkspaceAsync(WorkspaceRequest request, CancellationToken ct)
    {
        var folder = FolderAt(request.Path);
        if (request.Unsaved) throw new StudioRefusal(409, "The open job has changes that are not saved: save them, or discard them, before opening another folder.");
        await ChangeWorkspaceAsync(folder, ct);
        string? warning = null;
        if (request.Remember && settings is { } store)
        {
            try { store.RememberFolder(folder.Root); }
            catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
            {
                warning = "The folder is open, but the settings file cannot be written: it is not among the recent folders.";
            }
        }
        return View(warning);
    }

    /// <summary>
    /// Puts <paramref name="folder"/> in place of the folder the studio works on, or refuses with 409 naming what still uses the
    /// one in place. The agent is closed in the same step as the folder moves, and a new conversation starts on its model.
    /// </summary>
    private async Task ChangeWorkspaceAsync(StudioFolder folder, CancellationToken ct)
    {
        if (runInProgress()) throw new StudioRefusal(409, "A run is in progress: stop it, or wait for it, before opening another folder.");
        var next = new StudioWorkspace(folder, secrets);
        using (engineSlot.Take("A change of folder"))
        {
            StudioAgent? closed = null;
            try
            {
                await workspace.ReplaceAsync(next, "page", () =>
                {
                    var agent = agentSlot.Current;
                    if (agent?.TryClose("opening another folder") is { } reason) return reason;
                    closed = agent;
                    return null;
                }, ct);
            }
            finally
            {
                // The conversation was about the folder left: a new one starts on the same model, as a change of model does.
                if (closed is { } old) await agentSlot.RenewAsync(old, () => createAgent(old.Renewed()));
            }
            onWorkspaceChanged();
        }
    }

    /// <summary>Removes a folder from the recent ones, by its path as the page shows it, whether or not it is still on the disk. The folder in place is untouched.</summary>
    private object ForgetRecent(WorkspaceRequest request)
    {
        var store = settings ?? throw new StudioRefusal(404, "This studio keeps no recent folders.");
        var path = request.Path?.Trim() ?? "";
        if (!Path.IsPathFullyQualified(path)) throw new StudioRefusal(400, $"'{path}' is not a full path.");
        try { store.RemoveFolder(path); }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            return View("The settings file cannot be written: the folder stays among the recent ones.");
        }
        return View();
    }
}
