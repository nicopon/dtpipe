using Microsoft.AspNetCore.Http;

namespace DtPipe.Cli.Studio;

/// <summary>
/// The folder the studio works on, which the page may change: a path typed or a recent one, tested before it is opened and
/// remembered among the recent folders only when the person asks. Every request that uses the folder reads it in the slot,
/// never a copy; a change is refused while the page holds unsaved changes, a request uses the folder, an engine action or a
/// run goes on, or the agent's turn runs or a card waits. It moves the process's current directory, starts a new conversation
/// with the agent, and forgets the run the old folder ended; the page then reads everything again.
/// </summary>
public sealed partial class StudioServer
{
    private sealed record WorkspaceRequest(string? Path, bool Remember, bool Unsaved);

    private readonly string _start;

    private StudioFolder Folder => _workspace.Current.Folder;

    private StudioLocalFiles Local => _workspace.Current.Local;

    private StudioVariables Variables => _workspace.Current.Variables;

    /// <summary>
    /// The requests that hold no lease on the workspace: its own routes, so a change does not count itself, and what the page asks
    /// while it waits for an answer (the agent's turn, a run, the coordinator's sign-in), none of which reads the folder. Every
    /// other request holds it.
    /// </summary>
    private static bool LeavesWorkspace(string method, string path) =>
        path == "/api/workspace" || path.StartsWith("/api/workspace/", StringComparison.Ordinal)
        || (method == "GET" && path is "/api/agent" or "/api/run" or "/api/coordinator");

    private async Task WorkspaceRouteAsync(HttpContext context, string method, string path)
    {
        switch (method, path)
        {
            case ("GET", "/api/workspace"):
                await JsonAsync(context, WorkspaceView());
                break;
            case ("POST", "/api/workspace/test"):
                var tested = FolderAt((await BodyAsync<WorkspaceRequest>(context)).Path);
                await JsonAsync(context, new { root = tested.Root, name = NameOf(tested.Root), jobs = tested.Jobs().Count });
                break;
            case ("DELETE", "/api/workspace/recents"):
                await JsonAsync(context, ForgetRecent(await BodyAsync<WorkspaceRequest>(context)));
                break;
            case ("PUT", "/api/workspace"):
                await JsonAsync(context, await OpenWorkspaceAsync(await BodyAsync<WorkspaceRequest>(context)));
                break;
            default:
                throw new StudioRefusal(404, $"No {method} {path}.");
        }
    }

    /// <summary>The folder as the page reads it: where it is, how it was chosen, the one the studio started on and the recent ones.</summary>
    private object WorkspaceView(string? warning = null)
    {
        var root = Folder.Root;
        var recents = _tools.Settings?.ReadFolders() ?? [];
        return new
        {
            root,
            name = NameOf(root),
            source = _workspace.Source,
            start = _start,
            recents,
            canRemember = _tools.Settings is not null,
            settingsWarning = _tools.Settings?.Warning,
            warning,
        };
    }

    private static string NameOf(string root) => Path.GetFileName(root) is { Length: > 0 } name ? name : root;

    /// <summary>The folder at a path the person typed, opened as the studio opens one, or refused with the reason: nothing changes.</summary>
    private static StudioFolder FolderAt(string? typed)
    {
        var path = typed?.Trim() ?? "";
        if (path.Length == 0) throw new StudioRefusal(400, "Type the folder's full path.");
        if (path == "~" || path.StartsWith("~/", StringComparison.Ordinal) || path.StartsWith("~" + Path.DirectorySeparatorChar, StringComparison.Ordinal))
            path = Path.Join(Environment.GetFolderPath(Environment.SpecialFolder.UserProfile), path[1..]);
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
    private async Task<object> OpenWorkspaceAsync(WorkspaceRequest request)
    {
        var folder = FolderAt(request.Path);
        if (request.Unsaved) throw new StudioRefusal(409, "The open job has changes that are not saved: save them, or discard them, before opening another folder.");
        await ChangeWorkspaceAsync(folder);
        string? warning = null;
        if (request.Remember && _tools.Settings is { } settings)
        {
            try { settings.RememberFolder(folder.Root); }
            catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
            {
                warning = "The folder is open, but the settings file cannot be written: it is not among the recent folders.";
            }
        }
        return WorkspaceView(warning);
    }

    /// <summary>What the studio does with a folder it opens, at start and after a change: the samples and drafts an earlier studio left there go.</summary>
    private void Opened()
    {
        Local.PurgeSamples();
        PurgeDrafts();
    }

    /// <summary>
    /// Puts <paramref name="folder"/> in place of the folder the studio works on, or refuses with 409 naming what still uses the
    /// one in place. The agent is closed in the same step as the folder moves, and a new conversation starts on its model.
    /// </summary>
    private async Task ChangeWorkspaceAsync(StudioFolder folder)
    {
        if (RunInProgress) throw new StudioRefusal(409, "A run is in progress: stop it, or wait for it, before opening another folder.");
        var next = new StudioWorkspace(folder, _tools.Secrets);
        using (TakeSlotFor("A change of folder"))
        {
            StudioAgent? closed = null;
            try
            {
                _workspace.Replace(next, "page", () =>
                {
                    var agent = Agent;
                    if (agent?.TryClose("opening another folder") is { } reason) return reason;
                    closed = agent;
                    return null;
                });
            }
            finally
            {
                // The conversation was about the folder left: a new one starts on the same model, as a change of model does.
                if (closed is { } old) await _slot.RenewAsync(old, () => NewAgent(old.Renewed()));
            }
            lock (_runLock) _run = null;
            Opened();
        }
    }

    /// <summary>Removes a folder from the recent ones, by its path as the page shows it, whether or not it is still on the disk. The folder in place is untouched.</summary>
    private object ForgetRecent(WorkspaceRequest request)
    {
        var settings = _tools.Settings ?? throw new StudioRefusal(404, "This studio keeps no recent folders.");
        var path = request.Path?.Trim() ?? "";
        if (!Path.IsPathFullyQualified(path)) throw new StudioRefusal(400, $"'{path}' is not a full path.");
        try { settings.RemoveFolder(path); }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            return WorkspaceView("The settings file cannot be written: the folder stays among the recent ones.");
        }
        return WorkspaceView();
    }
}
