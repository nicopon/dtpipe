namespace DtPipe.Cli.Studio;

/// <summary>
/// The folder the studio works on, which the page may change. Every request that uses it reads it in the slot, never a copy;
/// a change is refused while a request uses the folder, an engine action or a run goes on, or the agent's turn runs or a card
/// waits. It moves the process's current directory, starts a new conversation with the agent, and forgets the run the old
/// folder ended.
/// </summary>
public sealed partial class StudioServer
{
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
}
