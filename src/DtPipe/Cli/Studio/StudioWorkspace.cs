using DtPipe.Cli.Security;

namespace DtPipe.Cli.Studio;

/// <summary>
/// The folder the studio works on, with what it keeps beside the jobs (<see cref="StudioLocalFiles"/>) and the variables it
/// launches them with: one object, which a change of folder replaces whole, so nothing of one folder is read with another.
/// </summary>
internal sealed class StudioWorkspace
{
    public StudioWorkspace(StudioFolder folder, ISecretsManager secrets)
    {
        Folder = folder;
        Local = new StudioLocalFiles(folder);
        Variables = new StudioVariables(Local, secrets);
    }

    public StudioFolder Folder { get; }

    public StudioLocalFiles Local { get; }

    public StudioVariables Variables { get; }
}

/// <summary>
/// The studio's workspace, which the page may replace. Every request that uses the folder holds it for its whole length
/// (<see cref="Hold"/>), and a replacement is refused while one is held. The process's current directory is the folder's
/// (a job's relative paths, the session store and the agent's tools read it): a replacement moves it in the same step, so
/// read <see cref="Current"/> where it is used, never a copy kept from an earlier request. <see cref="Source"/> is
/// <c>argument</c> (the folder the studio started on) or <c>page</c>.
/// </summary>
internal sealed class WorkspaceSlot(StudioWorkspace workspace, string source)
{
    // A page sends requests of its own as the person acts (the view it keeps, the composition): a change waits that long for them.
    private static readonly TimeSpan Drain = TimeSpan.FromSeconds(5);

    private readonly SlotHolds _holds = new(
        "The folder is being used by another request: wait for it to finish.", "Another folder is being opened: reload the page.");
    private StudioWorkspace _workspace = workspace;
    private string _source = source;

    public StudioWorkspace Current => Volatile.Read(ref _workspace);

    public string Source => Volatile.Read(ref _source);

    /// <summary>Holds the workspace in place until the lease is disposed.</summary>
    public SlotHolds.Lease<StudioWorkspace> Hold() => _holds.Hold(() => _workspace);

    /// <summary>
    /// Puts <paramref name="next"/> in place and moves the process's current directory to its folder, or refuses with 409 while a
    /// request still holds the workspace once <see cref="Drain"/> has passed, or when <paramref name="closeOthers"/> names what
    /// still uses it. <paramref name="closeOthers"/>
    /// runs under the slot's lock once no request is counted, so a request that holds the workspace refuses the replacement
    /// before anything is closed, and none starts until the folder has moved.
    /// </summary>
    public Task ReplaceAsync(StudioWorkspace next, string source, Func<string?> closeOthers, CancellationToken ct = default) =>
        _holds.ReplaceAsync(() =>
        {
            if (closeOthers() is { } reason) throw new StudioRefusal(409, reason);
            Directory.SetCurrentDirectory(next.Folder.Root);
            Volatile.Write(ref _workspace, next);
            Volatile.Write(ref _source, source);
        }, Drain, ct);
}
