using DtPipe.Cli.Expressions;
using DtPipe.Cli.Node;
using DtPipe.Cli.Security;
using DtPipe.Jobs;
using DtPipe.Jobs.Coordinator;
using YamlDotNet.RepresentationModel;

namespace DtPipe.Cli.Studio;

/// <summary>
/// Shared workspace-bound design, brick recognition, secret checking, and sampling helpers used across job, debug,
/// run, coordinator, and agent routes. Reads the workspace and coordinator slots live on each call.
/// </summary>
internal sealed class StudioDesignContext(
    WorkspaceSlot workspace,
    CoordinatorSlot coordinator,
    EngineSlot engineSlot,
    StudioTools tools)
{
    private StudioWorkspace CurrentWorkspace => workspace.Current;

    private CoordinatorLink? Coordinator => coordinator.Current;

    /// <summary>A job's path, confined to the folder, or null for none.</summary>
    public string? JobPath(string? relative)
    {
        if (string.IsNullOrEmpty(relative)) return null;
        CurrentWorkspace.Folder.ResolveForWrite(relative);
        return relative;
    }

    /// <summary>The designer's model over the catalog read last: what a card names is found, nothing is recognised.</summary>
    public DesignService Design() =>
        tools.DesignWith is { } with && Coordinator is { } link ? with(new StudioBricks(link.Catalog)) : tools.Design;

    /// <summary>
    /// The model for taking <paramref name="yaml"/> apart: the coordinator says what each branch is when a
    /// person is signed in; otherwise no branch is a brick and each opens as a local card.
    /// </summary>
    public async Task<DesignService> DesignForAsync(string yaml, CancellationToken ct)
    {
        // Held where the link is awaited, whichever route asks: opening a job and the agent's save ask it too.
        using var hold = coordinator.Hold();
        if (tools.DesignWith is not { } with || hold.Value is not { } link) return tools.Design;
        string job;
        try { job = ForRecognition(yaml); }
        catch (Exception) { return Design(); }
        // Nothing to ask for a job none of whose branches references a brick: it never leaves this machine.
        var answers = job.Length == 0 ? null : await link.RecognizeAsync(job, ct);
        return with(new StudioBricks(link.Catalog, job, answers));
    }

    /// <summary>
    /// What the coordinator is asked to recognise: only the top-level branches that are a reference to a brick
    /// (<see cref="BrickRef.Of"/>), so a branch of this machine, a connection string included, is never sent. A
    /// reference it misses opens as a local card, which loses nothing. Empty when no branch qualifies.
    /// </summary>
    public static string ForRecognition(string yaml)
    {
        var kept = new YamlMappingNode();
        foreach (var (alias, branch) in JobYaml.Branches(JobYaml.Parse(yaml)))
            if (BrickRef.Of(alias, branch) is not null) kept.Add(alias, JobYaml.CloneBranch(branch));
        return kept.Children.Count == 0 ? "" : JobYaml.Serialize(kept);
    }

    /// <summary>
    /// The job a sample or a run gives the engine here, each brick card bound, or the refusal that says why it cannot run here:
    /// a source brick while nobody is signed in, a branch that uses the brick scheme as no reference does, a reference no card
    /// binds. The engine has no <c>brick:</c>, and what is left of it fails there before the branch can give a reason. The
    /// composition the page shows asks it too (<see cref="WithRunHere"/>), so Problems says it before a run does.
    /// </summary>
    public BrickSampling RunnableHere(string yaml, List<SampledBrick>? bricks, string gesture)
    {
        var sampling = BrickSampling.Of(yaml, bricks, Coordinator?.Catalog);
        if (sampling.Sources.Count > 0 && Coordinator?.Status().State != CoordinatorLink.SignedIn)
            throw new StudioRefusal(409, $"This job reads a node's brick: sign in to the coordinator to {gesture} it.");
        RefuseMisusedReferences(sampling.Yaml);
        RefuseReferences(sampling.Yaml, SignedInCatalog());
        return sampling;
    }

    /// <summary>
    /// The composition of the page's cards, with the refusal a sample or a run here would give among its warnings: the job
    /// composes, and runs only on a node or once signed in. A composition with errors says those first.
    /// </summary>
    public DesignResult WithRunHere(DesignResult composed)
    {
        if (!composed.Valid) return composed;
        var bricks = composed.Model.Steps.Where(s => s.Brick is not null && s.Alias is not null)
            .Select(s => new SampledBrick(composed.Steps.FirstOrDefault(i => i.Alias == s.Alias)?.Branch ?? s.Alias!, s.Brick!))
            .ToList();
        try
        {
            RunnableHere(composed.Yaml, bricks, "run");
            return composed;
        }
        catch (StudioRefusal refusal) { return composed with { Warnings = [.. composed.Warnings, refusal.Message] }; }
        catch (Exception ex) when (ex is FormatException or YamlDotNet.Core.YamlException) { return composed; }
    }

    /// <summary>
    /// Refuses a job in which a branch uses the brick scheme as no reference does (<see cref="BrickRef.Misuse"/>): one that
    /// reads a brick and says more, a malformed reference. No card binds such a branch, here or on a node.
    /// </summary>
    public static void RefuseMisusedReferences(string yaml)
    {
        List<string> misuses;
        try { misuses = JobYaml.Branches(JobYaml.Parse(yaml)).Select(b => BrickRef.Misuse(b.Alias, b.Branch)).OfType<string>().ToList(); }
        catch (Exception ex) when (ex is FormatException or YamlDotNet.Core.YamlException) { return; }
        if (misuses.Count > 0) throw new StudioRefusal(409, string.Join(" ", misuses));
    }

    /// <summary>
    /// Refuses a job in which a branch still references a brick: the engine has no <c>brick:</c>, so what a sample or a run
    /// does not bind to a throwaway file or a node's rows must not reach it. A job that is not readable is the engine's to report.
    /// With <paramref name="signedIn"/>, the catalog read last while a person is signed in: a reference it does not hold is
    /// named as no node's brick, not as a sign-in to make. Without it, the refusal asks for a sign-in.
    /// </summary>
    public static void RefuseReferences(string yaml, BrickCatalogView? signedIn = null)
    {
        IReadOnlyDictionary<string, BrickReference> left;
        try { left = BrickRef.In(JobYaml.Parse(yaml)); }
        catch (Exception ex) when (ex is FormatException or YamlDotNet.Core.YamlException) { return; }
        if (left.Count == 0) return;
        var unknown = signedIn is null ? new List<BrickReference>() : left.Values.Where(r => signedIn.Bricks.All(b => b.Key != r.Key)).ToList();
        if (unknown.Count > 0)
            throw new StudioRefusal(409, "No node offers the brick a branch references: "
                + string.Join(", ", unknown.Select(r => $"'{r.Alias}' ({r.Key})"))
                + ". Choose a brick of the palette, or write the branch's own endpoint.");
        throw new StudioRefusal(409, "A branch references a brick, which only its node reads: "
            + string.Join(", ", left.Values.Select(r => $"'{r.Alias}' ({r.Key})"))
            + ". Sign in to the coordinator to sample it.");
    }

    /// <summary>The catalog read from the coordinator while a person is signed in to it, or null.</summary>
    public BrickCatalogView? SignedInCatalog() =>
        Coordinator is { } link && link.Status().State == CoordinatorLink.SignedIn ? link.Catalog : null;

    /// <summary>The job with each brick's branch replaced as a sample replaces it; as it is when that cannot be done.</summary>
    public string WithoutBricks(string yaml, List<SampledBrick>? bricks)
    {
        try { return BrickSampling.Of(yaml, bricks, Coordinator?.Catalog).Yaml; }
        catch (Exception ex) when (ex is StudioRefusal or FormatException or YamlDotNet.Core.YamlException) { return yaml; }
    }

    /// <summary>
    /// What a library keeps for good is no place for a credential: the places of the job that spell one out as a value, never the
    /// value, and what to write instead. Judged by <see cref="JobSecrets"/>, best-effort in both directions, as a brick's own refusal
    /// is. Null when the job holds none.
    /// </summary>
    public static string? SecretsRefusal(string yaml)
    {
        var at = JobSecrets.Literals(yaml);
        if (at.Count == 0) return null;
        const int Named = 8;
        var places = string.Join(", ", at.Take(Named)) + (at.Count > Named ? $" and {at.Count - Named} more" : "");
        return $"{places}: a library is shared and its history is kept, so a credential published there cannot be taken back. {NodeBricks.UseAReference}";
    }

    /// <summary>A location a binding can carry: its pairs are separated by commas.</summary>
    public static string Bindable(string path) => path.Contains(',')
        ? throw new StudioRefusal(400, "The folder's path holds a comma, which a binding cannot carry: open the studio on another folder to sample a brick.")
        : path;

    /// <summary>
    /// The sample of <paramref name="yaml"/> through the studio's one sample path, with its bricks bound as a
    /// sample binds them, its variables armed for this call alone and the engine slot held. <paramref name="rewrite"/>
    /// gives the job to run from the one the bricks left: the debug run's cut.
    /// </summary>
    public async Task<string> SampleCoreAsync(string yaml, int rows, string? path, List<SampledBrick>? bricks,
        Func<string, int, StudioBindings, CancellationToken, Task<string>> sampler,
        Func<string, (string Yaml, IReadOnlySet<string> Dropped, IReadOnlySet<string> Unread)>? rewrite, CancellationToken ct)
    {
        // Each brick's branch is read or written on its node: here, its rows come from the coordinator.
        var sampling = RunnableHere(yaml, bricks, "sample");

        // Every variable resolves before anything runs, and is named when it would not.
        var ws = CurrentWorkspace;
        var launch = ws.Variables.LaunchEnvironment(JobPath(path), sampling.Yaml);

        using var lease = engineSlot.Take("A sample");
        var files = new List<string>();
        try
        {
            var inputs = new List<(string Alias, string Path)>();
            foreach (var (alias, brick) in sampling.Sources)
            {
                byte[] arrow;
                try { arrow = await (Coordinator ?? throw new StudioRefusal(409, StudioHttp.NoCoordinator)).SampleAsync(brick.Node, brick.Id, Math.Min(rows, CoordinatorApi.MaxSampleRows), ct); }
                catch (CoordinatorLinkException ex) { throw new StudioRefusal(502, $"'{alias}': {ex.Message}"); }
                files.Add(ws.Local.SampleFile(arrow));
                inputs.Add((alias, Bindable(files[^1])));
            }
            var outputs = new List<(string Alias, string Path)>();
            foreach (var alias in sampling.Sinks)
            {
                files.Add(ws.Local.SampleFile(null));
                outputs.Add((alias, Bindable(files[^1])));
            }
            // The job to run, and the bindings of the branches it still has.
            var (yamlToRun, dropped, unread) = rewrite?.Invoke(sampling.Yaml) ?? (sampling.Yaml, new HashSet<string>(), new HashSet<string>());
            // A branch read back from a checkpoint reads nothing: no input to bind.
            string? Join(List<(string Alias, string Path)> pairs, IReadOnlySet<string> skipped) =>
                pairs.Where(p => !dropped.Contains(p.Alias) && !skipped.Contains(p.Alias)).Select(p => $"{p.Alias}={p.Path}").ToList() is { Count: > 0 } kept ? string.Join(',', kept) : null;
            var bindings = new StudioBindings(Join(inputs, unread), Join(outputs, new HashSet<string>()));

            // The run reads ${{NAME}} through its own resolver, which asks the launch values first: each chosen
            // value is this action's alone, and the process environment is never written.
            using var armed = LaunchValues.Arm(launch);
            // Nobody watches the terminal for a prompt: a run that would ask one fails instead.
            using var _ = NonInteractiveGuard.Suppress();
            return await sampler(yamlToRun, rows, bindings, ct);
        }
        finally
        {
            foreach (var file in files) File.Delete(file);
        }
    }
}
