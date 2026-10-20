using System.Collections.Concurrent;
using DtPipe.Coordinator.Contracts;
using DtPipe.Jobs.Coordinator;

namespace DtPipe.Coordinator.Server;

/// <summary>The status of a pipeline, and of the whole report, as the wire writes it: the worst of what was found.</summary>
public static class HealthStatus
{
    public const string Ok = "ok";
    public const string Warning = "warning";
    public const string Critical = "critical";

    internal static string Of(IEnumerable<HealthCause> causes) =>
        causes.Any(c => c.Severity == HealthSeverity.Critical) ? Critical : causes.Any(c => c.Severity == HealthSeverity.Warning) ? Warning : Ok;

    internal static string Worst(IEnumerable<string> statuses) =>
        statuses.Contains(Critical) ? Critical : statuses.Contains(Warning) ? Warning : Ok;
}

/// <summary>How much a cause weighs: <see cref="Info"/> is told and changes no status.</summary>
public static class HealthSeverity
{
    public const string Info = "info";
    public const string Warning = "warning";
    public const string Critical = "critical";
}

/// <summary>What a pipeline's health read found besides binding problems and <see cref="LaunchCauses"/>, which keep their own codes.</summary>
public static class HealthCauses
{
    public const string NotArmed = "not-armed";
    public const string Deploying = "deploying";
    public const string DeploymentFailed = "deployment-failed";
    public const string JobUnreadable = "job-unreadable";
    public const string CheckFailed = "check-failed";
    public const string VerdictBreaking = "verdict-breaking";
    public const string VerdictUnread = "verdict-unread";
    public const string VerdictPending = "verdict-pending";
    public const string BrickChanged = "brick-changed";
    public const string ReplicaDegraded = "replica-degraded";
    public const string ReplicasDisagree = "replicas-disagree";
    public const string LibraryNewer = "library-newer";
    public const string LibraryRemoved = "library-removed";
    public const string LastRun = "last-run";
}

/// <summary>One finding: <paramref name="Code"/> is a <see cref="BindingCauses"/>, <see cref="LaunchCauses"/> or <see cref="HealthCauses"/> value, <paramref name="Severity"/> a <see cref="HealthSeverity"/>.</summary>
public sealed record HealthCause(string Code, string Message, string Severity, string? Alias = null, string? Node = null, string? Fragment = null);

/// <summary><paramref name="Replicas"/> of the fragment's node are connected, <paramref name="Eligible"/> of them could run it now.</summary>
public sealed record FragmentHealth(string Name, string Node, string Version, int Replicas, int Eligible);

/// <summary>
/// A pipeline as the supervisor reads it. <see cref="Commit"/> is the commit it is armed at and <see cref="LibraryCommit"/> the one the library
/// holds (null when the library no longer has it); <see cref="Causes"/> says why the status is not <c>ok</c>, then what is only worth knowing.
/// </summary>
public sealed record PipelineHealthView(
    string Id, string Status, string? Commit, string? LibraryCommit, DateTimeOffset? ArmedAt, string? ArmedBy,
    IReadOnlyList<HealthCause> Causes, IReadOnlyList<FragmentHealth> Fragments);

public sealed record HealthReport(string Status, DateTimeOffset At, IReadOnlyList<PipelineHealthView> Pipelines);

/// <summary>What the library holds of a pipeline now, by content: the narrow question a health read asks of it.</summary>
public sealed record LibraryHead(string? Commit, string JobSha, string? BindingsSha);

/// <summary>Where a health read learns what the library holds now; <see cref="PipelineLibrary"/> in the server, a script in a test.</summary>
public interface ILibraryHeads
{
    /// <summary>The pipeline as the library holds it, or null when it holds none.</summary>
    Task<LibraryHead?> HeadAsync(string pipelineId, CancellationToken ct);
}

/// <summary>
/// Whether each armed pipeline can be launched, read and never acted on: the coordinator stays passive, and a supervisor asks. A pipeline
/// can be launched when the job at its armed commit has bindings that hold, no mode found its brick changed beyond the binding, and a
/// plan is made with a runner and a replica that could run each fragment. That is not that the run will succeed: the replicas' check and
/// the sinks' check stay at arming and launching.
/// <para>
/// A computation asks no node: a verdict of a mode that needs one is read from <see cref="BindingVerifier"/> if it is known, and when it is
/// not, the ask starts once in the background and the pipeline is a <c>warning</c> until it answers. A result is kept while nothing the
/// server publishes about nodes, rights, the library or the deployments moves (and no background ask has ended), and for at most
/// <see cref="CoordinatorServerOptions.HealthMaxAge"/>, since the state of TransportR's registrations moves without an event. A result
/// that holds a replica connected and not eligible for a fragment of its plan is kept for <see cref="Unsettled"/> at most, the state
/// being one a registration ends and no event says so. One computation runs at a time; a request that waited for it reads its result when it is still the state's.
/// </para>
/// </summary>
public sealed class PipelineHealth
{
    /// <summary>The events that say the state a health read depends on moved.</summary>
    public static readonly IReadOnlyList<string> Subjects = ["nodes", "rights", "library", "deploy", "deployments"];

    private static readonly TimeSpan KnownBound = TimeSpan.FromSeconds(5);

    /// <summary>How long a read is kept while a connected replica cannot run a fragment: its client may register at any moment, with no event.</summary>
    public static readonly TimeSpan Unsettled = TimeSpan.FromSeconds(5);

    private readonly DeploymentManager _deployments;
    private readonly IJobSource _jobs;
    private readonly ILibraryHeads _library;
    private readonly AutoPlacer _placer;
    private readonly BindingVerifier _verifier;
    private readonly BrickCatalog _bricks;
    private readonly NodeInventory _inventory;
    private readonly RunJournal _journal;
    private readonly EventBus _bus;
    private readonly TimeSpan _maxAge;
    private readonly TimeProvider _time;
    private readonly ILogger<PipelineHealth> _logger;

    private readonly SemaphoreSlim _computing = new(1, 1);
    private readonly object _lock = new();
    private (long Generation, DateTimeOffset At, HealthReport Report)? _kept;
    private long _asked;
    private int _computed;

    // A verdict the node could not give is no verdict on the brick, and the verifier keeps none: it is remembered here for the age
    // a read is, so a node that stays silent is asked once per age and not once per read.
    private readonly ConcurrentDictionary<AskKey, (BindingVerdict Verdict, DateTimeOffset At)> _unread = new();
    private readonly ConcurrentDictionary<AskKey, byte> _asking = new();

    private readonly record struct AskKey(string Pipeline, string Commit, string Alias, string Brick, string Binding);

    public PipelineHealth(
        DeploymentManager deployments, IJobSource jobs, ILibraryHeads library, AutoPlacer placer, BindingVerifier verifier, BrickCatalog bricks,
        NodeInventory inventory, RunJournal journal, EventBus bus, CoordinatorServerOptions options, ILogger<PipelineHealth> logger, TimeProvider? time = null)
    {
        (_deployments, _jobs, _library, _placer, _verifier, _bricks, _inventory, _journal, _bus, _maxAge, _logger, _time) =
            (deployments, jobs, library, placer, verifier, bricks, inventory, journal, bus, options.HealthMaxAge, logger, time ?? TimeProvider.System);
    }

    /// <summary>How many times the report was computed: a request answered from what is kept adds none.</summary>
    public int Computed => Volatile.Read(ref _computed);

    private long Generation => _bus.CountOf(Subjects) + Interlocked.Read(ref _asked);

    private bool TryKept(long generation, out HealthReport report)
    {
        lock (_lock)
        {
            if (_kept is { } kept && kept.Generation == generation && _time.GetUtcNow() - kept.At < AgeBound(kept.Report))
            {
                report = kept.Report;
                return true;
            }
        }
        report = null!;
        return false;
    }

    private TimeSpan AgeBound(HealthReport report) =>
        report.Pipelines.Any(p => p.Fragments.Any(f => f.Eligible < f.Replicas)) ? (_maxAge < Unsettled ? _maxAge : Unsettled) : _maxAge;

    /// <summary>The health of every armed pipeline: what is kept when the state has not moved, else a computation, which only one request makes at a time.</summary>
    public async Task<HealthReport> ReadAsync(CancellationToken ct)
    {
        if (TryKept(Generation, out var report)) return report;
        await _computing.WaitAsync(ct);
        try
        {
            // The state is read again once it is our turn: the request before us may have computed for it.
            var generation = Generation;
            if (TryKept(generation, out report)) return report;
            report = await ComputeAsync(ct);
            // Stamped with the generation read before computing: a change during the computation is seen by the next request.
            lock (_lock) _kept = (generation, _time.GetUtcNow(), report);
            return report;
        }
        finally
        {
            _computing.Release();
        }
    }

    /// <summary>The health of one pipeline, which is <c>critical</c> when it is not armed, so that a disarmed pipeline raises an alert.</summary>
    public async Task<HealthReport> ReadAsync(string pipelineId, CancellationToken ct)
    {
        var report = await ReadAsync(ct);
        var view = report.Pipelines.FirstOrDefault(p => p.Id == pipelineId)
            ?? new PipelineHealthView(pipelineId, HealthStatus.Critical, null, null, null, null,
                [new HealthCause(HealthCauses.NotArmed, $"'{pipelineId}' is not armed: no launch of it can be made.", HealthSeverity.Critical)], []);
        return new HealthReport(view.Status, report.At, [view]);
    }

    private async Task<HealthReport> ComputeAsync(CancellationToken ct)
    {
        Interlocked.Increment(ref _computed);
        var views = new List<PipelineHealthView>();
        foreach (var (armed, state, message) in _deployments.Armed().OrderBy(a => a.Armed.PipelineId, StringComparer.Ordinal))
        {
            try
            {
                views.Add(await ViewOfAsync(armed, state, message, ct));
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                // A pipeline whose health cannot be read is critical: one that fails must not blind the supervisor to the others.
                _logger.LogWarning(ex, "The health of '{Pipeline}' could not be read", armed.PipelineId);
                views.Add(new PipelineHealthView(armed.PipelineId, HealthStatus.Critical, armed.Commit, null, armed.ArmedAt, armed.By,
                    [new HealthCause(HealthCauses.CheckFailed, $"The health of '{armed.PipelineId}' could not be read: {ex.Message}", HealthSeverity.Critical)], []));
            }
        }
        return new HealthReport(HealthStatus.Worst(views.Select(v => v.Status)), _time.GetUtcNow(), views);
    }

    private async Task<PipelineHealthView> ViewOfAsync(ArmedDeployment armed, DeploymentState state, string? stateMessage, CancellationToken ct)
    {
        var causes = new List<HealthCause>();
        var fragments = new List<FragmentHealth>();
        var id = armed.PipelineId;

        // The library is told whatever else is found: a pipeline removed from it stays armed and launchable, and that is worth knowing.
        string? libraryCommit = null;
        var head = await _library.HeadAsync(id, ct);
        if (head is null)
            causes.Add(new(HealthCauses.LibraryRemoved, $"'{id}' is no longer in the library; the armed version still runs.", HealthSeverity.Info));
        else
        {
            libraryCommit = head.Commit;
            if (head.JobSha != armed.JobSha || head.BindingsSha != armed.BindingsSha)
                causes.Add(new(HealthCauses.LibraryNewer, "The library holds a later version than the one armed.", HealthSeverity.Info));
        }
        if (_journal.List(id, limit: 1).FirstOrDefault() is { } last)
            causes.Add(new(HealthCauses.LastRun, $"{last.RunId} {last.State.Word()}{(last.FinishedAt is { } at ? $" at {at:u}" : "")}.", HealthSeverity.Info));

        if (state == DeploymentState.Deploying)
            causes.Add(new(HealthCauses.Deploying, $"'{id}' is being armed.", HealthSeverity.Warning));
        else if (state == DeploymentState.Failed)
            causes.Add(new(HealthCauses.DeploymentFailed, $"The arming of '{id}' failed: {stateMessage}".TrimEnd(), HealthSeverity.Critical));

        var job = await _jobs.JobAtAsync(id, armed.Commit, ct);
        if (job is null)
        {
            causes.Add(new(HealthCauses.JobUnreadable, $"The job of '{id}' cannot be read at {Short(armed.Commit)}.", HealthSeverity.Critical));
            return ViewOf(armed, libraryCommit, causes, fragments);
        }

        // Binding problems come first: they are the cause of a launch that is refused, and the plan below repeats them.
        var problems = _placer.Check(job, armed.Bindings);
        foreach (var p in problems)
            causes.Add(new(p.Cause, p.Message, HealthSeverity.Critical, p.Alias, armed.Bindings.GetValueOrDefault(p.Alias)?.Node));
        var unwell = problems.Select(p => p.Alias).ToHashSet(StringComparer.Ordinal);
        var offline = problems.Where(p => p.Cause == BindingCauses.Offline).Select(p => armed.Bindings[p.Alias].Node).ToHashSet(StringComparer.Ordinal);

        foreach (var (alias, binding) in armed.Bindings.Where(b => !Bindings.IsStrict(b.Value) && !unwell.Contains(b.Key)).OrderBy(b => b.Key, StringComparer.Ordinal))
            causes.AddRange(await VerdictCausesAsync(armed, alias, binding, ct));

        var launch = await _deployments.PlanLaunchAsync(id, job, armed.Bindings, ct);
        foreach (var cause in launch.Causes ?? [])
        {
            // A node with no connected replica has no replica that could run its fragment: its binding says so already.
            if (cause.Code is LaunchCauses.NoEligibleReplica or LaunchCauses.NoAdmissibleRunner && cause.Node is { } node && offline.Contains(node)) continue;
            causes.Add(new(cause.Code, cause.Message, HealthSeverity.Critical, Node: cause.Node, Fragment: cause.Fragment));
        }

        var eligibility = await _deployments.EligibilityOfAsync(launch.Plan);
        var nodes = _inventory.Snapshot().ToDictionary(n => n.Name, StringComparer.Ordinal);
        foreach (var fragment in launch.Plan.Fragments)
        {
            var eligible = eligibility?.GetValueOrDefault(fragment.Name)?.Eligible ?? [];
            var connected = _inventory.ReplicasOf(fragment.Node);
            fragments.Add(new FragmentHealth(fragment.Name, fragment.Node, fragment.Version, connected.Count, eligible.Count));
            // Some replicas of the node cannot run the fragment and another can: degraded, and measurable without an expected number of them.
            if (eligible.Count > 0 && connected.Any(r => !eligible.Contains(r.Key)))
                causes.Add(new(HealthCauses.ReplicaDegraded,
                    $"{connected.Count(r => !eligible.Contains(r.Key))} of the {connected.Count} replicas of {fragment.Node} cannot run {fragment.Name}; {eligible.Count} can.",
                    HealthSeverity.Warning, Node: fragment.Node, Fragment: fragment.Name));
        }
        foreach (var node in launch.Plan.Fragments.Select(f => f.Node).Concat(armed.Bindings.Values.Select(b => b.Node)).Distinct(StringComparer.Ordinal).Order(StringComparer.Ordinal))
            if (nodes.TryGetValue(node, out var view) && view.ReplicasDisagree)
                causes.Add(new(HealthCauses.ReplicasDisagree, $"The replicas of {node} offer different bricks.", HealthSeverity.Warning, Node: node));

        return ViewOf(armed, libraryCommit, causes, fragments);
    }

    private static PipelineHealthView ViewOf(ArmedDeployment armed, string? libraryCommit, List<HealthCause> causes, List<FragmentHealth> fragments)
    {
        // What is wrong first, then what is only worth knowing.
        var ordered = causes.OrderBy(c => c.Severity switch { HealthSeverity.Critical => 0, HealthSeverity.Warning => 1, _ => 2 }).ToList();
        return new PipelineHealthView(armed.PipelineId, HealthStatus.Of(ordered), armed.Commit, libraryCommit, armed.ArmedAt, armed.By, ordered, fragments);
    }

    private static string Short(string commit) => commit[..Math.Min(8, commit.Length)];

    /// <summary>What the mode of a binding that is not strict found of its brick: a verdict that is known, or the ask that starts now.</summary>
    private async Task<IEnumerable<HealthCause>> VerdictCausesAsync(ArmedDeployment armed, string alias, Binding binding, CancellationToken ct)
    {
        var id = armed.PipelineId;
        var where = $"'{alias}' is bound to brick '{binding.Brick}' on {binding.Node} in mode '{Bindings.ModeOf(binding)}'";
        var key = new AskKey(id, armed.Commit, alias, _bricks.Find($"{binding.Node}/{binding.Brick}")?.Version ?? "", Bindings.ShaOf(binding));
        BindingVerdict? verdict = null;

        switch (_verifier.StateOf(id, armed.Commit, alias, binding))
        {
            case VerdictState.Known:
                verdict = await KnownAsync(id, armed.Commit, alias, binding, key, ct);
                break;
            case VerdictState.Absent when _unread.TryGetValue(key, out var unread) && _time.GetUtcNow() - unread.At < _maxAge:
                verdict = unread.Verdict;
                break;
        }
        if (verdict is null)
        {
            if (_verifier.StateOf(id, armed.Commit, alias, binding) == VerdictState.Absent) Ask(id, armed.Commit, alias, binding, key);
            return [new HealthCause(HealthCauses.VerdictPending, $"{where}, and its brick changed since the check: the node is being asked.", HealthSeverity.Warning, alias, binding.Node)];
        }

        return verdict.Verdict switch
        {
            BindingVerdicts.Breaking => [new HealthCause(HealthCauses.VerdictBreaking, $"{where}, and its brick changed beyond it: {verdict.Report}".TrimEnd(), HealthSeverity.Critical, alias, binding.Node)],
            BindingVerdicts.Unread => [new HealthCause(HealthCauses.VerdictUnread, $"{where}, and its brick could not be checked: {verdict.Report}".TrimEnd(), HealthSeverity.Critical, alias, binding.Node)],
            BindingVerdicts.Unchanged => [],
            BindingVerdicts.Tolerated => [new HealthCause(HealthCauses.BrickChanged, $"{where}, and its brick changed since the check; nothing verifies a tolerant binding.", HealthSeverity.Info, alias, binding.Node)],
            _ => [new HealthCause(HealthCauses.BrickChanged, $"{where}, and its brick changed since the check, within the mode: {verdict.Verdict}.", HealthSeverity.Info, alias, binding.Node)],
        };
    }

    /// <summary>A verdict the verifier holds, or null when it has none after all (it was dropped between the question and the read).</summary>
    private async Task<BindingVerdict?> KnownAsync(string id, string commit, string alias, Binding binding, AskKey key, CancellationToken ct)
    {
        using var bound = CancellationTokenSource.CreateLinkedTokenSource(ct);
        bound.CancelAfter(KnownBound);
        try
        {
            var verdict = await _verifier.VerdictAsync(id, commit, alias, binding, bound.Token);
            if (verdict.Verdict == BindingVerdicts.Unread) _unread[key] = (verdict, _time.GetUtcNow());
            return verdict;
        }
        catch (OperationCanceledException) when (!ct.IsCancellationRequested)
        {
            return null;
        }
    }

    /// <summary>Asks the node about a binding once, in the background: its answer is kept by the verifier or, when it is none, here, and ends the cached read.</summary>
    private void Ask(string id, string commit, string alias, Binding binding, AskKey key)
    {
        if (!_asking.TryAdd(key, 0)) return;
        _ = Task.Run(async () =>
        {
            try
            {
                var verdict = await _verifier.VerdictAsync(id, commit, alias, binding, CancellationToken.None);
                if (verdict.Verdict == BindingVerdicts.Unread) _unread[key] = (verdict, _time.GetUtcNow());
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "The verdict of '{Alias}' in '{Pipeline}' could not be read", alias, id);
            }
            finally
            {
                _asking.TryRemove(key, out _);
                // The read that said "in flight" is stale now, and no event says so.
                Interlocked.Increment(ref _asked);
            }
        });
    }
}
