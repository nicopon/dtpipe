using System.Text.Json;
using System.Text.Json.Serialization;
using DtPipe.Coordinator;
using DtPipe.Coordinator.Server.Access;
using DtPipe.Coordinator.Server.Cursors;
using DtPipe.Coordinator.Server.Library;
using DtPipe.Coordinator.Server.Stores;

namespace DtPipe.Coordinator.Server.Runs;

/// <summary>A fragment of the plan a run ran, as the plan named it.</summary>
public sealed record RunPlanFragment(string Name, string Node, string Version);

/// <summary>
/// A binding as a run met it: the brick it names and that brick's version then, the mode it is held to and what that mode found
/// (<see cref="BindingVerdicts.Exact"/> for <see cref="BindingModes.Strict"/>: the branch's text is the brick's; the others are
/// <see cref="BindingVerifier"/>'s).
/// </summary>
public sealed record RunPlanBinding(string Alias, string Node, string Brick, string BrickVersion, string Mode, string Verdict);

/// <summary>Verdicts of a binding's mode, as <see cref="RunPlanBinding.Verdict"/>.</summary>
public static class BindingVerdicts
{
    /// <summary>The branch's canonical form equals the brick's: nothing can have changed (<see cref="BindingModes.Strict"/>).</summary>
    public const string Exact = "exact";

    /// <summary>The brick is at the version its binding was checked at.</summary>
    public const string Unchanged = "unchanged";

    /// <summary>The brick changed and its binding is <see cref="BindingModes.Tolerant"/>: no node was asked.</summary>
    public const string Tolerated = "tolerated";

    /// <summary>A source changed and still produces the schema it was pinned with.</summary>
    public const string Same = "same";

    /// <summary>A source changed and what it produces still satisfies the consumers of the pinned schema.</summary>
    public const string Compatible = "compatible";

    /// <summary>A sink changed and its target still accepts the pinned contract.</summary>
    public const string Accepted = "accepted";

    /// <summary>The brick changed beyond what its mode accepts: the run is refused.</summary>
    public const string Breaking = "breaking";

    /// <summary>The node could not tell, so the run is refused; it says nothing about the brick and is not kept.</summary>
    public const string Unread = "unread";
}

/// <summary>
/// The plan a run ran, computed when it started: its id, the runner it chose (null when no branch needed one), its fragments
/// and its bindings. A run refused before it had a plan has none.
/// </summary>
public sealed record RunPlan(
    string PlanId, string? Runner, IReadOnlyList<RunPlanFragment> Fragments, IReadOnlyList<RunPlanBinding> Bindings);

/// <summary>
/// <see cref="Plan"/> is the plan the run ran, journaled with it. <see cref="LaunchedBy"/> names who started it
/// (<see cref="TrustedIdp.CallerOf"/>): a person's email, or the id of the identity that did, a service or a person verified by a
/// key; an id holds no <c>@</c> and is unique among the identities of every kind, so a launcher's id names its own runs only.
/// <see cref="IdempotencyKey"/> is the key a service sent with the request, when it sent one; it means something only together
/// with <see cref="LaunchedBy"/>. <see cref="SinkMayBePartial"/> is
/// true for a run a restart ended after it had started: its fragments had been launched and may have written part of their
/// sinks. It ranks no recovery strategy and nothing reruns the run; a run that was only queued, or that ended any other way, is not
/// labelled. <see cref="Cursors"/> says what became of each cursor a run that succeeded reported: kept, or not and why.
/// </summary>
public sealed record RunView(
    string RunId, string PlanId, [property: JsonConverter(typeof(WireEnumConverter<RunState>))] RunState State, DateTimeOffset StartedAt,
    DateTimeOffset? FinishedAt,
    RunResult? Result, string? Description, string? Refusal,
    string? PipelineId = null, DateTimeOffset? QueuedAt = null, RunPlan? Plan = null, string? LaunchedBy = null, string? IdempotencyKey = null,
    bool SinkMayBePartial = false, IReadOnlyList<CursorOutcome>? Cursors = null)
{
    [JsonIgnore] public bool IsOver => State is not (RunState.Queued or RunState.Running);

    /// <summary>
    /// Why the run did not succeed, in one line a person reads: the first line of the failing fragment's own error, the
    /// fragment that stopped answering, or the refusal without the runner it was tried on. It is presentation, derived from
    /// <see cref="Result"/> and <see cref="Refusal"/> each time it is read: it changes no state, code or verdict, and is null
    /// for a run that succeeded or that its requester cancelled.
    /// </summary>
    public string? Reason => State switch
    {
        RunState.Failed when Result is { Cause: { } cause } result => FailureLine(result, cause),
        RunState.Refused or RunState.Error => RefusalLine(Refusal),
        _ => null,
    };

    private static string? FailureLine(RunResult result, string cause)
    {
        if (result.Reports.TryGetValue(cause, out var report))
            return FirstLine(report.FirstFault);
        return result.CauseIsUnresponsive ? $"{cause} stopped answering." : cause;
    }

    private static string? RefusalLine(string? refusal)
    {
        var line = FirstLine(refusal);
        return line is null ? null : System.Text.RegularExpressions.Regex.Replace(line, @"^Runner \S+: ", "");
    }

    private static string? FirstLine(string? text) =>
        text?.Split('\n').Select(l => l.Trim()).FirstOrDefault(l => l.Length > 0);
}

/// <summary>
/// Every run, a JSON document each in the <see cref="IRunStore"/>, read back at start. Runs are what happened, not what was
/// designed: the library's git history never holds them. A run is written when it is queued, again when it starts and when it
/// ends, so a restart finds what it interrupted: those runs are ended then (<see cref="RestartedRefusal"/>), and the ones that had
/// started are labelled <see cref="RunView.SinkMayBePartial"/> and leave their fragments <see cref="Owed"/>: their children may still run, and the abandonment is kept in the store until
/// a replica gives them up, so a second restart before the replicas announce forgets nothing. History (<see cref="List"/>, <see cref="Find"/>) holds the runs
/// that are over and keeps the latest <see cref="KeptPerPipeline"/> of each pipeline, in memory as in the store: the journal decides, and
/// names the runs it drops for the store to erase in the write that ends the run that pushed them out, or at start; the runs of no pipeline
/// share one bucket of that size, and the runs of a pipeline no longer in the library are kept like the others. A run in flight is in the store
/// only. A run not yet over that cannot be written throws <see cref="StoreUnavailableException"/>, so none starts without its
/// record; one that is over is kept in memory whatever the store says, since refusing it would help nobody, and a store that missed
/// it leaves it in flight for the next start to end; the documents that write should have erased stay in the store until that start,
/// which drops them as it drops any run beyond the bucket.
/// </summary>
public sealed class RunJournal
{
    /// <summary>How many runs that are over the journal keeps of each pipeline.</summary>
    public const int KeptPerPipeline = 10;

    private static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web)
    {
        Converters = { new JsonStringEnumConverter() },
    };

    private readonly IRunStore _store;
    private readonly ILogger<RunJournal> _logger;
    private readonly object _lock = new();
    private readonly List<RunView> _runs = [];

    /// <summary>The refusal of a run a restart interrupted.</summary>
    public const string RestartedRefusal = "The coordinator restarted before this run ended.";

    public RunJournal(IRunStore store, ILogger<RunJournal> logger)
    {
        (_store, _logger) = (store, logger);
        var stored = store.Load();
        var loaded = new Dictionary<string, RunView>(StringComparer.Ordinal);
        foreach (var document in stored.History.Concat(stored.InFlight))
        {
            try
            {
                if (JsonSerializer.Deserialize<RunView>(document, Json) is { } run) loaded[run.RunId] = run;
            }
            catch (Exception ex)
            {
                logger.LogWarning("Run journal document skipped: {Error}", ex.Message);
            }
        }
        // The counter is raised to every id read, before the buckets are cut: a run's id and its start time do not follow one order.
        store.RaiseSequenceTo(loaded.Keys.Select(IdNumber).DefaultIfEmpty(0).Max());
        foreach (var run in loaded.Values.Where(r => r.IsOver)) _runs.Add(run);
        var interrupted = loaded.Values.Where(r => !r.IsOver).ToList();
        // Owed before the runs are ended: a crash between the two finds the runs in flight again and owes them again.
        store.AddOwed(
            [.. interrupted.Where(Started)
                .SelectMany(r => r.Plan!.Fragments.Select(f => new OwedFragment(r.RunId, f.Name, f.Node)))],
            DateTimeOffset.UtcNow + IRunStore.OwedFor);
        Owed = store.ReadOwed(DateTimeOffset.UtcNow);
        foreach (var run in interrupted)
        {
            var ended = run with { State = RunState.Error, FinishedAt = DateTimeOffset.UtcNow, Refusal = RestartedRefusal, SinkMayBePartial = Started(run) };
            Write(ended, []);
            _runs.Add(ended);
        }
        // A run over that a crash left among the runs in flight is moved to the history.
        foreach (var run in stored.InFlight.Select(d => Parse(d)).Where(r => r is { IsOver: true })) Write(run!, []);
        // Newest end first, the order the store reads back and Record keeps.
        _runs.Sort(Newest);
        var forgotten = Cut();
        if (forgotten.Count == 0) return;
        try
        {
            store.Forget(forgotten);
        }
        catch (StoreUnavailableException ex)
        {
            logger.LogWarning("{Count} runs beyond the {Kept} kept of their pipeline stay in the store until the next start: {Error}", forgotten.Count, KeptPerPipeline, ex.Message);
        }
    }

    /// <summary>The fragments of runs that started and were interrupted, whose replicas have not all given them up: their children may still run.</summary>
    public IReadOnlyList<OwedFragment> Owed { get; }

    /// <summary>A replica gave the fragment up. A store that cannot say so leaves it owed, to be asked again at the next start.</summary>
    public void Settle(OwedFragment owed)
    {
        try
        {
            _store.RemoveOwed(owed);
        }
        catch (StoreUnavailableException ex)
        {
            _logger.LogWarning("{Fragment} of run {RunId} stays owed; it is asked again at the next start: {Error}", owed.Fragment, owed.RunId, ex.Message);
        }
    }

    public string NextRunId() => $"run-{_store.NextSequence()}";

    /// <summary>
    /// Writes the run, whatever its state; one that is over also enters the history, and pushes out of its pipeline's bucket the runs
    /// beyond <see cref="KeptPerPipeline"/>, whose documents the same write erases.
    /// </summary>
    public void Record(RunView run)
    {
        if (!run.IsOver)
        {
            Write(run, []);
            return;
        }
        IReadOnlyCollection<string> forgotten;
        lock (_lock)
        {
            _runs.RemoveAll(r => r.RunId == run.RunId);
            _runs.Insert(0, run);
            forgotten = Cut();
        }
        try
        {
            Write(run, forgotten);
        }
        catch (StoreUnavailableException ex)
        {
            _logger.LogError("Run {RunId} is over but the store did not keep it; a restart ends it as interrupted: {Error}", run.RunId, ex.Message);
        }
    }

    public IReadOnlyList<RunView> List(string? pipelineId = null, int limit = 50)
    {
        lock (_lock) return _runs.Where(r => pipelineId is null || r.PipelineId == pipelineId).Take(limit).ToList();
    }

    /// <summary>The latest run that is over of each pipeline, newest first; a run of no pipeline is no pipeline's latest.</summary>
    public IReadOnlyList<RunView> Latest()
    {
        lock (_lock) return _runs.Where(r => r.PipelineId is not null).DistinctBy(r => r.PipelineId).ToList();
    }

    /// <summary>The latest run kept that <paramref name="match"/> accepts, however far back it is.</summary>
    public RunView? Find(Func<RunView, bool> match)
    {
        lock (_lock) return _runs.FirstOrDefault(match);
    }

    private void Write(RunView run, IReadOnlyCollection<string> forget) =>
        _store.Put(new StoredRun(run.RunId, JsonSerializer.Serialize(run, Json), run.IsOver, EndOf(run)), forget);

    private static DateTimeOffset EndOf(RunView run) => run.FinishedAt ?? run.StartedAt;

    /// <summary>Orders runs by the end the store orders them by, newest first, the later id first among equals.</summary>
    private static int Newest(RunView a, RunView b)
    {
        var byEnd = EndOf(b).CompareTo(EndOf(a));
        return byEnd != 0 ? byEnd : IdNumber(b.RunId).CompareTo(IdNumber(a.RunId));
    }

    /// <summary>
    /// Removes from the history, newest first, every run beyond <see cref="KeptPerPipeline"/> of its pipeline and returns their ids;
    /// the runs of no pipeline are one bucket. Under the lock, or before the journal is shared.
    /// </summary>
    private List<string> Cut()
    {
        var beyond = _runs.GroupBy(r => r.PipelineId).SelectMany(g => g.Skip(KeptPerPipeline)).Select(r => r.RunId).ToHashSet();
        _runs.RemoveAll(r => beyond.Contains(r.RunId));
        return [.. beyond];
    }

    private static RunView? Parse(string document)
    {
        try { return JsonSerializer.Deserialize<RunView>(document, Json); }
        catch (JsonException) { return null; }
    }

    /// <summary>A run whose fragments were launched: a sink may hold part of what they wrote.</summary>
    private static bool Started(RunView run) => run.State == RunState.Running && run.Plan is not null;

    private static long IdNumber(string runId) => long.TryParse(runId.Split('-').Last(), out var n) ? n : 0;
}
