using System.Text.Json;
using System.Text.Json.Serialization;
using DtPipe.Coordinator;

namespace DtPipe.Coordinator.Server;

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
/// <see cref="Plan"/> is the plan the run ran, journaled with it. <see cref="LaunchedBy"/> names who started it: a person's email,
/// or a service's client id (an id never holds an <c>@</c>). <see cref="IdempotencyKey"/> is the key a service sent with the
/// request, when it sent one; it means something only together with <see cref="LaunchedBy"/>.
/// </summary>
public sealed record RunView(
    string RunId, string PlanId, [property: JsonConverter(typeof(WireEnumConverter<RunState>))] RunState State, DateTimeOffset StartedAt,
    DateTimeOffset? FinishedAt,
    RunResult? Result, string? Description, string? Refusal,
    string? PipelineId = null, DateTimeOffset? QueuedAt = null, RunPlan? Plan = null, string? LaunchedBy = null, string? IdempotencyKey = null)
{
    [JsonIgnore] public bool IsOver => State is not (RunState.Queued or RunState.Running);
}

/// <summary>
/// Every run, a JSON document each in the <see cref="IRunStore"/>, read back at start. Runs are what happened, not what was
/// designed: the library's git history never holds them. A run is written when it is queued, again when it starts and when it
/// ends, so a restart finds what it interrupted: those runs are ended then (<see cref="RestartedRefusal"/>), and the ones that had
/// started are <see cref="Orphans"/>, whose children may still run. History (<see cref="List"/>, <see cref="Find"/>) holds the runs
/// that are over and keeps the latest <see cref="IRunStore.Window"/>, in memory as in the store; a run in flight is in the store
/// only. A run not yet over that cannot be written throws <see cref="StoreUnavailableException"/>, so none starts without its
/// record; one that is over is kept in memory whatever the store says, since refusing it would help nobody, and a store that missed
/// it leaves it in flight for the next start to end.
/// </summary>
public sealed class RunJournal
{
    private const int Kept = IRunStore.Window;
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
        // The counter is raised to every id read, before the window: a run's id and its start time do not follow one order.
        store.RaiseSequenceTo(loaded.Keys.Select(IdNumber).DefaultIfEmpty(0).Max());
        foreach (var run in loaded.Values.Where(r => r.IsOver)) _runs.Add(run);
        foreach (var run in loaded.Values.Where(r => !r.IsOver))
        {
            if (run.State == RunState.Running && run.Plan is not null) Orphans.Add(run);
            var ended = run with { State = RunState.Error, FinishedAt = DateTimeOffset.UtcNow, Refusal = RestartedRefusal };
            Write(ended);
            _runs.Add(ended);
        }
        // A run over that a crash left among the runs in flight is moved to the history.
        foreach (var run in stored.InFlight.Select(d => Parse(d)).Where(r => r is { IsOver: true })) Write(run!);
        _runs.Sort((a, b) => b.StartedAt.CompareTo(a.StartedAt));
        if (_runs.Count > Kept) _runs.RemoveRange(Kept, _runs.Count - Kept);
    }

    /// <summary>The runs a restart found started and not over: their fragments were placed on replicas, and their children may still run.</summary>
    public List<RunView> Orphans { get; } = [];

    public string NextRunId() => $"run-{_store.NextSequence()}";

    /// <summary>Writes the run, whatever its state; one that is over also enters the history.</summary>
    public void Record(RunView run)
    {
        if (!run.IsOver)
        {
            Write(run);
            return;
        }
        lock (_lock)
        {
            _runs.RemoveAll(r => r.RunId == run.RunId);
            _runs.Insert(0, run);
            if (_runs.Count > Kept) _runs.RemoveAt(_runs.Count - 1);
        }
        try
        {
            Write(run);
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

    /// <summary>The latest run kept that <paramref name="match"/> accepts, however far back it is.</summary>
    public RunView? Find(Func<RunView, bool> match)
    {
        lock (_lock) return _runs.FirstOrDefault(match);
    }

    private void Write(RunView run) =>
        _store.Put(new StoredRun(run.RunId, JsonSerializer.Serialize(run, Json), run.IsOver, run.FinishedAt ?? run.StartedAt));

    private static RunView? Parse(string document)
    {
        try { return JsonSerializer.Deserialize<RunView>(document, Json); }
        catch (JsonException) { return null; }
    }

    private static long IdNumber(string runId) => long.TryParse(runId.Split('-').Last(), out var n) ? n : 0;
}
