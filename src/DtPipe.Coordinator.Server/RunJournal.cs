using System.Text.Json;
using System.Text.Json.Serialization;
using DtPipe.Coordinator;

namespace DtPipe.Coordinator.Server;

/// <summary>A fragment of the plan a run ran, as the plan named it.</summary>
public sealed record RunPlanFragment(string Name, string Node, string Version);

/// <summary>
/// A binding as a run met it: the brick it names and that brick's version then, the mode it is held to and what that mode found
/// (<see cref="BindingVerdicts.Exact"/> for <see cref="BindingModes.Strict"/>: the branch's text is the brick's).
/// </summary>
public sealed record RunPlanBinding(string Alias, string Node, string Brick, string BrickVersion, string Mode, string Verdict);

/// <summary>Verdicts of a binding's mode, as <see cref="RunPlanBinding.Verdict"/>.</summary>
public static class BindingVerdicts
{
    /// <summary>The branch's canonical form equals the brick's: nothing can have changed.</summary>
    public const string Exact = "exact";
}

/// <summary>
/// The plan a run ran, computed when it started: its id, the runner it chose (null when no branch needed one), its fragments
/// and its bindings. A run refused before it had a plan has none.
/// </summary>
public sealed record RunPlan(
    string PlanId, string? Runner, IReadOnlyList<RunPlanFragment> Fragments, IReadOnlyList<RunPlanBinding> Bindings);

/// <summary><see cref="Plan"/> is the plan the run ran, journaled with it.</summary>
public sealed record RunView(
    string RunId, string PlanId, [property: JsonConverter(typeof(WireEnumConverter<RunState>))] RunState State, DateTimeOffset StartedAt,
    DateTimeOffset? FinishedAt,
    RunResult? Result, string? Description, string? Refusal,
    string? PipelineId = null, DateTimeOffset? QueuedAt = null, RunPlan? Plan = null)
{
    [JsonIgnore] public bool IsOver => State is not (RunState.Queued or RunState.Running);
}

/// <summary>
/// Every finished run, one JSON file each under <c>runs/&lt;pipeline&gt;/</c> in the state directory, read back at
/// start. Runs are what happened, not what was designed: the library's git history never holds them.
/// </summary>
public sealed class RunJournal
{
    private const int Kept = 200;
    private static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web)
    {
        WriteIndented = true,
        Converters = { new JsonStringEnumConverter() },
    };

    private readonly string _dir;
    private readonly object _lock = new();
    private readonly List<RunView> _runs = [];
    private int _seq;

    public RunJournal(CoordinatorServerOptions options, ILogger<RunJournal> logger)
    {
        _dir = Path.Combine(options.StateDir, "runs");
        Directory.CreateDirectory(_dir);
        foreach (var file in Directory.EnumerateFiles(_dir, "*.json", SearchOption.AllDirectories))
        {
            try
            {
                if (JsonSerializer.Deserialize<RunView>(File.ReadAllText(file), Json) is { } run) _runs.Add(run);
            }
            catch (Exception ex)
            {
                logger.LogWarning("Run journal entry {File} skipped: {Error}", file, ex.Message);
            }
        }
        _runs.Sort((a, b) => b.StartedAt.CompareTo(a.StartedAt));
        _seq = _runs.Select(r => int.TryParse(r.RunId.Split('-').Last(), out var n) ? n : 0).DefaultIfEmpty(0).Max();
    }

    public string NextRunId() => $"run-{Interlocked.Increment(ref _seq)}";

    public void Record(RunView run)
    {
        var dir = Path.Combine(_dir, DirectoryName(run.PipelineId ?? run.PlanId));
        Directory.CreateDirectory(dir);
        File.WriteAllText(Path.Combine(dir, run.RunId + ".json"), JsonSerializer.Serialize(run, Json));
        lock (_lock)
        {
            _runs.RemoveAll(r => r.RunId == run.RunId);
            _runs.Insert(0, run);
            if (_runs.Count > Kept) _runs.RemoveAt(_runs.Count - 1);
        }
    }

    public IReadOnlyList<RunView> List(string? pipelineId = null, int limit = 50)
    {
        lock (_lock) return _runs.Where(r => pipelineId is null || r.PipelineId == pipelineId).Take(limit).ToList();
    }

    private static string DirectoryName(string name) =>
        string.Concat(name.Select(c => char.IsLetterOrDigit(c) || c is '-' or '_' ? c : '_'));
}
