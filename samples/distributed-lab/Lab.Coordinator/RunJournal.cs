using System.Text.Json;
using System.Text.Json.Serialization;
using DtPipe.Coordinator;

namespace DtPipe.Lab.Coordinator;

public sealed record RunView(
    string RunId, string PlanId, string State, DateTimeOffset StartedAt, DateTimeOffset? FinishedAt,
    IReadOnlyDictionary<string, string>? Pins, RunResult? Result, string? Description, string? Refusal,
    string? PipelineId = null, DateTimeOffset? QueuedAt = null)
{
    [JsonIgnore] public bool IsOver => State is not ("queued" or "running");
}

/// <summary>
/// Every finished run, one JSON file each under <c>.state/runs/&lt;pipeline&gt;/</c>, read back at
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

    public RunJournal(LabOptions options, ILogger<RunJournal> logger)
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
        var dir = Path.Combine(_dir, Safe(run.PipelineId ?? run.PlanId));
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

    private static string Safe(string name) =>
        string.Concat(name.Select(c => char.IsLetterOrDigit(c) || c is '-' or '_' ? c : '_'));
}
