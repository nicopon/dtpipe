using System.Collections.Concurrent;

namespace DtPipe.DryRun;

/// <summary>
/// Collects the reports a sample run produces, per branch, so a caller that is not a terminal
/// can read what a human would have seen rendered.
///
/// It exists because MCP and the CLI want the same run to answer differently — a table for a
/// person, JSON for a model — and the alternative is a second execution path that produces the
/// model's answer, which is the duplication this cycle removes. One run, one report, two
/// presentations.
/// </summary>
public sealed class SampleReportCollector
{
    private readonly ConcurrentDictionary<string, SampleReport> _reports = new(StringComparer.OrdinalIgnoreCase);

    /// <summary>Off by default: an ordinary run collects nothing and keeps no rows alive.</summary>
    public bool Enabled { get; set; }

    /// <summary>
    /// The alias of a single-branch job's one branch, set by the run that executes it. Its report
    /// arrives with no alias - a linear run carries none - and is filed under this name, not under a
    /// fixed "main": a caller looking the branch up by the name the job gives it (<c>split</c>, the
    /// MCP sample) would otherwise find nothing, or a branch the job does not have.
    /// </summary>
    public string? LinearBranch { get; set; }

    public void Publish(string? branchAlias, SampleReport report)
    {
        if (!Enabled) return;
        _reports[branchAlias ?? LinearBranch ?? "main"] = report;
    }

    public IReadOnlyDictionary<string, SampleReport> Reports => _reports;

    private readonly ConcurrentDictionary<string, string> _checkpoints = new(StringComparer.OrdinalIgnoreCase);

    /// <summary>
    /// The key of the checkpoint each branch committed, in a real run as in a sample: the one place a caller
    /// that cannot read the console learns where a branch's rows were kept.
    /// </summary>
    public IReadOnlyDictionary<string, string> Checkpoints => _checkpoints;

    /// <summary>Notes the checkpoint a branch committed; ignored while the collector is off.</summary>
    public void RecordCheckpoint(string? branchAlias, string key)
    {
        if (!Enabled) return;
        _checkpoints[branchAlias ?? LinearBranch ?? "main"] = key;
    }

    private readonly SemaphoreSlim _turn = new(1, 1);

    /// <summary>
    /// One collecting run at a time: the collector belongs to the process, so a second run's
    /// <see cref="Clear"/> would empty the first's reports and the first to finish would switch
    /// <see cref="Enabled"/> off under the other. A caller holds the turn from before it clears
    /// until it has read <see cref="Reports"/>; concurrent samples then run one after the other.
    /// </summary>
    public async Task<IDisposable> TakeTurnAsync(CancellationToken ct)
    {
        await _turn.WaitAsync(ct);
        return new Turn(_turn);
    }

    private sealed class Turn(SemaphoreSlim turn) : IDisposable
    {
        private int _released;
        public void Dispose()
        {
            if (Interlocked.Exchange(ref _released, 1) == 0) turn.Release();
        }
    }

    public void Clear()
    {
        _reports.Clear();
        _checkpoints.Clear();
        LinearBranch = null;
    }
}
