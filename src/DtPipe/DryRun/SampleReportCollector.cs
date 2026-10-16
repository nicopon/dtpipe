using System.Collections.Concurrent;

namespace DtPipe.DryRun;

/// <summary>
/// What one run reports to a caller that is not a terminal: the sample report of each branch and the
/// key of each checkpoint it committed. A run owns one; nothing else writes to it.
///
/// It exists because MCP and the CLI want the same run to answer differently — a table for a
/// person, JSON for a model — and the alternative is a second execution path that produces the
/// model's answer. One run, one report, two presentations.
/// </summary>
public sealed class RunReports
{
    private readonly ConcurrentDictionary<string, SampleReport> _reports = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentDictionary<string, string> _checkpoints = new(StringComparer.OrdinalIgnoreCase);

    /// <summary>
    /// The alias of a single-branch job's one branch. Its report arrives with no alias - a linear
    /// run carries none - and is filed under this name, not under a fixed "main": a caller looking
    /// the branch up by the name the job gives it (<c>split</c>, the MCP sample) would otherwise find
    /// nothing, or a branch the job does not have.
    /// </summary>
    public string? LinearBranch { get; set; }

    public IReadOnlyDictionary<string, SampleReport> Reports => _reports;

    /// <summary>
    /// The key of the checkpoint each branch committed, in a real run as in a sample: the one place a caller
    /// that cannot read the console learns where a branch's rows were kept.
    /// </summary>
    public IReadOnlyDictionary<string, string> Checkpoints => _checkpoints;

    public void Publish(string? branchAlias, SampleReport report) =>
        _reports[branchAlias ?? LinearBranch ?? "main"] = report;

    public void RecordCheckpoint(string? branchAlias, string key) =>
        _checkpoints[branchAlias ?? LinearBranch ?? "main"] = key;
}

/// <summary>
/// Hands the report of a branch to the run that is executing it. The engine and the console observer
/// are singletons that publish to this one object; it forwards to the <see cref="RunReports"/> the
/// calling run armed with <see cref="Begin"/>, and drops the report when none is armed, so an
/// ordinary run collects nothing and keeps no rows alive.
///
/// The armed collection follows the run's own async flow, branches included: two runs at once each
/// read their own, and neither needs the other to wait.
/// </summary>
public sealed class SampleReportCollector
{
    private readonly AsyncLocal<RunReports?> _current = new();

    /// <summary>
    /// Arms a collection for the calling flow. Call it from a frame that encloses the whole run: a value
    /// set inside an <c>async</c> method is gone once that method returns.
    /// </summary>
    public RunScope Begin()
    {
        var run = new RunReports();
        var scope = new RunScope(this, run, _current.Value);
        _current.Value = run;
        return scope;
    }

    /// <summary>Names the one branch of a linear run; ignored when no run armed a collection.</summary>
    public string? LinearBranch
    {
        get => _current.Value?.LinearBranch;
        set
        {
            if (_current.Value is { } run) run.LinearBranch = value;
        }
    }

    public void Publish(string? branchAlias, SampleReport report) => _current.Value?.Publish(branchAlias, report);

    /// <summary>Notes the checkpoint a branch committed; ignored when no run armed a collection.</summary>
    public void RecordCheckpoint(string? branchAlias, string key) => _current.Value?.RecordCheckpoint(branchAlias, key);

    public sealed class RunScope(SampleReportCollector owner, RunReports run, RunReports? previous) : IDisposable
    {
        public RunReports Run => run;

        public void Dispose() => owner._current.Value = previous;
    }
}
