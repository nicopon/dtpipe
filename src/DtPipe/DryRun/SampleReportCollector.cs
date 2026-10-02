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

    public void Clear()
    {
        _reports.Clear();
        LinearBranch = null;
    }
}
