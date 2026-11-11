using DtPipe.Cli.Pipeline;
using DtPipe.Core.Abstractions.Dag;
using DtPipe.Core.Models;
using DtPipe.Core.Pipelines.Dag;
using DtPipe.Feedback;

namespace DtPipe.Engine;

/// <summary>
/// What a run asks of the process that hosts it. <see cref="JobExecutor"/> calls each member at the point
/// the run reaches it, inside the one-run gate, so the host's console and signal handling belong to one run.
/// </summary>
public interface IRunHost
{
    /// <summary>
    /// Turns the user's interrupt into <paramref name="userCancellation"/> and sets the run's logging up;
    /// disposing the result stops listening for the interrupt.
    /// </summary>
    IDisposable BeginRun(GlobalOptions globals, CancellationTokenSource userCancellation);

    /// <summary>The branch a sample of a DAG with several branches shows.</summary>
    string ChooseSampleBranch(JobDagDefinition dag);

    void ShowTopology(JobDagDefinition dag);

    void ShowTopology(JobDefinition job);

    /// <summary>Runs <paramref name="run"/>, the whole DAG, under whatever display the host gives it.</summary>
    Task<int> RunDagAsync(JobDagDefinition dag, GlobalOptions globals, Func<Task<int>> run, CancellationToken ct);

    /// <summary>
    /// The branches that completed, shown once the DAG has ended. <paramref name="exitCode"/> is the DAG's verdict: a
    /// host shows a non-zero one as such, never as a success summary.
    /// </summary>
    void ShowResults(IReadOnlyList<BranchSummary> results, JobDagDefinition dag, int exitCode);

    /// <summary>The run was refused before any branch started, one plain-text reason per error.</summary>
    void ShowRefusal(RunRefusal refusal, IEnumerable<string> errors);

    /// <summary>What the orchestrator reported while the DAG ran.</summary>
    void ShowDagEvent(DagLogEvent dagEvent);

    /// <summary>A branch stopped on <paramref name="fault"/>; <paramref name="reason"/> is its flattened chain.</summary>
    void ShowBranchFault(string reason, Exception fault);

    /// <summary>A branch stopped because the user interrupted the run.</summary>
    void ShowUserCancellation();
}

public enum RunRefusal
{
    Binding,
    Validation
}
