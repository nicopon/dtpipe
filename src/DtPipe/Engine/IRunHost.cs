using DtPipe.Cli.Pipeline;
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

    void ShowResults(IReadOnlyList<BranchSummary> results, JobDagDefinition dag);
}
