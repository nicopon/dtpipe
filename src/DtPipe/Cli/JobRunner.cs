using System.Collections.Concurrent;
using System.Runtime.ExceptionServices;
using DtPipe.Cli.Pipeline;
using DtPipe.Core.Pipelines.Dag;
using DtPipe.Core.Models;
using DtPipe.DryRun;

namespace DtPipe.Cli;

/// <summary>What one run answered: its exit code, the reason each failed branch gave, and what it reported.</summary>
/// <param name="Fault">The exception the engine threw, with <paramref name="ExitCode"/> 1; null when it answered.</param>
public sealed record RunOutcome(
    int ExitCode,
    Exception? Fault,
    IReadOnlyDictionary<string, string> Failures,
    IReadOnlyDictionary<string, SampleReport> Reports,
    IReadOnlyDictionary<string, string> Checkpoints)
{
    /// <summary>For a caller that lets the engine's exception reach its own handler.</summary>
    public void ThrowIfFaulted()
    {
        if (Fault is not null) ExceptionDispatchInfo.Capture(Fault).Throw();
    }
}

/// <summary>
/// The one way a caller that is not the command line runs a resolved job: MCP, the studio, <c>split</c>
/// and <c>contract</c>. It arms the run's report collection around <see cref="JobService.ExecutePipelineAsync"/>
/// and hands the result back, so no caller passes a dictionary in or reads shared state out.
/// </summary>
public sealed class JobRunner(JobService jobService, SampleReportCollector collector)
{
    public async Task<RunOutcome> RunAsync(
        Dictionary<string, JobDefinition> jobs, JobDagDefinition dag, Dictionary<string, CliJobContext> contexts,
        GlobalOptions globals, CancellationToken ct)
    {
        var failures = new ConcurrentDictionary<string, string>();
        // Armed in this frame, which encloses the whole run: every branch inherits it.
        using var scope = collector.Begin();
        int exitCode;
        Exception? fault = null;
        try
        {
            exitCode = await jobService.ExecutePipelineAsync(jobs, dag, contexts, globals, ct, failures);
        }
        catch (Exception ex)
        {
            exitCode = 1;
            fault = ex;
        }

        return new RunOutcome(
            exitCode, fault,
            new Dictionary<string, string>(failures),
            new Dictionary<string, SampleReport>(scope.Run.Reports, StringComparer.OrdinalIgnoreCase),
            new Dictionary<string, string>(scope.Run.Checkpoints, StringComparer.OrdinalIgnoreCase));
    }
}
