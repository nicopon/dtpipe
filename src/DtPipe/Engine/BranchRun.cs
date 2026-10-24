using System.Collections.Concurrent;
using DtPipe.Cli.Pipeline;
using DtPipe.Core.Models;
using DtPipe.Core.Pipelines.Dag;
using DtPipe.Feedback;

namespace DtPipe.Engine;

/// <summary>
/// One branch for <see cref="LinearPipelineService"/> to run: the job as written, with its input and output
/// unresolved, how the orchestrator wired it, and what the run around it asks.
/// </summary>
public sealed record BranchRun(JobDefinition Job)
{
    public CliJobContext? Context { get; init; }

    /// <summary>The name the run knows the branch by; a branch's failure is recorded under it.</summary>
    public string? Alias { get; init; }

    /// <summary>True for a branch of a DAG, whose alias names its channel and its summary.</summary>
    public bool IsDag { get; init; }

    /// <summary>The orchestrator's endpoints and fingerprint for this branch.</summary>
    public BranchChannelContext? Channels { get; init; }

    public ConcurrentQueue<BranchSummary>? Results { get; init; }

    /// <summary>The run's options: how strictly flags bind, the sample's branch, quiet.</summary>
    public GlobalOptions? Globals { get; init; }

    /// <summary>Cancelled by the user's interrupt alone, so the branch can answer 130 (F16).</summary>
    public CancellationToken UserCancellation { get; init; }
}
