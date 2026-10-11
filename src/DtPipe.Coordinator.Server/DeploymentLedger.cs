using DtPipe.Coordinator.Contracts;

namespace DtPipe.Coordinator.Server;

/// <summary>
/// What the deployments and the run queue share, and the one lock that guards it: the deployments by pipeline, the
/// pipeline deployed last, and the runs, each in exactly one of two lists until it finishes - waiting to be sent, then
/// sent (waiting on its deployment's gate, then running). A decision that reads both sides, a deployment refused while a
/// run of its pipeline is queued, a run accepted only on a deployment that is not failed, is taken under <see cref="Lock"/>.
/// </summary>
internal sealed class DeploymentLedger
{
    public readonly object Lock = new();
    public readonly Dictionary<string, Deployment> Deployments = new(StringComparer.Ordinal);
    public readonly List<Ticket> Queued = [];
    public readonly List<Ticket> Sent = [];
    public string? LastDeployed;

    /// <summary>The runs in flight, oldest first, then the waiting ones in the order they came. The caller holds <see cref="Lock"/>.</summary>
    public IEnumerable<Ticket> Active() =>
        Sent.Where(t => t.Running).OrderBy(t => t.View.StartedAt)
            .Concat(Sent.Where(t => !t.Running).Concat(Queued).OrderBy(t => t.View.QueuedAt));

    /// <summary>Refuses a deployment change while a run of the pipeline is queued or in flight. The caller holds <see cref="Lock"/>.</summary>
    public void ThrowIfBusy(string pipelineId)
    {
        if (Sent.Concat(Queued).Any(t => t.View.PipelineId == pipelineId))
            throw new ConflictException($"A run of '{pipelineId}' is queued or in flight.");
    }
}

internal sealed class Deployment
{
    public required DistributionPlan Plan;
    public required string Origin;
    public required DateTimeOffset DeployedAt;
    public string State = "deploying";
    public string? Message;
    public readonly SemaphoreSlim Gate = new(1, 1);
}

internal sealed class Ticket(RunView view)
{
    public RunView View = view;
    public readonly CancellationTokenSource Cancel = new();
    /// <summary>The data nodes it holds, from when it is sent until it finishes.</summary>
    public IReadOnlySet<string> Held = new HashSet<string>();
    public bool Running => View.State == "running";
}
