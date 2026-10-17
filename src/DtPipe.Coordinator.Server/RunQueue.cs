using DtPipe.Coordinator.Contracts;

namespace DtPipe.Coordinator.Server;

/// <summary>A run <see cref="RunQueue.Launch"/> answered, and whether an earlier request had already made it.</summary>
public sealed record LaunchedRun(RunView Run, bool Replayed);

/// <summary>
/// The runs: queued, sent to the coordinator's own <see cref="IRunOrchestrator"/>, which places, launches, wires and
/// judges, and journaled when they finish; this class only asks. A run is sent once no run already sent, and no older run
/// still waiting, uses one of its data nodes (<see cref="RunDispatch"/>): runs on disjoint data nodes are in flight
/// together, and a runner never counts. A run sent holds its data nodes until it finishes.
/// </summary>
internal sealed class RunQueue
{
    private readonly DeploymentLedger _ledger;
    private readonly DeploymentService _deployments;
    private readonly LaunchPlanner _planner;
    private readonly NodeInventory _inventory;
    private readonly IRunOrchestrator _orchestrator;
    private readonly EventBus _bus;
    private readonly RunJournal _journal;
    private readonly ILogger _logger;

    public RunQueue(
        DeploymentLedger ledger, DeploymentService deployments, LaunchPlanner planner, NodeInventory inventory, IRunOrchestrator orchestrator,
        EventBus bus, RunJournal journal, ILogger logger)
    {
        (_ledger, _deployments, _planner, _inventory, _orchestrator, _bus, _journal, _logger) =
            (ledger, deployments, planner, inventory, orchestrator, bus, journal, logger);
    }

    /// <summary>The oldest run in flight, else the next waiting, else the last finished: of one pipeline, or of any.</summary>
    public RunView? CurrentRun(string? pipelineId = null)
    {
        lock (_ledger.Lock)
            return _ledger.Active().FirstOrDefault(t => pipelineId is null || t.View.PipelineId == pipelineId)?.View
                ?? _journal.List(pipelineId, limit: 1).FirstOrDefault();
    }

    /// <summary>The runs in flight, oldest first, then the waiting ones in the order they came.</summary>
    public IReadOnlyList<RunView> Queue
    {
        get { lock (_ledger.Lock) return _ledger.Active().Select(t => t.View).ToList(); }
    }

    /// <summary>The run <paramref name="runId"/>: in flight or waiting, else journaled.</summary>
    public RunView? Find(string runId)
    {
        lock (_ledger.Lock)
            return _ledger.Active().FirstOrDefault(t => t.View.RunId == runId)?.View ?? _journal.Find(r => r.RunId == runId);
    }

    /// <summary>Queues a run of <paramref name="pipelineId"/>, or of the plan deployed last when it is null, launched by <paramref name="by"/>.</summary>
    public RunView Enqueue(string? pipelineId, string? by = null) => Launch(pipelineId, by, null).Run;

    /// <summary>
    /// <see cref="Enqueue"/> for a caller that may ask twice: a second request from <paramref name="by"/> with the same
    /// <paramref name="idempotencyKey"/> answers the run the first one made, whatever became of it, and queues none. The same
    /// key for another pipeline is refused. The key is looked up where the run is queued, under the ledger's lock, among the runs
    /// in flight and the journaled ones: a run is journaled before it leaves the ledger, so none is missed between the two.
    /// </summary>
    public LaunchedRun Launch(string? pipelineId, string? by, string? idempotencyKey)
    {
        Ticket ticket;
        lock (_ledger.Lock)
        {
            pipelineId ??= _ledger.LastDeployed ?? throw new ConflictException("Deploy a plan first.");
            if (idempotencyKey is not null && EarlierRun(by, idempotencyKey) is { } earlier)
            {
                if (earlier.PipelineId != pipelineId)
                    throw new ConflictException($"The key '{idempotencyKey}' started a run of '{earlier.PipelineId}', not of '{pipelineId}'.");
                return new LaunchedRun(earlier, true);
            }
            var deployment = _ledger.Deployments.GetValueOrDefault(pipelineId) ?? throw new ConflictException($"'{pipelineId}' is not deployed.");
            if (deployment.State == DeploymentState.Failed) throw new ConflictException($"The deployment of '{pipelineId}' failed: {deployment.Message}");
            var now = DateTimeOffset.UtcNow;
            ticket = new Ticket(new RunView(
                _journal.NextRunId(), deployment.PlanId, RunState.Queued, now, null, null, null, null, pipelineId, now, LaunchedBy: by, IdempotencyKey: idempotencyKey));
            _ledger.Queued.Add(ticket);
        }
        var view = ticket.View;
        _bus.Publish("run", view);
        _deployments.PublishDeployments();
        Dispatch();
        return new LaunchedRun(view, false);
    }

    private RunView? EarlierRun(string? by, string key)
    {
        bool Matches(RunView r) => r.IdempotencyKey == key && r.LaunchedBy == by;
        return _ledger.Active().Select(t => t.View).FirstOrDefault(Matches) ?? _journal.Find(Matches);
    }

    /// <summary>
    /// Cancels a run in flight or still waiting. With no id, the one in flight: several in flight must
    /// be named, and none leaves everything as it is.
    /// </summary>
    public void Cancel(string? runId = null)
    {
        Ticket? waiting;
        RunView? cancelled = null;
        lock (_ledger.Lock)
        {
            if (runId is null)
            {
                var running = _ledger.Sent.Where(t => t.Running).ToList();
                if (running.Count > 1)
                    throw new ConflictException($"{running.Count} runs are in flight: name the one to cancel.");
                running.SingleOrDefault()?.Cancel.Cancel();
                return;
            }
            // A run sent ends through its token: the orchestrator cancels it in flight, and a wait on
            // its deployment's gate ends it before it starts.
            if (_ledger.Sent.FirstOrDefault(t => t.View.RunId == runId) is { } sent)
            {
                sent.Cancel.Cancel();
                return;
            }
            // Out of the queue under the lock, so the dispatcher can no longer send it.
            waiting = _ledger.Queued.FirstOrDefault(t => t.View.RunId == runId);
            if (waiting is not null)
            {
                // Journaled before it leaves the ledger, as Finish does, so a retry of its key finds it in one or the other.
                cancelled = waiting.View with { State = RunState.Cancelled, StartedAt = DateTimeOffset.UtcNow, FinishedAt = DateTimeOffset.UtcNow, Refusal = "Cancelled while queued." };
                _journal.Record(cancelled);
                _ledger.Queued.Remove(waiting);
            }
        }
        if (waiting is not null && cancelled is not null) Finish(waiting, cancelled);
    }

    /// <summary>
    /// Sends every waiting run that may start now (<see cref="RunDispatch"/>); runs when a run is
    /// queued and when one finishes, the only two moments a run can become free to start.
    /// </summary>
    private void Dispatch()
    {
        var roles = new Dictionary<string, NodeRole>(StringComparer.Ordinal);
        foreach (var node in _inventory.Snapshot()) roles[node.Name] = node.Role;

        var start = new List<Ticket>();
        lock (_ledger.Lock)
        {
            // A run whose pipeline is gone holds nothing: it is sent at once and refused.
            var waiting = _ledger.Queued.Select(t => (Ticket: t, Nodes: _ledger.Deployments.GetValueOrDefault(t.View.PipelineId!) is { } d
                ? _deployments.HeldNodes(d, roles) : new HashSet<string>())).ToList();
            var startable = RunDispatch.Startable(
                _ledger.Sent.Select(t => t.Held), waiting.Select(w => new RunDispatch.Waiting(w.Ticket.View.RunId, w.Nodes)).ToList());
            foreach (var (ticket, nodes) in waiting.Where(w => startable.Contains(w.Ticket.View.RunId)))
            {
                ticket.Held = nodes;
                _ledger.Queued.Remove(ticket);
                _ledger.Sent.Add(ticket);
                start.Add(ticket);
            }
        }
        foreach (var ticket in start) _ = Task.Run(() => StartAsync(ticket));
    }

    private async Task StartAsync(Ticket ticket)
    {
        try
        {
            await RunOneAsync(ticket);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Run {RunId} could not be handled", ticket.View.RunId);
            // Its data nodes are released only when it finishes.
            bool sent;
            lock (_ledger.Lock) sent = _ledger.Sent.Contains(ticket);
            if (sent) Finish(ticket, ticket.View with { State = RunState.Error, Refusal = ex.Message });
        }
    }

    private async Task RunOneAsync(Ticket ticket)
    {
        var view = ticket.View;
        var pipelineId = view.PipelineId!;
        Deployment? deployment;
        lock (_ledger.Lock)
        {
            if (!_ledger.Sent.Contains(ticket)) return;
            deployment = _ledger.Deployments.GetValueOrDefault(pipelineId);
        }

        // The deployment's gate is passed to read its state, not held: it is held while it is deployed or removed.
        // The plan is computed here, once the run is sent and outside the ledger's lock: it asks no node, but it reads the library.
        DistributionPlan plan;
        RunPlan journaled;
        IReadOnlyList<RunFragment> fragments;
        try
        {
            if (deployment is null) throw new ConflictException($"'{pipelineId}' is no longer deployed.");
            await deployment.Gate.WaitAsync(ticket.Cancel.Token);
            try
            {
                if (deployment.State != DeploymentState.Ready) throw new ConflictException($"'{pipelineId}' is not ready: {deployment.State.Word()} {deployment.Message}".Trim());
            }
            finally
            {
                deployment.Gate.Release();
            }
            var (launch, bindings, verdicts) = await _deployments.PlanForRunAsync(deployment, ticket.Cancel.Token);
            if (!launch.Ok) throw new ConflictException(launch.Refusal);
            plan = launch.Plan;
            var roles = new Dictionary<string, NodeRole>(StringComparer.Ordinal);
            foreach (var node in _inventory.Snapshot()) roles[node.Name] = node.Role;
            if (RunDispatch.Unheld(plan, ticket.Held, roles) is { Count: > 0 } unheld)
                throw new ConflictException($"The plan of '{pipelineId}' reaches {string.Join(", ", unheld)}, which the run does not hold: it is not run.");
            journaled = _planner.Describe(plan, launch.Runner, bindings, verdicts);
            fragments = _deployments.RunFragmentsOf(plan);
        }
        catch (Exception ex)
        {
            lock (_ledger.Lock) if (!_ledger.Sent.Contains(ticket)) return;
            var cancelled = ex is OperationCanceledException;
            Finish(ticket, view with { State = cancelled ? RunState.Cancelled : RunState.Refused, StartedAt = DateTimeOffset.UtcNow, Refusal = cancelled ? "Cancelled while queued." : ex.Message });
            return;
        }

        view = view with { State = RunState.Running, StartedAt = DateTimeOffset.UtcNow, PlanId = plan.Id, Plan = journaled };
        lock (_ledger.Lock) ticket.View = view;
        _bus.Publish("run", view);
        _deployments.PublishDeployments();

        var spec = new RunSpec(
            view.RunId, fragments,
            plan.Edges.Select(e => new RunEdge(e.ProducerFragment, e.ProducerAlias, e.ConsumerFragment, e.ConsumerAlias)).ToList());

        RunView final;
        try
        {
            var result = await _orchestrator.RunAsync(spec, ticket.Cancel.Token);
            final = view with { State = StateOf(result.Outcome), Result = result, Description = result.Describe() };
        }
        catch (Exception ex) when (ex is NoEligibleReplicaException or UnboundFragmentException)
        {
            final = view with { State = RunState.Refused, Refusal = ex.Message };
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Run {RunId} failed unexpectedly", spec.RunId);
            final = view with { State = RunState.Error, Refusal = ex.Message };
        }
        Finish(ticket, final);
    }

    private static RunState StateOf(RunOutcome outcome) => outcome switch
    {
        RunOutcome.Succeeded => RunState.Succeeded,
        RunOutcome.Failed => RunState.Failed,
        RunOutcome.Cancelled => RunState.Cancelled,
        _ => throw new ArgumentOutOfRangeException(nameof(outcome), outcome, "No state for this outcome."),
    };

    private void Finish(Ticket ticket, RunView final)
    {
        final = final with { FinishedAt = DateTimeOffset.UtcNow };
        _journal.Record(final);
        lock (_ledger.Lock)
        {
            _ledger.Queued.Remove(ticket);
            _ledger.Sent.Remove(ticket);
        }
        // The token source is left to the collector: a run finished here may still be waiting on
        // its deployment's gate with that token.
        _bus.Publish("run", final);
        _deployments.PublishDeployments();
        Dispatch();
    }
}
