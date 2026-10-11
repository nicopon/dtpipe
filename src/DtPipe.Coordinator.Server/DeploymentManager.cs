using DtPipe.Coordinator;
using DtPipe.Coordinator.Contracts;
using TransportR.Interfaces;

namespace DtPipe.Coordinator.Server;

/// <summary>A request the server refuses in its current state (HTTP 409).</summary>
public sealed class ConflictException(string message) : Exception(message);

/// <summary><paramref name="Replicas"/>: how many replicas of the node could run the fragment now, which is what a run needs.</summary>
public sealed record DeployedFragment(string Name, string Node, string Version, int? Replicas = null);

public sealed record DeploymentView(
    string PipelineId, string PlanId, string Origin, string State, DateTimeOffset DeployedAt, string? Message,
    IReadOnlyList<DeployedFragment> Fragments, int Edges, int Queued, string? RunningRunId);

/// <summary>
/// Deploys plans and drives runs through the coordinator's own <see cref="IRunOrchestrator"/>, which places,
/// launches, wires and judges; this class only asks. A deployment is the coordinator's: the plan, where it came from,
/// and a state. Deploying asks every connected replica of each node whether it would run its fragments (<c>Check</c>), for
/// an early answer; it is ready when every fragment has a replica that says so and that admission could place a run on.
/// Nothing is left on a node, so a replica that arrives later is as good as one that was asked, and one that goes away
/// takes nothing with it. What decides a run is admission, at the run.
/// <para>
/// Several pipelines may be deployed at once, their fragments told apart by name
/// (<c>&lt;pipeline&gt;@&lt;node&gt;</c>). A run is sent to the orchestrator once no run already
/// sent, and no older run still waiting, uses one of its data nodes (<see cref="RunDispatch"/>): runs
/// on disjoint data nodes are in flight together, and a runner never counts. A run sent holds its data
/// nodes until it finishes. Each deployment has a gate, held while it is deployed or removed; a run passes it to read the
/// state and does not hold it while it runs.
/// </para>
/// </summary>
public sealed class DeploymentManager
{
    private static readonly TimeSpan RegistrationTimeout = TimeSpan.FromSeconds(30);

    private sealed class Deployment
    {
        public required DistributionPlan Plan;
        public required string Origin;
        public required DateTimeOffset DeployedAt;
        public string State = "deploying";
        public string? Message;
        public readonly SemaphoreSlim Gate = new(1, 1);
    }

    private sealed class Ticket(RunView view)
    {
        public RunView View = view;
        public readonly CancellationTokenSource Cancel = new();
        /// <summary>The data nodes it holds, from when it is sent until it finishes.</summary>
        public IReadOnlySet<string> Held = new HashSet<string>();
        public bool Running => View.State == "running";
    }

    private readonly NodeInventory _inventory;
    private readonly INodeLink _link;
    private readonly IRunOrchestrator _orchestrator;
    private readonly ReplicaAdmission _admission;
    private readonly IFlowControlService _flowControl;
    private readonly RightsStore _rights;
    private readonly EventBus _bus;
    private readonly RunJournal _journal;
    private readonly ILogger<DeploymentManager> _logger;

    private readonly object _lock = new();
    private readonly Dictionary<string, Deployment> _deployments = new(StringComparer.Ordinal);
    // A run is in exactly one of the two lists until it finishes: waiting to be sent, then sent
    // (waiting on its deployment's gate, then running).
    private readonly List<Ticket> _queued = [];
    private readonly List<Ticket> _sent = [];
    private string? _lastDeployed;

    public DeploymentManager(
        NodeInventory inventory, INodeLink link, IRunOrchestrator orchestrator, ReplicaAdmission admission,
        IFlowControlService flowControl, RightsStore rights, EventBus bus, RunJournal journal, ILogger<DeploymentManager> logger)
    {
        (_inventory, _link, _orchestrator, _admission, _flowControl, _rights, _bus, _journal, _logger) =
            (inventory, link, orchestrator, admission, flowControl, rights, bus, journal, logger);
    }

    /// <summary>The plan deployed last: what the Lab view shows and what a run naming no pipeline runs.</summary>
    public DistributionPlan? ActivePlan
    {
        get { lock (_lock) return _lastDeployed is not null && _deployments.TryGetValue(_lastDeployed, out var d) ? d.Plan : null; }
    }

    /// <summary>The oldest run in flight, else the next waiting, else the last finished: of one pipeline, or of any.</summary>
    public RunView? CurrentRun(string? pipelineId = null)
    {
        lock (_lock)
            return Active().FirstOrDefault(t => pipelineId is null || t.View.PipelineId == pipelineId)?.View
                ?? _journal.List(pipelineId, limit: 1).FirstOrDefault();
    }

    /// <summary>The runs in flight, oldest first, then the waiting ones in the order they came.</summary>
    public IReadOnlyList<RunView> Queue
    {
        get { lock (_lock) return Active().Select(t => t.View).ToList(); }
    }

    private IEnumerable<Ticket> Active() =>
        _sent.Where(t => t.Running).OrderBy(t => t.View.StartedAt)
            .Concat(_sent.Where(t => !t.Running).Concat(_queued).OrderBy(t => t.View.QueuedAt));

    /// <summary>The deployments, each fragment with the number of replicas that could run it now.</summary>
    public async Task<IReadOnlyList<DeploymentView>> DeploymentsAsync()
    {
        List<Deployment> deployments;
        lock (_lock) deployments = _deployments.Values.OrderByDescending(d => d.DeployedAt).ToList();
        var views = new List<DeploymentView>();
        foreach (var deployment in deployments)
        {
            IReadOnlyDictionary<string, FragmentEligibility>? eligibility = null;
            try { eligibility = await _admission.EvaluateAsync(RunFragmentsOf(deployment.Plan)); }
            catch (Exception ex) when (ex is ConflictException) { /* its node has no identity, or a disabled one: no replica can run it */ }
            lock (_lock) views.Add(View(deployment, eligibility));
        }
        return views;
    }

    /// <summary>The plan id a pipeline is deployed from, or null when it is not deployed.</summary>
    public string? DeployedPlanId(string pipelineId)
    {
        lock (_lock) return _deployments.GetValueOrDefault(pipelineId)?.Plan.Id;
    }

    public DistributionPlan? PlanOf(string pipelineId)
    {
        lock (_lock) return _deployments.GetValueOrDefault(pipelineId)?.Plan;
    }

    public async Task DeployAsync(DistributionPlan plan, string origin, CancellationToken ct)
    {
        if (plan.Fragments.Any(f => f.Form == FragmentForm.Unknown))
            throw new ConflictException("This plan does not say how each fragment reaches its node: distribute the pipeline again.");
        if (!plan.Deployable)
            throw new ConflictException("This plan cannot be deployed: " +
                string.Join(" ", plan.Errors.Append(plan.FlowRejection ?? "").Where(s => s.Length > 0)));
        // A plan read back from the library is checked against the matrix as it is now.
        var refused = plan.Edges.Where(e => !_flowControl.CanSendTo(e.ProducerGroups.ToHashSet(), e.ConsumerGroups.ToHashSet())).ToList();
        if (refused.Count > 0)
            throw new ConflictException("The flow matrix refuses " + string.Join(", ", refused.Select(e => $"{string.Join("+", e.ProducerGroups)} -> {string.Join("+", e.ConsumerGroups)}")) + ".");
        var offline = plan.Fragments.Where(f => _inventory.ReplicasOf(f.Node).Count == 0).Select(f => f.Node).Distinct().ToList();
        if (offline.Count > 0) throw new ConflictException($"Offline: {string.Join(", ", offline)}.");

        Deployment? previous;
        lock (_lock)
        {
            ThrowIfBusy(plan.PipelineId);
            previous = _deployments.GetValueOrDefault(plan.PipelineId);
        }
        if (previous is not null) await previous.Gate.WaitAsync(ct);

        var deployment = new Deployment { Plan = plan, Origin = origin, DeployedAt = DateTimeOffset.UtcNow };
        await deployment.Gate.WaitAsync(ct);
        try
        {
            lock (_lock)
            {
                _deployments[plan.PipelineId] = deployment;
                _lastDeployed = plan.PipelineId;
            }
            Publish(deployment);

            await CheckAsync(deployment, ct);
            deployment.State = "ready";
            Publish(deployment);
        }
        catch (Exception ex)
        {
            // A deployment is all of its fragments or none: a refused one is not kept.
            deployment.State = "failed";
            deployment.Message = ex.Message;
            Publish(deployment);
            Forget(deployment);
            throw;
        }
        finally
        {
            deployment.Gate.Release();
            previous?.Gate.Release();
        }
    }

    /// <summary>Drops the deployment, if it is still the one registered for its pipeline.</summary>
    private void Forget(Deployment deployment)
    {
        lock (_lock)
        {
            if (!_deployments.TryGetValue(deployment.Plan.PipelineId, out var current) || current != deployment) return;
            _deployments.Remove(deployment.Plan.PipelineId);
            if (_lastDeployed == deployment.Plan.PipelineId)
                _lastDeployed = _deployments.Values.OrderByDescending(d => d.DeployedAt).FirstOrDefault()?.Plan.PipelineId;
        }
        PublishDeployments();
    }

    public async Task UndeployAsync(string pipelineId, CancellationToken ct)
    {
        Deployment deployment;
        lock (_lock)
        {
            deployment = _deployments.GetValueOrDefault(pipelineId) ?? throw new ConflictException($"'{pipelineId}' is not deployed.");
            ThrowIfBusy(pipelineId);
        }
        await deployment.Gate.WaitAsync(ct);
        try
        {
            lock (_lock)
            {
                _deployments.Remove(pipelineId);
                if (_lastDeployed == pipelineId) _lastDeployed = _deployments.Values.OrderByDescending(d => d.DeployedAt).FirstOrDefault()?.Plan.PipelineId;
            }
            _bus.Publish("deploy", new { planId = deployment.Plan.Id, pipelineId, state = "undeployed" });
            PublishDeployments();
        }
        finally
        {
            deployment.Gate.Release();
        }
    }

    /// <summary>Queues a run of <paramref name="pipelineId"/>, or of the plan deployed last when it is null.</summary>
    public RunView Enqueue(string? pipelineId)
    {
        Ticket ticket;
        lock (_lock)
        {
            pipelineId ??= _lastDeployed ?? throw new ConflictException("Deploy a plan first.");
            var deployment = _deployments.GetValueOrDefault(pipelineId) ?? throw new ConflictException($"'{pipelineId}' is not deployed.");
            if (deployment.State == "failed") throw new ConflictException($"The deployment of '{pipelineId}' failed: {deployment.Message}");
            var now = DateTimeOffset.UtcNow;
            ticket = new Ticket(new RunView(_journal.NextRunId(), deployment.Plan.Id, "queued", now, null, null, null, null, pipelineId, now));
            _queued.Add(ticket);
        }
        var view = ticket.View;
        _bus.Publish("run", view);
        PublishDeployments();
        Dispatch();
        return view;
    }

    /// <summary>
    /// Cancels a run in flight or still waiting. With no id, the one in flight: several in flight must
    /// be named, and none leaves everything as it is.
    /// </summary>
    public void Cancel(string? runId = null)
    {
        Ticket? waiting;
        lock (_lock)
        {
            if (runId is null)
            {
                var running = _sent.Where(t => t.Running).ToList();
                if (running.Count > 1)
                    throw new ConflictException($"{running.Count} runs are in flight: name the one to cancel.");
                running.SingleOrDefault()?.Cancel.Cancel();
                return;
            }
            // A run sent ends through its token: the orchestrator cancels it in flight, and a wait on
            // its deployment's gate ends it before it starts.
            if (_sent.FirstOrDefault(t => t.View.RunId == runId) is { } sent)
            {
                sent.Cancel.Cancel();
                return;
            }
            // Out of the queue under the lock, so the dispatcher can no longer send it.
            waiting = _queued.FirstOrDefault(t => t.View.RunId == runId);
            if (waiting is not null) _queued.Remove(waiting);
        }
        if (waiting is not null)
            Finish(waiting, waiting.View with { State = "cancelled", StartedAt = DateTimeOffset.UtcNow, Refusal = "Cancelled while queued." });
    }

    /// <summary>Fault injection: ends the child of the fragment's run on every replica of its node.</summary>
    public async Task KillAsync(string fragmentName, CancellationToken ct)
    {
        var node = RequireFragment(fragmentName).Fragment.Node;
        var replicas = _inventory.ReplicasOf(node);
        if (replicas.Count == 0) throw new ConflictException($"{node} is offline.");
        foreach (var (key, _) in replicas)
        {
            using var bound = CancellationTokenSource.CreateLinkedTokenSource(ct);
            bound.CancelAfter(TimeSpan.FromSeconds(10));
            await _link.KillAsync(key, fragmentName, bound.Token);
        }
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
        lock (_lock)
        {
            // A run whose pipeline is gone holds nothing: it is sent at once and refused.
            var waiting = _queued.Select(t => (Ticket: t, Nodes: _deployments.GetValueOrDefault(t.View.PipelineId!) is { } d
                ? RunDispatch.DataNodes(d.Plan, roles) : new HashSet<string>())).ToList();
            var startable = RunDispatch.Startable(
                _sent.Select(t => t.Held), waiting.Select(w => new RunDispatch.Waiting(w.Ticket.View.RunId, w.Nodes)).ToList());
            foreach (var (ticket, nodes) in waiting.Where(w => startable.Contains(w.Ticket.View.RunId)))
            {
                ticket.Held = nodes;
                _queued.Remove(ticket);
                _sent.Add(ticket);
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
            lock (_lock) sent = _sent.Contains(ticket);
            if (sent) Finish(ticket, ticket.View with { State = "error", Refusal = ex.Message });
        }
    }

    private async Task RunOneAsync(Ticket ticket)
    {
        var view = ticket.View;
        var pipelineId = view.PipelineId!;
        Deployment? deployment;
        lock (_lock)
        {
            if (!_sent.Contains(ticket)) return;
            deployment = _deployments.GetValueOrDefault(pipelineId);
        }

        // The deployment's gate is passed to read its state, not held: it is held while it is deployed or removed.
        IReadOnlyList<RunFragment> fragments;
        try
        {
            if (deployment is null) throw new ConflictException($"'{pipelineId}' is no longer deployed.");
            await deployment.Gate.WaitAsync(ticket.Cancel.Token);
            try
            {
                if (deployment.State != "ready") throw new ConflictException($"'{pipelineId}' is not ready: {deployment.State} {deployment.Message}".Trim());
            }
            finally
            {
                deployment.Gate.Release();
            }
            fragments = RunFragmentsOf(deployment.Plan);
        }
        catch (Exception ex)
        {
            lock (_lock) if (!_sent.Contains(ticket)) return;
            var cancelled = ex is OperationCanceledException;
            Finish(ticket, view with { State = cancelled ? "cancelled" : "refused", StartedAt = DateTimeOffset.UtcNow, Refusal = cancelled ? "Cancelled while queued." : ex.Message });
            return;
        }

        var plan = deployment.Plan;
        view = view with { State = "running", StartedAt = DateTimeOffset.UtcNow };
        lock (_lock) ticket.View = view;
        _bus.Publish("run", view);
        PublishDeployments();

        var spec = new RunSpec(
            view.RunId, fragments,
            plan.Edges.Select(e => new RunEdge(e.ProducerFragment, e.ProducerAlias, e.ConsumerFragment, e.ConsumerAlias)).ToList());

        RunView final;
        try
        {
            var result = await _orchestrator.RunAsync(spec, ticket.Cancel.Token);
            final = view with { State = result.Outcome.ToString().ToLowerInvariant(), Result = result, Description = result.Describe() };
        }
        catch (Exception ex) when (ex is NoEligibleReplicaException or UnboundFragmentException)
        {
            final = view with { State = "refused", Refusal = ex.Message };
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Run {RunId} failed unexpectedly", spec.RunId);
            final = view with { State = "error", Refusal = ex.Message };
        }
        Finish(ticket, final);
    }

    private void Finish(Ticket ticket, RunView final)
    {
        final = final with { FinishedAt = DateTimeOffset.UtcNow };
        _journal.Record(final);
        lock (_lock)
        {
            _queued.Remove(ticket);
            _sent.Remove(ticket);
        }
        // The token source is left to the collector: a run finished here may still be waiting on
        // its deployment's gate with that token.
        _bus.Publish("run", final);
        PublishDeployments();
        Dispatch();
    }

    /// <summary>The fragment as a replica is asked to run it.</summary>
    private static FragmentSpec SpecOf(PlannedFragment fragment) => new(
        fragment.Name, fragment.Version, fragment.Edges, fragment.Composition, fragment.Form == FragmentForm.Bricks ? null : fragment.Yaml);

    /// <summary>
    /// The plan's fragments as a run names them, read from the rights as they are now: the subject each node connects
    /// under, which admission binds a fragment to, and the groups its replicas' data plane clients must hold.
    /// </summary>
    private IReadOnlyList<RunFragment> RunFragmentsOf(DistributionPlan plan) =>
        plan.Fragments.Select(f => new RunFragment(SpecOf(f), _rights.SubjectOf(f.Node), _rights.GroupsOf(f.Node))).ToList();

    /// <summary>
    /// Asks every connected replica of each node whether it would run its fragments, then waits, up to the registration
    /// timeout, until each fragment has a replica that said yes and that a run could be placed on. This is an early
    /// word and nothing stays on any replica. A replica that refuses is not the answer while another may do (a rolling
    /// update leaves some at the old brick): a fragment fails at once only when every replica asked has answered and none
    /// said yes, naming the node and why the first said no; otherwise it waits for one that did.
    /// </summary>
    private async Task CheckAsync(Deployment deployment, CancellationToken ct)
    {
        var plan = deployment.Plan;
        var fragments = RunFragmentsOf(plan);

        using var bound = CancellationTokenSource.CreateLinkedTokenSource(ct);
        bound.CancelAfter(RegistrationTimeout);
        var asks = plan.Fragments
            .SelectMany(f => _inventory.ReplicasOf(f.Node).Select(r => (Fragment: f, r.Key, Answer: _link.CheckAsync(r.Key, SpecOf(f), bound.Token))))
            .ToList();
        foreach (var ask in asks)
            try { await ask.Answer; } catch (Exception) { /* told below, by fragment */ }

        bool Said(PlannedFragment f, bool yes) =>
            asks.Any(a => a.Fragment == f && a.Answer.IsCompletedSuccessfully && a.Answer.Result.Ok == yes);

        // Every replica answered and none said yes, at least one saying why not: that is the answer, at once.
        var refusals = plan.Fragments.Where(f => !Said(f, true) && Said(f, false)).Select(f =>
            $"{f.Name} was not registered: refused by {f.Node}: " +
            asks.Where(a => a.Fragment == f && a.Answer.IsCompletedSuccessfully && !a.Answer.Result.Ok).Select(a => a.Answer.Result.Reason).First()).ToList();
        if (refusals.Count > 0) throw new ConflictException(string.Join(" ", refusals));

        var deadline = DateTime.UtcNow + RegistrationTimeout;
        while (true)
        {
            ct.ThrowIfCancellationRequested();
            var eligibility = await _admission.EvaluateAsync(fragments);
            var missing = plan.Fragments.Where(f =>
            {
                var said = asks.Where(a => a.Fragment == f && a.Answer.IsCompletedSuccessfully && a.Answer.Result.Ok).Select(a => a.Key).ToHashSet();
                return !eligibility[f.Name].Eligible.Any(said.Contains);
            }).ToList();
            if (missing.Count == 0) return;
            if (DateTime.UtcNow > deadline)
                throw new ConflictException("No replica could run: " + string.Join(" ", missing.Select(f =>
                    $"{f.Name} ({eligibility[f.Name].Reason ?? "no replica that was asked is eligible."})")));
            await Task.Delay(200, ct);
        }
    }

    private void ThrowIfBusy(string pipelineId)
    {
        if (_sent.Concat(_queued).Any(t => t.View.PipelineId == pipelineId))
            throw new ConflictException($"A run of '{pipelineId}' is queued or in flight.");
    }

    private (Deployment Deployment, PlannedFragment Fragment) RequireFragment(string fragmentName)
    {
        lock (_lock)
        {
            foreach (var deployment in _deployments.Values)
                if (deployment.Plan.Fragments.FirstOrDefault(f => f.Name == fragmentName) is { } fragment)
                    return (deployment, fragment);
        }
        throw new ConflictException($"'{fragmentName}' is not a fragment of a deployed plan.");
    }

    private DeploymentView View(Deployment d, IReadOnlyDictionary<string, FragmentEligibility>? eligibility) => new(
        d.Plan.PipelineId, d.Plan.Id, d.Origin, d.State, d.DeployedAt, d.Message,
        d.Plan.Fragments.Select(f => new DeployedFragment(f.Name, f.Node, f.Version, eligibility?.GetValueOrDefault(f.Name)?.Eligible.Count ?? 0)).ToList(),
        d.Plan.Edges.Count,
        _sent.Where(t => !t.Running).Concat(_queued).Count(t => t.View.PipelineId == d.Plan.PipelineId),
        _sent.FirstOrDefault(t => t.Running && t.View.PipelineId == d.Plan.PipelineId)?.View.RunId);

    private void Publish(Deployment deployment)
    {
        _bus.Publish("deploy", new { planId = deployment.Plan.Id, pipelineId = deployment.Plan.PipelineId, state = deployment.State, message = deployment.Message });
        PublishDeployments();
    }

    // One snapshot at a time, each taken when its turn comes: a later one is never overtaken by an earlier one.
    private readonly SemaphoreSlim _publishing = new(1, 1);

    private void PublishDeployments() => _ = Task.Run(async () =>
    {
        await _publishing.WaitAsync();
        try { _bus.Publish("deployments", await DeploymentsAsync()); }
        catch (Exception ex) { _logger.LogWarning(ex, "The deployments could not be published"); }
        finally { _publishing.Release(); }
    });
}
