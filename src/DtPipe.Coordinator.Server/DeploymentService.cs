using DtPipe.Coordinator.Contracts;
using TransportR.Interfaces;

namespace DtPipe.Coordinator.Server;

/// <summary>
/// The deployments: what is deployed, where it came from, and its state. Deploying asks every connected replica of each
/// node whether it would run its fragments (<c>Check</c>), for an early answer; it is ready when every fragment has a
/// replica that says so and that admission could place a run on. Nothing is left on a node, so a replica that arrives
/// later is as good as one that was asked, and one that goes away takes nothing with it. What decides a run is admission,
/// at the run. Each deployment has a gate, held while it is deployed or removed; a run passes it to read the state and
/// does not hold it while it runs.
/// </summary>
internal sealed class DeploymentService
{
    private static readonly TimeSpan RegistrationTimeout = TimeSpan.FromSeconds(30);

    private readonly DeploymentLedger _ledger;
    private readonly NodeInventory _inventory;
    private readonly INodeLink _link;
    private readonly ReplicaAdmission _admission;
    private readonly IFlowControlService _flowControl;
    private readonly RightsStore _rights;
    private readonly EventBus _bus;
    private readonly ILogger _logger;

    public DeploymentService(
        DeploymentLedger ledger, NodeInventory inventory, INodeLink link, ReplicaAdmission admission,
        IFlowControlService flowControl, RightsStore rights, EventBus bus, ILogger logger)
    {
        (_ledger, _inventory, _link, _admission, _flowControl, _rights, _bus, _logger) =
            (ledger, inventory, link, admission, flowControl, rights, bus, logger);
    }

    /// <summary>The plan deployed last: what the Lab view shows and what a run naming no pipeline runs.</summary>
    public DistributionPlan? ActivePlan
    {
        get { lock (_ledger.Lock) return _ledger.LastDeployed is not null && _ledger.Deployments.TryGetValue(_ledger.LastDeployed, out var d) ? d.Plan : null; }
    }

    /// <summary>The deployments, each fragment with the number of replicas that could run it now.</summary>
    public async Task<IReadOnlyList<DeploymentView>> DeploymentsAsync()
    {
        List<Deployment> deployments;
        lock (_ledger.Lock) deployments = _ledger.Deployments.Values.OrderByDescending(d => d.DeployedAt).ToList();
        var views = new List<DeploymentView>();
        foreach (var deployment in deployments)
        {
            IReadOnlyDictionary<string, FragmentEligibility>? eligibility = null;
            try { eligibility = await _admission.EvaluateAsync(RunFragmentsOf(deployment.Plan)); }
            catch (Exception ex) when (ex is ConflictException) { /* its node has no identity, or a disabled one: no replica can run it */ }
            lock (_ledger.Lock) views.Add(View(deployment, eligibility));
        }
        return views;
    }

    /// <summary>The plan id a pipeline is deployed from, or null when it is not deployed.</summary>
    public string? DeployedPlanId(string pipelineId)
    {
        lock (_ledger.Lock) return _ledger.Deployments.GetValueOrDefault(pipelineId)?.Plan.Id;
    }

    public DistributionPlan? PlanOf(string pipelineId)
    {
        lock (_ledger.Lock) return _ledger.Deployments.GetValueOrDefault(pipelineId)?.Plan;
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
        lock (_ledger.Lock)
        {
            _ledger.ThrowIfBusy(plan.PipelineId);
            previous = _ledger.Deployments.GetValueOrDefault(plan.PipelineId);
        }
        if (previous is not null) await previous.Gate.WaitAsync(ct);

        var deployment = new Deployment { Plan = plan, Origin = origin, DeployedAt = DateTimeOffset.UtcNow };
        await deployment.Gate.WaitAsync(ct);
        try
        {
            lock (_ledger.Lock)
            {
                _ledger.Deployments[plan.PipelineId] = deployment;
                _ledger.LastDeployed = plan.PipelineId;
            }
            Publish(deployment);

            await CheckAsync(deployment, ct);
            deployment.State = DeploymentState.Ready;
            Publish(deployment);
        }
        catch (Exception ex)
        {
            // A deployment is all of its fragments or none: a refused one is not kept.
            deployment.State = DeploymentState.Failed;
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
        lock (_ledger.Lock)
        {
            if (!_ledger.Deployments.TryGetValue(deployment.Plan.PipelineId, out var current) || current != deployment) return;
            _ledger.Deployments.Remove(deployment.Plan.PipelineId);
            if (_ledger.LastDeployed == deployment.Plan.PipelineId)
                _ledger.LastDeployed = _ledger.Deployments.Values.OrderByDescending(d => d.DeployedAt).FirstOrDefault()?.Plan.PipelineId;
        }
        PublishDeployments();
    }

    public async Task UndeployAsync(string pipelineId, CancellationToken ct)
    {
        Deployment deployment;
        lock (_ledger.Lock)
        {
            deployment = _ledger.Deployments.GetValueOrDefault(pipelineId) ?? throw new ConflictException($"'{pipelineId}' is not deployed.");
            _ledger.ThrowIfBusy(pipelineId);
        }
        await deployment.Gate.WaitAsync(ct);
        try
        {
            lock (_ledger.Lock)
            {
                _ledger.Deployments.Remove(pipelineId);
                if (_ledger.LastDeployed == pipelineId) _ledger.LastDeployed = _ledger.Deployments.Values.OrderByDescending(d => d.DeployedAt).FirstOrDefault()?.Plan.PipelineId;
            }
            _bus.Publish("deploy", new { planId = deployment.Plan.Id, pipelineId, state = "undeployed" });
            PublishDeployments();
        }
        finally
        {
            deployment.Gate.Release();
        }
    }

    /// <summary>The fragment as a replica is asked to run it.</summary>
    private static FragmentSpec SpecOf(PlannedFragment fragment) => new(
        fragment.Name, fragment.Version, fragment.Edges, fragment.Composition, fragment.Form == FragmentForm.Bricks ? null : fragment.Yaml);

    /// <summary>
    /// The plan's fragments as a run names them, read from the rights as they are now: the subject each node connects
    /// under, which admission binds a fragment to, and the groups its replicas' data plane clients must hold.
    /// </summary>
    public IReadOnlyList<RunFragment> RunFragmentsOf(DistributionPlan plan) =>
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

    private DeploymentView View(Deployment d, IReadOnlyDictionary<string, FragmentEligibility>? eligibility) => new(
        d.Plan.PipelineId, d.Plan.Id, d.Origin, d.State, d.DeployedAt, d.Message,
        d.Plan.Fragments.Select(f => new DeployedFragment(f.Name, f.Node, f.Version, eligibility?.GetValueOrDefault(f.Name)?.Eligible.Count ?? 0)).ToList(),
        d.Plan.Edges.Count,
        _ledger.Sent.Where(t => !t.Running).Concat(_ledger.Queued).Count(t => t.View.PipelineId == d.Plan.PipelineId),
        _ledger.Sent.FirstOrDefault(t => t.Running && t.View.PipelineId == d.Plan.PipelineId)?.View.RunId);

    private void Publish(Deployment deployment)
    {
        _bus.Publish("deploy", new { planId = deployment.Plan.Id, pipelineId = deployment.Plan.PipelineId, state = deployment.State.Word(), message = deployment.Message });
        PublishDeployments();
    }

    // One snapshot at a time, each taken when its turn comes: a later one is never overtaken by an earlier one.
    private readonly SemaphoreSlim _publishing = new(1, 1);

    public void PublishDeployments() => _ = Task.Run(async () =>
    {
        await _publishing.WaitAsync();
        try { _bus.Publish("deployments", await DeploymentsAsync()); }
        catch (Exception ex) { _logger.LogWarning(ex, "The deployments could not be published"); }
        finally { _publishing.Release(); }
    });
}
