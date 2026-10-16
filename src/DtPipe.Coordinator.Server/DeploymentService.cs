using DtPipe.Coordinator.Contracts;
using DtPipe.Jobs.Coordinator;

namespace DtPipe.Coordinator.Server;

/// <summary>
/// The deployments: what is deployed, where it came from, and its state. A library pipeline is <em>armed</em> at a commit with
/// its bindings (<see cref="ArmedDeployment"/>), kept in <see cref="DeploymentStore"/> and read back at start; its plan is
/// computed at each launch, so a deployment holds none. The Lab view's plan is fixed and lives in memory. Deploying asks every
/// connected replica of each node whether it would run its fragments (<c>Check</c>), for an early answer; it is ready when every
/// fragment has a replica that says so and that admission could place a run on. Nothing is left on a node, so a replica that
/// arrives later is as good as one that was asked, and one that goes away takes nothing with it. What decides a run is
/// admission, at the run. Each deployment has a gate, held while it is deployed or removed; a run passes it to read the state
/// and does not hold it while it runs.
/// </summary>
internal sealed class DeploymentService
{
    private static readonly TimeSpan RegistrationTimeout = TimeSpan.FromSeconds(30);

    private readonly DeploymentLedger _ledger;
    private readonly NodeInventory _inventory;
    private readonly INodeLink _link;
    private readonly ReplicaAdmission _admission;
    private readonly IPlanRegistry _planRegistry;
    private readonly LaunchPlanner _planner;
    private readonly DeploymentStore _store;
    private readonly IJobSource _jobs;
    private readonly EventBus _bus;
    private readonly ILogger _logger;

    public DeploymentService(
        DeploymentLedger ledger, NodeInventory inventory, INodeLink link, ReplicaAdmission admission,
        IPlanRegistry planRegistry, LaunchPlanner planner, DeploymentStore store, IJobSource jobs, EventBus bus, ILogger logger)
    {
        (_ledger, _inventory, _link, _admission, _planRegistry, _planner, _store, _jobs, _bus, _logger) =
            (ledger, inventory, link, admission, planRegistry, planner, store, jobs, bus, logger);
        // An armed deployment is an intention: it is ready as it comes back, and a run decides at admission whether it can go.
        foreach (var armed in _store.Load().OrderBy(a => a.ArmedAt))
        {
            _ledger.Deployments[armed.PipelineId] = new Deployment
            {
                PipelineId = armed.PipelineId, Armed = armed, Origin = "library", DeployedAt = armed.ArmedAt, State = DeploymentState.Ready,
            };
            _ledger.LastDeployed = armed.PipelineId;
        }
    }

    /// <summary>The plan the Lab view deployed last: what it shows and what a run naming no pipeline runs. Null when that is an armed pipeline.</summary>
    public DistributionPlan? ActivePlan
    {
        get
        {
            lock (_ledger.Lock)
                return _ledger.LastDeployed is not null && _ledger.Deployments.TryGetValue(_ledger.LastDeployed, out var d) && d.Armed is null ? d.Plan : null;
        }
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
            try { if (deployment.Plan is { } plan) eligibility = await _admission.EvaluateAsync(RunFragmentsOf(plan)); }
            catch (Exception ex) when (ex is ConflictException) { /* its node has no identity, or a disabled one: no replica can run it */ }
            lock (_ledger.Lock) views.Add(View(deployment, eligibility));
        }
        return views;
    }

    /// <summary>The plan id a pipeline is deployed from, or null when it is not deployed.</summary>
    public string? DeployedPlanId(string pipelineId)
    {
        lock (_ledger.Lock) return _ledger.Deployments.GetValueOrDefault(pipelineId)?.PlanId;
    }

    /// <summary>The fixed plan of a pipeline, or the last plan computed for it when it is armed.</summary>
    public DistributionPlan? PlanOf(string pipelineId)
    {
        lock (_ledger.Lock) return _ledger.Deployments.GetValueOrDefault(pipelineId)?.Plan;
    }

    /// <summary>What a library pipeline is armed with, or null when it is not armed.</summary>
    public ArmedDeployment? ArmedOf(string pipelineId)
    {
        lock (_ledger.Lock) return _ledger.Deployments.GetValueOrDefault(pipelineId)?.Armed;
    }

    /// <summary>Deploys a plan fixed by the Lab view: in memory only, and it replaces whatever the pipeline was deployed with.</summary>
    public async Task DeployAsync(DistributionPlan plan, string origin, CancellationToken ct)
    {
        if (!plan.Deployable)
            throw new ConflictException("This plan cannot be deployed: " +
                string.Join(" ", plan.Errors.Append(plan.FlowRejection ?? "").Where(s => s.Length > 0)));
        // A plan is checked against the matrix as it is now.
        if (PlanFlow.Judge(_planRegistry, plan.Id, plan.Edges).Rejection is { } rejection) throw new ConflictException(rejection);
        var offline = plan.Fragments.Where(f => _inventory.ReplicasOf(f.Node).Count == 0).Select(f => f.Node).Distinct().ToList();
        if (offline.Count > 0) throw new ConflictException($"Offline: {string.Join(", ", offline)}.");

        await InstallAsync(new Deployment { PipelineId = plan.PipelineId, Plan = plan, Origin = origin, DeployedAt = DateTimeOffset.UtcNow }, ct);
    }

    /// <summary>
    /// Arms a library pipeline: <paramref name="armed"/> and its <paramref name="job"/>. The plan a launch would make now must
    /// hold and every node must answer for its fragments; a refused arming is not kept, and the pipeline's earlier deployment
    /// goes with it, as for a fixed plan.
    /// </summary>
    public async Task ArmAsync(ArmedDeployment armed, string job, CancellationToken ct)
    {
        var launch = await _planner.ChooseAsync(armed.PipelineId, job, armed.Bindings, ct);
        if (!launch.Ok) throw new ConflictException(launch.Refusal);
        await InstallAsync(new Deployment
        {
            PipelineId = armed.PipelineId, Plan = launch.Plan, Armed = armed, Job = job, Origin = "library", DeployedAt = armed.ArmedAt,
        }, ct);
    }

    private async Task InstallAsync(Deployment deployment, CancellationToken ct)
    {
        var pipelineId = deployment.PipelineId;
        Deployment? previous;
        lock (_ledger.Lock)
        {
            _ledger.ThrowIfBusy(pipelineId);
            previous = _ledger.Deployments.GetValueOrDefault(pipelineId);
        }
        if (previous is not null) await previous.Gate.WaitAsync(ct);

        await deployment.Gate.WaitAsync(ct);
        try
        {
            lock (_ledger.Lock)
            {
                _ledger.Deployments[pipelineId] = deployment;
                _ledger.LastDeployed = pipelineId;
            }
            // Memory and disk change together: what replaces an armed deployment leaves no file behind.
            _store.Delete(pipelineId);
            Publish(deployment);

            await CheckAsync(deployment, ct);
            deployment.State = DeploymentState.Ready;
            if (deployment.Armed is { } armed) _store.Save(armed);
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

    /// <summary>
    /// The plan a launch of <paramref name="deployment"/> runs: the fixed plan, or, for an armed deployment, the one computed now
    /// from its job and its bindings at the armed commit. <c>Bindings</c> are those the plan was made under.
    /// </summary>
    public async Task<(Launch Launch, IReadOnlyDictionary<string, Binding> Bindings)> PlanForRunAsync(Deployment deployment, CancellationToken ct)
    {
        if (deployment.Armed is not { } armed) return (new Launch(deployment.Plan!, null, []), new Dictionary<string, Binding>());
        var job = deployment.Job ??= await _jobs.JobAtAsync(armed.PipelineId, armed.Commit, ct)
            ?? throw new ConflictException($"The job of '{armed.PipelineId}' cannot be read at {armed.Commit[..Math.Min(8, armed.Commit.Length)]}.");
        var launch = await _planner.ChooseAsync(armed.PipelineId, job, armed.Bindings, ct);
        if (launch.Ok) deployment.Plan = launch.Plan;
        return (launch, armed.Bindings);
    }

    /// <summary>The nodes a run of <paramref name="deployment"/> holds, runners excepted: those its bindings name, or its fixed plan's.</summary>
    public IReadOnlySet<string> HeldNodes(Deployment deployment, IReadOnlyDictionary<string, NodeRole> roles) =>
        deployment.Armed is { } armed
            ? RunDispatch.DataNodes(armed.Bindings.Values.Select(b => b.Node), roles)
            : RunDispatch.DataNodes(deployment.Plan!, roles);

    /// <summary>Drops the deployment, if it is still the one registered for its pipeline.</summary>
    private void Forget(Deployment deployment)
    {
        lock (_ledger.Lock)
        {
            if (!_ledger.Deployments.TryGetValue(deployment.PipelineId, out var current) || current != deployment) return;
            _ledger.Deployments.Remove(deployment.PipelineId);
            if (_ledger.LastDeployed == deployment.PipelineId)
                _ledger.LastDeployed = _ledger.Deployments.Values.OrderByDescending(d => d.DeployedAt).FirstOrDefault()?.PipelineId;
        }
        _store.Delete(deployment.PipelineId);
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
                if (_ledger.LastDeployed == pipelineId) _ledger.LastDeployed = _ledger.Deployments.Values.OrderByDescending(d => d.DeployedAt).FirstOrDefault()?.PipelineId;
            }
            _store.Delete(pipelineId);
            _bus.Publish("deploy", new { planId = deployment.PlanId, pipelineId, state = "undeployed" });
            PublishDeployments();
        }
        finally
        {
            deployment.Gate.Release();
        }
    }

    private static FragmentSpec SpecOf(PlannedFragment fragment) => LaunchPlanner.SpecOf(fragment);

    /// <summary>
    /// The plan's fragments as a run names them, read from the rights as they are now: the subject each node connects
    /// under, which admission binds a fragment to, and the groups its replicas' data plane clients must hold.
    /// </summary>
    public IReadOnlyList<RunFragment> RunFragmentsOf(DistributionPlan plan) => _planner.RunFragmentsOf(plan);

    /// <summary>
    /// Asks every connected replica of each node whether it would run its fragments, then waits, up to the registration
    /// timeout, until each fragment has a replica that said yes and that a run could be placed on. This is an early
    /// word and nothing stays on any replica. A replica that refuses is not the answer while another may do (a rolling
    /// update leaves some at the old brick): a fragment fails at once only when every replica asked has answered and none
    /// said yes, naming the node and why the first said no; otherwise it waits for one that did.
    /// </summary>
    private async Task CheckAsync(Deployment deployment, CancellationToken ct)
    {
        var plan = deployment.Plan!;
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
        d.PipelineId, d.PlanId, d.Origin, d.State, d.DeployedAt, d.Message,
        (d.Plan?.Fragments ?? []).Select(f => new DeployedFragment(f.Name, f.Node, f.Version, eligibility?.GetValueOrDefault(f.Name)?.Eligible.Count ?? 0)).ToList(),
        d.Plan?.Edges.Count ?? 0,
        _ledger.Sent.Where(t => !t.Running).Concat(_ledger.Queued).Count(t => t.View.PipelineId == d.PipelineId),
        _ledger.Sent.FirstOrDefault(t => t.Running && t.View.PipelineId == d.PipelineId)?.View.RunId,
        d.Armed?.Commit, d.Armed?.Bindings.Values.Select(b => b.Node).Distinct().Order(StringComparer.Ordinal).ToList());

    private void Publish(Deployment deployment)
    {
        _bus.Publish("deploy", new { planId = deployment.PlanId, pipelineId = deployment.PipelineId, state = deployment.State.Word(), message = deployment.Message });
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
