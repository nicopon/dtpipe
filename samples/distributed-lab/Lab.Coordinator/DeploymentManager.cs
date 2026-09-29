using System.Threading.Channels;
using DtPipe.Coordinator;
using DtPipe.Lab.Contracts;
using Microsoft.AspNetCore.SignalR;
using TransportR.Interfaces;

namespace DtPipe.Lab.Coordinator;

/// <summary>A request the lab refuses in its current state (HTTP 409).</summary>
public sealed class LabConflictException(string message) : Exception(message);

public sealed record DeployedFragment(string Name, string Node, string Version);

public sealed record DeploymentView(
    string PipelineId, string PlanId, string Origin, string State, DateTimeOffset DeployedAt, string? Message,
    IReadOnlyList<DeployedFragment> Fragments, int Edges, int Queued, string? RunningRunId);

/// <summary>
/// Deploys plans on their nodes and drives runs through the coordinator's own
/// <see cref="IRunOrchestrator"/>, which admits, launches, wires and judges; this class only asks.
/// <para>
/// Several pipelines may be deployed at once, their fragments told apart by name
/// (<c>&lt;pipeline&gt;@&lt;node&gt;</c>). The orchestrator refuses a second run while one is in
/// flight, so runs wait in one queue and start one at a time. Each deployment has a gate, held
/// while it deploys, runs and re-arms: a run starts only on registered, unspent instances, and a
/// deployment is never replaced under a run.
/// </para>
/// </summary>
public sealed class DeploymentManager
{
    private static readonly TimeSpan RegistrationTimeout = TimeSpan.FromSeconds(30);

    private sealed class Deployment
    {
        public required LabPlan Plan;
        public required string Origin;
        public required DateTimeOffset DeployedAt;
        public string Generation = "";
        public string State = "deploying";
        public string? Message;
        public readonly SemaphoreSlim Gate = new(1, 1);
    }

    private sealed record Ticket(RunView View, CancellationTokenSource Cancel);

    private readonly NodeInventory _inventory;
    private readonly IHubContext<LabHub> _labHub;
    private readonly IRunOrchestrator _orchestrator;
    private readonly INodeRegistry _registry;
    private readonly IFlowControlService _flowControl;
    private readonly EventBus _bus;
    private readonly RunJournal _journal;
    private readonly ILogger<DeploymentManager> _logger;

    private readonly object _lock = new();
    private readonly Dictionary<string, Deployment> _deployments = new(StringComparer.Ordinal);
    private readonly List<Ticket> _queued = [];
    private readonly Channel<Ticket> _queue = Channel.CreateUnbounded<Ticket>();
    private Ticket? _current;
    private string? _lastDeployed;

    public DeploymentManager(
        NodeInventory inventory, IHubContext<LabHub> labHub, IRunOrchestrator orchestrator, INodeRegistry registry,
        IFlowControlService flowControl, EventBus bus, RunJournal journal, ILogger<DeploymentManager> logger)
    {
        (_inventory, _labHub, _orchestrator, _registry, _flowControl, _bus, _journal, _logger) =
            (inventory, labHub, orchestrator, registry, flowControl, bus, journal, logger);
        _ = Task.Run(WorkAsync);
    }

    /// <summary>The plan deployed last: what the Lab view shows and what a run naming no pipeline runs.</summary>
    public LabPlan? ActivePlan
    {
        get { lock (_lock) return _lastDeployed is not null && _deployments.TryGetValue(_lastDeployed, out var d) ? d.Plan : null; }
    }

    /// <summary>The run in flight, else the next queued, else the last finished.</summary>
    public RunView? CurrentRun
    {
        get { lock (_lock) return _current?.View ?? _queued.FirstOrDefault()?.View ?? _journal.List(limit: 1).FirstOrDefault(); }
    }

    public IReadOnlyList<RunView> Queue
    {
        get { lock (_lock) return [.. _current is null ? [] : new[] { _current.View }, .. _queued.Select(t => t.View)]; }
    }

    public IReadOnlyList<DeploymentView> Deployments
    {
        get { lock (_lock) return _deployments.Values.OrderByDescending(d => d.DeployedAt).Select(View).ToList(); }
    }

    public LabPlan? PlanOf(string pipelineId)
    {
        lock (_lock) return _deployments.GetValueOrDefault(pipelineId)?.Plan;
    }

    public async Task DeployAsync(LabPlan plan, string origin, CancellationToken ct)
    {
        if (!plan.Deployable)
            throw new LabConflictException("This plan cannot be deployed: " +
                string.Join(" ", plan.Errors.Append(plan.FlowRejection ?? "").Where(s => s.Length > 0)));
        // A plan read back from the library is checked against the matrix as it is now.
        var refused = plan.Edges.Where(e => !_flowControl.CanSendTo(new HashSet<string> { e.ProducerGroup }, new HashSet<string> { e.ConsumerGroup })).ToList();
        if (refused.Count > 0)
            throw new LabConflictException("The flow matrix refuses " + string.Join(", ", refused.Select(e => $"{e.ProducerGroup} -> {e.ConsumerGroup}")) + ".");
        var offline = plan.Fragments.Where(f => _inventory.ConnectionOf(f.Node) is null).Select(f => f.Node).Distinct().ToList();
        if (offline.Count > 0) throw new LabConflictException($"Offline: {string.Join(", ", offline)}.");

        Deployment? previous;
        lock (_lock)
        {
            ThrowIfBusy(plan.PipelineId);
            previous = _deployments.GetValueOrDefault(plan.PipelineId);
        }
        if (previous is not null) await previous.Gate.WaitAsync(ct);

        var deployment = new Deployment { Plan = plan, Origin = origin, DeployedAt = DateTimeOffset.UtcNow, Generation = NewGeneration() };
        await deployment.Gate.WaitAsync(ct);
        try
        {
            lock (_lock)
            {
                _deployments[plan.PipelineId] = deployment;
                _lastDeployed = plan.PipelineId;
            }
            Publish(deployment);

            var names = plan.Fragments.Select(f => f.Name).Concat(previous?.Plan.Fragments.Select(f => f.Name) ?? []).Distinct().ToList();
            await UndeployEverywhereAsync(names, ct);
            foreach (var fragment in plan.Fragments)
                await SendAsync(fragment.Node, LabHubMethods.Deploy,
                    new FragmentDeployment(fragment.Name, fragment.Yaml, fragment.Edges, deployment.Generation), ct);

            await WaitRegisteredAsync(deployment, ct);
            deployment.State = "ready";
            Publish(deployment);
        }
        catch (Exception ex)
        {
            deployment.State = "failed";
            deployment.Message = ex.Message;
            Publish(deployment);
            // A deployment is all of its fragments or none: what did register must not stay admissible.
            try { await UndeployEverywhereAsync(plan.Fragments.Select(f => f.Name).ToList(), CancellationToken.None); }
            catch (Exception cleanup) { _logger.LogWarning(cleanup, "Undeploying the fragments of {Pipeline} failed", plan.PipelineId); }
            throw;
        }
        finally
        {
            deployment.Gate.Release();
            previous?.Gate.Release();
        }
    }

    private async Task UndeployEverywhereAsync(IReadOnlyList<string> fragmentNames, CancellationToken ct)
    {
        foreach (var node in _inventory.Snapshot().Where(n => n.Online))
            foreach (var name in fragmentNames)
                await SendAsync(node.Name, LabHubMethods.Undeploy, name, ct);
    }

    public async Task UndeployAsync(string pipelineId, CancellationToken ct)
    {
        Deployment deployment;
        lock (_lock)
        {
            deployment = _deployments.GetValueOrDefault(pipelineId) ?? throw new LabConflictException($"'{pipelineId}' is not deployed.");
            ThrowIfBusy(pipelineId);
        }
        await deployment.Gate.WaitAsync(ct);
        try
        {
            foreach (var node in _inventory.Snapshot().Where(n => n.Online))
                foreach (var fragment in deployment.Plan.Fragments)
                    await SendAsync(node.Name, LabHubMethods.Undeploy, fragment.Name, ct);
            lock (_lock)
            {
                _deployments.Remove(pipelineId);
                if (_lastDeployed == pipelineId) _lastDeployed = _deployments.Values.OrderByDescending(d => d.DeployedAt).FirstOrDefault()?.Plan.PipelineId;
            }
            _bus.Publish("deploy", new { planId = deployment.Plan.Id, pipelineId, state = "undeployed" });
            _bus.Publish("deployments", Deployments);
        }
        finally
        {
            deployment.Gate.Release();
        }
    }

    /// <summary>Queues a run of <paramref name="pipelineId"/>, or of the plan deployed last when it is null.</summary>
    public RunView Enqueue(string? pipelineId, IReadOnlyDictionary<string, string>? pins)
    {
        Ticket ticket;
        lock (_lock)
        {
            pipelineId ??= _lastDeployed ?? throw new LabConflictException("Deploy a plan first.");
            var deployment = _deployments.GetValueOrDefault(pipelineId) ?? throw new LabConflictException($"'{pipelineId}' is not deployed.");
            if (deployment.State == "failed") throw new LabConflictException($"The deployment of '{pipelineId}' failed: {deployment.Message}");
            var now = DateTimeOffset.UtcNow;
            ticket = new Ticket(
                new RunView(_journal.NextRunId(), deployment.Plan.Id, "queued", now, null, pins, null, null, null, pipelineId, now),
                new CancellationTokenSource());
            _queued.Add(ticket);
        }
        _queue.Writer.TryWrite(ticket);
        _bus.Publish("run", ticket.View);
        _bus.Publish("deployments", Deployments);
        return ticket.View;
    }

    /// <summary>Cancels a run in flight or still queued; with no id, the one in flight.</summary>
    public void Cancel(string? runId = null)
    {
        Ticket? queued;
        lock (_lock)
        {
            if (runId is null || _current?.View.RunId == runId)
            {
                _current?.Cancel.Cancel();
                return;
            }
            queued = _queued.FirstOrDefault(t => t.View.RunId == runId);
        }
        // A queued run never started: it ends now, and the worker skips it when it comes up.
        if (queued is not null)
            Finish(queued, queued.View with { State = "cancelled", StartedAt = DateTimeOffset.UtcNow, Refusal = "Cancelled while queued." });
    }

    public Task KillAsync(string fragmentName, CancellationToken ct) =>
        SendAsync(RequireFragment(fragmentName).Fragment.Node, LabHubMethods.Kill, fragmentName, ct);

    /// <summary>
    /// A second instance of one fragment on the same node, from a job that differs by a comment:
    /// same behaviour, different version. Admission then refuses the run as misaligned.
    /// </summary>
    public async Task DeployVariantAsync(string fragmentName, CancellationToken ct)
    {
        var (deployment, fragment) = RequireFragment(fragmentName);
        lock (_lock) ThrowIfBusy(deployment.Plan.PipelineId);
        if (deployment.Gate.CurrentCount == 0) throw new LabConflictException("Fragments are being deployed.");
        var yaml = fragment.Yaml + $"# variant {Guid.NewGuid():N}\n";
        await SendAsync(fragment.Node, LabHubMethods.Deploy,
            new FragmentDeployment(fragment.Name, yaml, fragment.Edges, NewGeneration(), Instance: "variant"), ct);
        _bus.Publish("deploy", new { planId = deployment.Plan.Id, pipelineId = deployment.Plan.PipelineId, state = "variant", fragment = fragment.Name });
    }

    private async Task WorkAsync()
    {
        await foreach (var ticket in _queue.Reader.ReadAllAsync())
        {
            try
            {
                await RunOneAsync(ticket);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Run {RunId} could not be handled", ticket.View.RunId);
            }
        }
    }

    private async Task RunOneAsync(Ticket ticket)
    {
        var view = ticket.View;
        var pipelineId = view.PipelineId!;
        Deployment? deployment;
        lock (_lock)
        {
            if (!_queued.Contains(ticket)) return;
            deployment = _deployments.GetValueOrDefault(pipelineId);
        }

        // The deployment's gate: held from here until its fragments are re-armed after the run.
        var gated = false;
        try
        {
            if (deployment is null) throw new LabConflictException($"'{pipelineId}' is no longer deployed.");
            await deployment.Gate.WaitAsync(ticket.Cancel.Token);
            gated = true;
            if (deployment.State != "ready") throw new LabConflictException($"'{pipelineId}' is not ready: {deployment.State} {deployment.Message}".Trim());
        }
        catch (Exception ex)
        {
            if (gated) deployment!.Gate.Release();
            lock (_lock) if (!_queued.Contains(ticket)) return;
            var cancelled = ex is OperationCanceledException;
            Finish(ticket, view with { State = cancelled ? "cancelled" : "refused", StartedAt = DateTimeOffset.UtcNow, Refusal = cancelled ? "Cancelled while queued." : ex.Message });
            return;
        }

        var plan = deployment.Plan;
        view = view with { State = "running", StartedAt = DateTimeOffset.UtcNow };
        lock (_lock)
        {
            _queued.Remove(ticket);
            _current = ticket with { View = view };
        }
        _bus.Publish("run", view);
        _bus.Publish("deployments", Deployments);

        var spec = new RunSpec(
            view.RunId,
            plan.Fragments.Select(f => new FragmentPin(f.Name, view.Pins?.GetValueOrDefault(f.Name) is { Length: > 0 } v ? v : null)).ToList(),
            plan.Edges.Select(e => new RunEdge(e.ProducerFragment, e.ProducerAlias, e.ConsumerFragment, e.ConsumerAlias)).ToList());

        RunView final;
        try
        {
            var result = await _orchestrator.RunAsync(spec, ticket.Cancel.Token);
            final = view with { State = result.Outcome.ToString().ToLowerInvariant(), Result = result, Description = result.Describe() };
        }
        catch (Exception ex) when (ex is AdmissionRefusedException or MisalignedInstancesException
                                       or PinnedVersionUnavailableException or InvalidOperationException)
        {
            final = view with { State = "refused", Refusal = ex.Message };
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Run {RunId} failed unexpectedly", spec.RunId);
            final = view with { State = "error", Refusal = ex.Message };
        }

        // A PipelineNode serves one run: every instance is replaced before this pipeline can run
        // again. The gate, still held, is released only once they are registered.
        deployment.State = "rearming";
        Finish(ticket, final);
        _ = Task.Run(() => RearmAsync(deployment));
    }

    private void Finish(Ticket ticket, RunView final)
    {
        final = final with { FinishedAt = DateTimeOffset.UtcNow };
        _journal.Record(final);
        lock (_lock)
        {
            _queued.Remove(ticket);
            if (_current?.View.RunId == final.RunId) _current = null;
        }
        // The token source is left to the collector: a queued run cancelled here may still be
        // waiting on its deployment's gate with that token.
        _bus.Publish("run", final);
        _bus.Publish("deployments", Deployments);
    }

    private async Task RearmAsync(Deployment deployment)
    {
        try
        {
            Publish(deployment);
            var generation = NewGeneration();
            deployment.Generation = generation;
            foreach (var fragment in deployment.Plan.Fragments)
            {
                var connection = _inventory.ConnectionOf(fragment.Node) ?? throw new LabConflictException($"{fragment.Node} is offline.");
                await _labHub.Clients.Client(connection).SendAsync(LabHubMethods.Rearm, fragment.Name, generation);
            }
            await WaitRegisteredAsync(deployment, CancellationToken.None);
            deployment.State = "ready";
            deployment.Message = null;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Re-arming {Pipeline} failed", deployment.Plan.PipelineId);
            deployment.State = "failed";
            deployment.Message = ex.Message;
        }
        finally
        {
            deployment.Gate.Release();
            Publish(deployment);
        }
    }

    /// <summary>
    /// Ready means the main instance reports itself registered in the deployment's current
    /// generation and the coordinator's registry holds it at the fragment's version. A closed
    /// instance has left the registry by then (a node unregisters before it disconnects), so
    /// admission has no stale one to pick.
    /// </summary>
    private async Task WaitRegisteredAsync(Deployment deployment, CancellationToken ct)
    {
        bool Ready(PlannedFragment fragment)
        {
            var main = _inventory.Reported(fragment.Name).FirstOrDefault(s => s.Instance == "main");
            if (main is not { State: FragmentState.Registered, ClientId: { } clientId } || main.Generation != deployment.Generation)
                return false;
            return _registry.GetLiveInstances(fragment.Name).Any(i => i.ClientId == clientId && i.Version == fragment.Version);
        }

        var deadline = DateTime.UtcNow + RegistrationTimeout;
        while (true)
        {
            // A host that refuses or fails to register a fragment says so in this generation; waiting
            // out the timeout would only hide why.
            foreach (var fragment in deployment.Plan.Fragments)
            {
                var failed = _inventory.Reported(fragment.Name)
                    .FirstOrDefault(s => s.Instance == "main" && s.State == FragmentState.Failed && s.Generation == deployment.Generation);
                if (failed is not null)
                    throw new LabConflictException($"{fragment.Name} was not registered: {failed.Message}");
            }
            var missing = deployment.Plan.Fragments.Where(f => !Ready(f)).Select(f => f.Name).ToList();
            if (missing.Count == 0) return;
            if (DateTime.UtcNow > deadline)
                throw new LabConflictException($"Not registered with the coordinator in time: {string.Join(", ", missing)}.");
            await Task.Delay(100, ct);
        }
    }

    private void ThrowIfBusy(string pipelineId)
    {
        if (_current?.View.PipelineId == pipelineId || _queued.Any(t => t.View.PipelineId == pipelineId))
            throw new LabConflictException($"A run of '{pipelineId}' is queued or in flight.");
    }

    private (Deployment Deployment, PlannedFragment Fragment) RequireFragment(string fragmentName)
    {
        lock (_lock)
        {
            foreach (var deployment in _deployments.Values)
                if (deployment.Plan.Fragments.FirstOrDefault(f => f.Name == fragmentName) is { } fragment)
                    return (deployment, fragment);
        }
        throw new LabConflictException($"'{fragmentName}' is not a fragment of a deployed plan.");
    }

    private DeploymentView View(Deployment d) => new(
        d.Plan.PipelineId, d.Plan.Id, d.Origin, d.State, d.DeployedAt, d.Message,
        d.Plan.Fragments.Select(f => new DeployedFragment(f.Name, f.Node, f.Version)).ToList(), d.Plan.Edges.Count,
        _queued.Count(t => t.View.PipelineId == d.Plan.PipelineId),
        _current?.View.PipelineId == d.Plan.PipelineId ? _current.View.RunId : null);

    private void Publish(Deployment deployment)
    {
        _bus.Publish("deploy", new { planId = deployment.Plan.Id, pipelineId = deployment.Plan.PipelineId, state = deployment.State, message = deployment.Message });
        _bus.Publish("deployments", Deployments);
    }

    private static string NewGeneration() => Guid.NewGuid().ToString("N");

    private Task SendAsync(string node, string method, object argument, CancellationToken ct)
    {
        var connection = _inventory.ConnectionOf(node) ?? throw new LabConflictException($"{node} is offline.");
        return _labHub.Clients.Client(connection).SendAsync(method, argument, ct);
    }
}
