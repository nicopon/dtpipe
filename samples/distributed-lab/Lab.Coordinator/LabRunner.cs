using DtPipe.Coordinator;
using DtPipe.Lab.Contracts;
using Microsoft.AspNetCore.SignalR;

namespace DtPipe.Lab.Coordinator;

/// <summary>A request the lab refuses in its current state (HTTP 409).</summary>
public sealed class LabConflictException(string message) : Exception(message);

public sealed record RunView(
    string RunId, string PlanId, string State, DateTimeOffset StartedAt, DateTimeOffset? FinishedAt,
    IReadOnlyDictionary<string, string>? Pins, RunResult? Result, string? Description, string? Refusal);

/// <summary>
/// Deploys a plan's fragments on their nodes and drives runs through the coordinator's own
/// <see cref="IRunOrchestrator"/>, which admits, launches, wires and judges; this class only asks.
/// One plan is deployed at a time, and one run is in flight at a time, as the orchestrator allows.
/// </summary>
public sealed class LabRunner(
    NodeInventory inventory, IHubContext<LabHub> labHub, IRunOrchestrator orchestrator, INodeRegistry registry,
    EventBus bus, ILogger<LabRunner> logger)
{
    private static readonly TimeSpan RegistrationTimeout = TimeSpan.FromSeconds(30);

    private readonly object _lock = new();
    private readonly SemaphoreSlim _deployGate = new(1, 1);
    private readonly List<RunView> _history = [];
    private LabPlan? _active;
    private string _generation = "";
    private RunView? _current;
    private CancellationTokenSource? _runCts;
    private int _runSeq;

    public LabPlan? ActivePlan { get { lock (_lock) return _active; } }
    public RunView? CurrentRun { get { lock (_lock) return _current; } }
    public IReadOnlyList<RunView> History { get { lock (_lock) return _history.ToList(); } }

    private bool Running => _current?.State == "running";

    public async Task DeployAsync(LabPlan plan, CancellationToken ct)
    {
        if (!plan.Deployable)
            throw new LabConflictException("This plan cannot be deployed: " +
                string.Join(" ", plan.Errors.Append(plan.FlowRejection ?? "").Where(s => s.Length > 0)));
        lock (_lock)
        {
            if (Running) throw new LabConflictException("A run is in flight.");
        }

        await _deployGate.WaitAsync(ct);
        try
        {
            var offline = plan.Fragments.Where(f => inventory.ConnectionOf(f.Node) is null).Select(f => f.Node).ToList();
            if (offline.Count > 0) throw new LabConflictException($"Offline: {string.Join(", ", offline)}.");

            bus.Publish("deploy", new { planId = plan.Id, state = "deploying" });
            var generation = NewGeneration();
            foreach (var node in inventory.Snapshot().Where(n => n.Online))
                await SendAsync(node.Name, LabHubMethods.Undeploy, "*", ct);
            foreach (var fragment in plan.Fragments)
                await SendAsync(fragment.Node, LabHubMethods.Deploy,
                    new FragmentDeployment(fragment.Name, fragment.Yaml, fragment.Edges, generation), ct);

            lock (_lock) (_active, _generation) = (plan, generation);
            await WaitRegisteredAsync(plan, ct);
            bus.Publish("deploy", new { planId = plan.Id, state = "ready" });
        }
        finally
        {
            _deployGate.Release();
        }
    }

    /// <summary>
    /// A second instance of one fragment on the same node, from a job that differs by a comment:
    /// same behaviour, different version. Admission then refuses the run as misaligned.
    /// </summary>
    public async Task DeployVariantAsync(string fragmentName, CancellationToken ct)
    {
        lock (_lock)
        {
            if (Running) throw new LabConflictException("A run is in flight.");
        }
        if (_deployGate.CurrentCount == 0) throw new LabConflictException("Fragments are being deployed.");
        var fragment = RequireFragment(fragmentName);
        var yaml = fragment.Yaml + $"# variant {Guid.NewGuid():N}\n";
        await SendAsync(fragment.Node, LabHubMethods.Deploy,
            new FragmentDeployment(fragment.Name, yaml, fragment.Edges, NewGeneration(), Instance: "variant"), ct);
        bus.Publish("deploy", new { planId = ActivePlan?.Id, state = "variant", fragment = fragment.Name });
    }

    public RunView StartRun(IReadOnlyDictionary<string, string>? pins)
    {
        LabPlan plan;
        RunView view;
        CancellationTokenSource cts;
        lock (_lock)
        {
            plan = _active ?? throw new LabConflictException("Deploy a plan first.");
            if (Running) throw new LabConflictException("A run is in flight.");
            if (_deployGate.CurrentCount == 0) throw new LabConflictException("Fragments are being deployed.");

            view = new RunView($"run-{++_runSeq}", plan.Id, "running", DateTimeOffset.UtcNow, null, pins, null, null, null);
            _current = view;
            _runCts = cts = new CancellationTokenSource();
        }

        var spec = new RunSpec(
            view.RunId,
            plan.Fragments.Select(f => new FragmentPin(f.Name, pins?.GetValueOrDefault(f.Name) is { Length: > 0 } v ? v : null)).ToList(),
            plan.Edges.Select(e => new RunEdge(e.ProducerFragment, e.ProducerAlias, e.ConsumerFragment, e.ConsumerAlias)).ToList());

        bus.Publish("run", view);
        _ = Task.Run(() => ExecuteAsync(plan, spec, view, cts));
        return view;
    }

    public void Cancel()
    {
        lock (_lock) _runCts?.Cancel();
    }

    public Task KillAsync(string fragmentName, CancellationToken ct) =>
        SendAsync(RequireFragment(fragmentName).Node, LabHubMethods.Kill, fragmentName, ct);

    private async Task ExecuteAsync(LabPlan plan, RunSpec spec, RunView view, CancellationTokenSource cts)
    {
        RunView final;
        try
        {
            var result = await orchestrator.RunAsync(spec, cts.Token);
            final = view with { State = result.Outcome.ToString().ToLowerInvariant(), Result = result, Description = result.Describe() };
        }
        catch (Exception ex) when (ex is AdmissionRefusedException or MisalignedInstancesException
                                       or PinnedVersionUnavailableException or InvalidOperationException)
        {
            final = view with { State = "refused", Refusal = ex.Message };
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Run {RunId} failed unexpectedly", spec.RunId);
            final = view with { State = "error", Refusal = ex.Message };
        }

        // A PipelineNode serves one run: every instance is replaced before the plan can run again.
        // The gate is taken before the run is reported finished, so no run starts in between.
        await _deployGate.WaitAsync();
        final = final with { FinishedAt = DateTimeOffset.UtcNow };
        lock (_lock)
        {
            _current = final;
            _history.Insert(0, final);
            if (_history.Count > 20) _history.RemoveAt(_history.Count - 1);
            _runCts = null;
        }
        cts.Dispose();
        bus.Publish("run", final);

        try
        {
            try
            {
                bus.Publish("deploy", new { planId = plan.Id, state = "rearming" });
                var generation = NewGeneration();
                lock (_lock) _generation = generation;
                foreach (var fragment in plan.Fragments)
                    await SendRearmAsync(fragment.Node, fragment.Name, generation);
                await WaitRegisteredAsync(plan, CancellationToken.None);
                bus.Publish("deploy", new { planId = plan.Id, state = "ready" });
            }
            finally
            {
                _deployGate.Release();
            }
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Re-arming plan {PlanId} failed", plan.Id);
            bus.Publish("deploy", new { planId = plan.Id, state = "failed", message = ex.Message });
        }
    }

    /// <summary>
    /// Ready means the main instance reports itself registered in the current generation, and every
    /// live instance the registry holds for the fragment is one a node host reports now. An
    /// instance just closed for a redeploy or a re-arm stays live until the hub sees its
    /// disconnect, with the same version as its replacement; admission may pick it, and a Launch
    /// sent to it is never answered.
    /// </summary>
    private async Task WaitRegisteredAsync(LabPlan plan, CancellationToken ct)
    {
        bool Ready(PlannedFragment fragment)
        {
            var reported = inventory.Reported(fragment.Name);
            var main = reported.FirstOrDefault(s => s.Instance == "main");
            if (main is not { State: FragmentState.Registered, ClientId: { } clientId } || main.Generation != _generation)
                return false;
            var live = registry.GetLiveInstances(fragment.Name);
            var known = reported.Where(s => s.ClientId is not null).Select(s => s.ClientId!.Value).ToHashSet();
            return live.Any(i => i.ClientId == clientId && i.Version == fragment.Version) && live.All(i => known.Contains(i.ClientId));
        }

        var deadline = DateTime.UtcNow + RegistrationTimeout;
        while (true)
        {
            var missing = plan.Fragments.Where(f => !Ready(f)).Select(f => f.Name).ToList();
            if (missing.Count == 0) return;
            if (DateTime.UtcNow > deadline)
                throw new LabConflictException($"Not registered with the coordinator in time: {string.Join(", ", missing)}.");
            await Task.Delay(100, ct);
        }
    }

    private PlannedFragment RequireFragment(string fragmentName) =>
        ActivePlan?.Fragments.FirstOrDefault(f => f.Name == fragmentName)
            ?? throw new LabConflictException($"'{fragmentName}' is not a fragment of the deployed plan.");

    private static string NewGeneration() => Guid.NewGuid().ToString("N");

    private Task SendRearmAsync(string node, string fragment, string generation)
    {
        var connection = inventory.ConnectionOf(node) ?? throw new LabConflictException($"{node} is offline.");
        return labHub.Clients.Client(connection).SendAsync(LabHubMethods.Rearm, fragment, generation);
    }

    private Task SendAsync(string node, string method, object argument, CancellationToken ct)
    {
        var connection = inventory.ConnectionOf(node) ?? throw new LabConflictException($"{node} is offline.");
        return labHub.Clients.Client(connection).SendAsync(method, argument, ct);
    }
}
