using DtPipe.Coordinator.Contracts;

namespace DtPipe.Coordinator.Server;

/// <summary>
/// Fault injection on a deployed fragment, which a host that tests the coordinator may offer through a route of its own: the
/// product's host maps none. A replica ends a child only when its own host enabled it.
/// </summary>
internal sealed class FaultInjection(DeploymentLedger ledger, NodeInventory inventory, INodeLink link)
{
    /// <summary>
    /// Fault injection: asks every replica of the fragment's node to end the child of its run; a replica whose host did not enable it
    /// ends none. How many children were ended in all.
    /// </summary>
    public async Task<int> KillAsync(string fragmentName, CancellationToken ct)
    {
        var node = RequireFragment(fragmentName).Fragment.Node;
        var replicas = inventory.ReplicasOf(node);
        if (replicas.Count == 0) throw new ConflictException($"{node} is offline.");
        var ended = 0;
        foreach (var (key, _) in replicas)
        {
            using var bound = CancellationTokenSource.CreateLinkedTokenSource(ct);
            bound.CancelAfter(TimeSpan.FromSeconds(10));
            ended += await link.KillAsync(key, fragmentName, bound.Token);
        }
        return ended;
    }

    private (Deployment Deployment, PlannedFragment Fragment) RequireFragment(string fragmentName)
    {
        lock (ledger.Lock)
        {
            foreach (var deployment in ledger.Deployments.Values)
                if (deployment.Plan?.Fragments.FirstOrDefault(f => f.Name == fragmentName) is { } fragment)
                    return (deployment, fragment);
        }
        throw new ConflictException($"'{fragmentName}' is not a fragment of a deployed plan.");
    }
}
