using DtPipe.Coordinator.Contracts;

namespace DtPipe.Coordinator.Server;

/// <summary>What only a test bench asks of a deployment: fault injection on the fragments it deployed. Called by the routes of lab mode.</summary>
internal sealed class LabFaults(DeploymentLedger ledger, NodeInventory inventory, INodeLink link)
{
    /// <summary>Fault injection: asks every replica of the fragment's node to end the child of its run; a replica whose host did not enable it ends none.</summary>
    public async Task KillAsync(string fragmentName, CancellationToken ct)
    {
        var node = RequireFragment(fragmentName).Fragment.Node;
        var replicas = inventory.ReplicasOf(node);
        if (replicas.Count == 0) throw new ConflictException($"{node} is offline.");
        foreach (var (key, _) in replicas)
        {
            using var bound = CancellationTokenSource.CreateLinkedTokenSource(ct);
            bound.CancelAfter(TimeSpan.FromSeconds(10));
            await link.KillAsync(key, fragmentName, bound.Token);
        }
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
