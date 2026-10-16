using DtPipe.Coordinator.Contracts;

namespace DtPipe.Coordinator.Server;

/// <summary>
/// Pure. Which waiting runs start now: one run at a time per data node, first come first served.
/// A run waits while a run already sent, or an older run still waiting, uses one of its data nodes.
/// A runner never counts: it hosts no data, and each fragment is its own child process.
/// </summary>
public static class RunDispatch
{
    public sealed record Waiting(string RunId, IReadOnlySet<string> DataNodes);

    /// <summary>The runs of <paramref name="waiting"/>, oldest first, that start now, in that order.</summary>
    public static IReadOnlyList<string> Startable(IEnumerable<IReadOnlySet<string>> sent, IReadOnlyList<Waiting> waiting)
    {
        var held = new HashSet<string>(StringComparer.Ordinal);
        foreach (var nodes in sent) held.UnionWith(nodes);

        var start = new List<string>();
        foreach (var run in waiting)
        {
            if (!run.DataNodes.Overlaps(held)) start.Add(run.RunId);
            // A run that waits still holds its nodes against younger ones: a stream of runs on
            // one node never starves an older run that needs it with another.
            held.UnionWith(run.DataNodes);
        }
        return start;
    }

    /// <summary>
    /// The nodes of a plan's fragments, runners excepted. A node the inventory does not know is a
    /// data node: an offline runner then serialises runs, never lets two share a data node.
    /// </summary>
    public static IReadOnlySet<string> DataNodes(DistributionPlan plan, IReadOnlyDictionary<string, NodeRole> roles) =>
        DataNodes(plan.Fragments.Select(f => f.Node), roles);

    /// <summary>The data nodes among <paramref name="nodes"/>: what an armed deployment's bindings name.</summary>
    public static IReadOnlySet<string> DataNodes(IEnumerable<string> nodes, IReadOnlyDictionary<string, NodeRole> roles) =>
        nodes
            .Where(node => !roles.TryGetValue(node, out var role) || role != NodeRole.Runner)
            .ToHashSet(StringComparer.Ordinal);

    /// <summary>
    /// The data nodes of <paramref name="plan"/> that the run does not hold, empty when it holds them all. A run holds its data
    /// nodes from the bindings of its deployment, before its plan exists, and a plan that reaches another data node would run
    /// beside a run it was never ordered after: it is refused.
    /// </summary>
    public static IReadOnlyList<string> Unheld(DistributionPlan plan, IReadOnlySet<string> held, IReadOnlyDictionary<string, NodeRole> roles) =>
        DataNodes(plan, roles).Where(node => !held.Contains(node)).Order(StringComparer.Ordinal).ToList();
}
