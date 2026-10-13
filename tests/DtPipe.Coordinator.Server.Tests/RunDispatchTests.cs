using DtPipe.Coordinator.Contracts;
using Xunit;

namespace DtPipe.Coordinator.Server.Tests;

public sealed class RunDispatchTests
{
    private static IReadOnlySet<string> Nodes(params string[] nodes) => nodes.ToHashSet(StringComparer.Ordinal);

    private static RunDispatch.Waiting Run(string id, params string[] nodes) => new(id, Nodes(nodes));

    [Fact]
    public void Runs_on_disjoint_data_nodes_all_start()
    {
        var start = RunDispatch.Startable([Nodes("node-1")], [Run("r2", "node-2"), Run("r3", "node-3", "node-4")]);

        Assert.Equal(["r2", "r3"], start);
    }

    [Fact]
    public void A_run_waits_while_a_run_sent_uses_one_of_its_data_nodes()
    {
        var start = RunDispatch.Startable([Nodes("node-1", "node-4")], [Run("r2", "node-2", "node-4")]);

        Assert.Empty(start);
    }

    [Fact]
    public void Two_waiting_runs_sharing_a_node_start_one_at_a_time_oldest_first()
    {
        var start = RunDispatch.Startable([], [Run("r1", "node-4"), Run("r2", "node-4")]);

        Assert.Equal(["r1"], start);
    }

    /// <summary>r2 is free now but waits behind r1, which needs node-2 too: a younger run never overtakes on a shared node.</summary>
    [Fact]
    public void An_older_waiting_run_holds_its_nodes_against_younger_ones()
    {
        var start = RunDispatch.Startable([Nodes("node-1")], [Run("r1", "node-1", "node-2"), Run("r2", "node-2"), Run("r3", "node-3")]);

        Assert.Equal(["r3"], start);
    }

    [Fact]
    public void A_runner_is_never_a_data_node()
    {
        var roles = new Dictionary<string, NodeRole> { ["node-1"] = NodeRole.Data, ["node-2"] = NodeRole.Data, ["runner-1"] = NodeRole.Runner };
        var first = RunDispatch.DataNodes(Plan("node-1", "runner-1"), roles);
        var second = RunDispatch.DataNodes(Plan("node-2", "runner-1"), roles);

        Assert.Equal(Nodes("node-1"), first);
        Assert.Equal(["r2"], RunDispatch.Startable([first], [new RunDispatch.Waiting("r2", second)]));
    }

    [Fact]
    public void A_node_the_inventory_does_not_know_counts_as_a_data_node()
    {
        var roles = new Dictionary<string, NodeRole> { ["node-1"] = NodeRole.Data };

        Assert.Equal(Nodes("node-1", "runner-1"), RunDispatch.DataNodes(Plan("node-1", "runner-1"), roles));
    }

    private static DistributionPlan Plan(params string[] nodes) => new(
        "1", "p", [], [],
        nodes.Select(n => new PlannedFragment($"p@{n}", n, [], "", "v", [], [], FragmentForm.Bricks)).ToList(),
        [], [], [], null);
}
