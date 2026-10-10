using DtPipe.Coordinator;
using DtPipe.Coordinator.Tests.Infrastructure;
using DtPipe.PipelineNode.Tests.Infrastructure;
using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace DtPipe.PipelineNode.Tests;

/// <summary>
/// A replica that says it is leaving is out of the registry when its disposal returns, not when the hub gets round to
/// processing the connection's close: a run placed in between must never pick it. The hub here delays every
/// disconnect, standing in for a coordinator behind a slow network or a busy thread pool, so the window that a fast
/// local hub closes by luck stays open.
/// </summary>
[Collection(ChildExitTimingCollection.Name)]
public class ReplicaReplacementTests
{
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(30);
    private static readonly TimeSpan SlowDisconnect = TimeSpan.FromSeconds(4);

    /// <summary>Delays the hub's own handling of a closed connection, never the client's close.</summary>
    private sealed class SlowDisconnectFilter(TimeSpan delay) : IHubFilter
    {
        public async Task OnDisconnectedAsync(
            HubLifetimeContext context, Exception? exception, Func<HubLifetimeContext, Exception?, Task> next)
        {
            await Task.Delay(delay);
            await next(context, exception);
        }
    }

    private static Task<CoordinatorTestHost> StartHost() =>
        CoordinatorTestHost.StartAsync(
            o => o.Groups["test"] = new TransportR.FlowControl.GroupAccess { CanSendTo = ["test"] },
            services =>
            {
                services.AddSignalR(o => o.AddFilter(new SlowDisconnectFilter(SlowDisconnect)));
                services.AddSingleton(new RunOrchestratorOptions
                {
                    ReadyTimeout = TimeSpan.FromSeconds(3),
                    ExitTimeout = TimeSpan.FromSeconds(3),
                });
            });

    private static Task<TestNode> ConnectNode(
        CoordinatorTestHost host, string jobPath, string fragmentName, params EdgeBinding[] edges) =>
        TestNode.StartAsync(host, fragmentName, jobPath, edges);

    private static RunSpec ChainSpec(string runId, params RunFragment[] fragments) => new(
        runId,
        Fragments: fragments,
        Edges:
        [
            new RunEdge("A", "out", "B", "in"),
            new RunEdge("B", "out", "C", "in"),
        ]);

    [Fact]
    public async Task ADisposedReplica_IsNoLongerPlaceable_WhenItsDisposalReturns()
    {
        using var fixture = ChainFixture.Create(rowCount: 1_000);
        await using var host = await StartHost();
        var registry = host.Host.Services.GetRequiredService<ReplicaRegistry>();

        var a = await ConnectNode(host, fixture.FragmentAJobPath, "A", new EdgeBinding("out", EdgeDirection.Outbound));
        Assert.Single(registry.Replicas("A"));

        await a.DisposeAsync();

        Assert.Empty(registry.Replicas("A"));
    }

    /// <summary>
    /// The sequence a client runs when it replaces a replica: dispose the old one, start its replacement (same node), start
    /// a run at once. Placement must go to the replacement.
    /// </summary>
    [Fact]
    public async Task ARunRightAfterAReplacement_UsesTheReplacement_NeverTheClosedReplica()
    {
        using var fixture = ChainFixture.Create(rowCount: 1_000);
        await using var host = await StartHost();
        var orchestrator = host.Host.Services.GetRequiredService<IRunOrchestrator>();

        var closed = await ConnectNode(host, fixture.FragmentAJobPath, "A", new EdgeBinding("out", EdgeDirection.Outbound));
        await using var b = await ConnectNode(host, fixture.FragmentBJobPath, "B",
            new EdgeBinding("in", EdgeDirection.Inbound), new EdgeBinding("out", EdgeDirection.Outbound));
        await using var c = await ConnectNode(host, fixture.FragmentCJobPath, "C", new EdgeBinding("in", EdgeDirection.Inbound));

        await closed.DisposeAsync();
        await using var replacement = await ConnectNode(host, fixture.FragmentAJobPath, "A", new EdgeBinding("out", EdgeDirection.Outbound));

        var result = await orchestrator.RunAsync(ChainSpec("run-after-replacement", replacement.Fragment, b.Fragment, c.Fragment)).WaitAsync(Bound);

        Assert.Equal(RunOutcome.Succeeded, result.Outcome);
        Assert.Equal(replacement.Key.Short, result.Replicas!["A"].Id);
        Assert.False(closed.IsLaunched);
    }

    /// <summary>
    /// A replica disposed the moment it completes (its <c>Exited</c> just sent, its peers still draining)
    /// is a fragment that finished, not one that was lost: the run must still succeed.
    /// </summary>
    [Fact]
    public async Task AReplicaDisposedAsSoonAsItCompletes_NeverAbortsTheRunItFinished()
    {
        using var fixture = ChainFixture.Create(rowCount: 50_000);
        await using var host = await StartHost();
        var orchestrator = host.Host.Services.GetRequiredService<IRunOrchestrator>();

        var a = await ConnectNode(host, fixture.FragmentAJobPath, "A", new EdgeBinding("out", EdgeDirection.Outbound));
        await using var b = await ConnectNode(host, fixture.FragmentBJobPath, "B",
            new EdgeBinding("in", EdgeDirection.Inbound), new EdgeBinding("out", EdgeDirection.Outbound));
        await using var c = await ConnectNode(host, fixture.FragmentCJobPath, "C", new EdgeBinding("in", EdgeDirection.Inbound));

        var run = orchestrator.RunAsync(ChainSpec("run-dispose-on-completion", a.Fragment, b.Fragment, c.Fragment));
        _ = a.Completion.ContinueWith(_ => a.DisposeAsync().AsTask(), TaskScheduler.Default).Unwrap();
        var result = await run.WaitAsync(Bound);

        Assert.Equal(RunOutcome.Succeeded, result.Outcome);
        Assert.Null(result.Cause);
    }
}
