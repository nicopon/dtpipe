using System.Runtime.InteropServices;
using DtPipe.Coordinator;
using DtPipe.Coordinator.Tests.Infrastructure;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace DtPipe.PipelineNode.Tests;

/// <summary>
/// J2: a node that takes more than one edge per direction, the join C4's own design motivated but
/// could not run distributed until now (<c>PipelineNode.ValidateEdges</c> refused it outright).
/// The three guards run in the order the plan lays out: plumbing alone (<c>--merge</c>), the real
/// join operator, then the join's own producer dying mid-flow - the "C demande X à A et Y à B, et B
/// tombe" sequence the coordinator's design has described since C4.
/// </summary>
public class JoinTests
{
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(30);

    [DllImport("libc", SetLastError = true)]
    private static extern int kill(int pid, int sig);

    // macOS/Darwin value (SDK sys/signal.h), same pattern as CoordinatorDrivenTests.
    private const int SIGKILL = 9;

    private static async Task<PipelineNode> ConnectNode(
        CoordinatorTestHost host, string jobPath, string fragmentName, params EdgeBinding[] edges) =>
        await PipelineNode.ConnectAsync(new PipelineNodeOptions
        {
            HubUrl = host.Url,
            DtPipeExecutable = DtPipeExecutableLocator.Path,
            FragmentJobPath = jobPath,
            Edges = edges,
        }, fragmentName);

    [Fact]
    public async Task TwoProducersMergedThroughASecondInboundEdge_MatchTheUnsplitWitness()
    {
        using var fixture = MergeFixture.Create(rowCountA: 4_000, rowCountB: 3_000);
        await using var host = await CoordinatorTestHost.StartAsync(
            o => o.Groups["test"] = new TransportR.FlowControl.GroupAccess { CanSendTo = ["test"] });
        var orchestrator = host.Host.Services.GetRequiredService<IRunOrchestrator>();

        await using var a = await ConnectNode(host, fixture.ProducerAJobPath, "A", new EdgeBinding("main", EdgeDirection.Outbound));
        await using var b = await ConnectNode(host, fixture.ProducerBJobPath, "B", new EdgeBinding("main", EdgeDirection.Outbound));
        // "a" first (rides C's real stdin), "b" second (the excess edge - a named pipe).
        await using var c = await ConnectNode(host, fixture.ConsumerJobPath, "C",
            new EdgeBinding("a", EdgeDirection.Inbound), new EdgeBinding("b", EdgeDirection.Inbound));

        var spec = new RunSpec("run-merge", [new FragmentPin("A"), new FragmentPin("B"), new FragmentPin("C")],
            [new RunEdge("A", "main", "C", "a"), new RunEdge("B", "main", "C", "b")]);

        var result = await orchestrator.RunAsync(spec).WaitAsync(Bound);

        Assert.Equal(RunOutcome.Succeeded, result.Outcome);
        Assert.Null(result.Cause);
        Assert.Empty(result.Consequences);

        var witnessRows = File.ReadAllLines(fixture.WitnessCsvPath).Order(StringComparer.Ordinal);
        var splitRows = File.ReadAllLines(fixture.SplitCsvPath).Order(StringComparer.Ordinal);
        Assert.Equal(witnessRows, splitRows);
    }

    [Fact]
    public async Task TwoProducersJoinedOnASharedKeyThroughASecondInboundEdge_MatchTheUnsplitWitness()
    {
        using var fixture = JoinFixture.Create(rowCount: 2_000);
        await using var host = await CoordinatorTestHost.StartAsync(
            o => o.Groups["test"] = new TransportR.FlowControl.GroupAccess { CanSendTo = ["test"] });
        var orchestrator = host.Host.Services.GetRequiredService<IRunOrchestrator>();

        await using var a = await ConnectNode(host, fixture.ProducerOrdersJobPath, "A", new EdgeBinding("main", EdgeDirection.Outbound));
        await using var b = await ConnectNode(host, fixture.ProducerCustomersJobPath, "B", new EdgeBinding("main", EdgeDirection.Outbound));
        await using var c = await ConnectNode(host, fixture.ConsumerJobPath, "C",
            new EdgeBinding("orders", EdgeDirection.Inbound), new EdgeBinding("customers", EdgeDirection.Inbound));

        var spec = new RunSpec("run-join", [new FragmentPin("A"), new FragmentPin("B"), new FragmentPin("C")],
            [new RunEdge("A", "main", "C", "orders"), new RunEdge("B", "main", "C", "customers")]);

        var result = await orchestrator.RunAsync(spec).WaitAsync(Bound);

        Assert.Equal(RunOutcome.Succeeded, result.Outcome);
        Assert.Null(result.Cause);
        Assert.Empty(result.Consequences);

        var witnessRows = File.ReadAllLines(fixture.WitnessCsvPath).Order(StringComparer.Ordinal);
        var splitRows = File.ReadAllLines(fixture.SplitCsvPath).Order(StringComparer.Ordinal);
        Assert.Equal(witnessRows, splitRows);
    }

    /// <summary>
    /// The join's own second producer killed mid-flow - the sequence the coordinator's C4 design
    /// motivated ("C demande X à A et Y à B, et B tombe") but that stayed unrunnable in a distributed
    /// setting until J2. Throttled so the kill lands mid-stream, same margin as
    /// <see cref="CoordinatorDrivenTests.MiddleFragmentDies_RunFailsWithItAsCause_OthersAsConsequences"/>.
    /// </summary>
    [Fact]
    public async Task TheJoinsSecondProducerKilledMidFlow_RunFailsWithItAsCause()
    {
        using var fixture = JoinFixture.Create(rowCount: 1_000_000, "--throttle", "100000");
        await using var host = await CoordinatorTestHost.StartAsync(
            o => o.Groups["test"] = new TransportR.FlowControl.GroupAccess { CanSendTo = ["test"] });
        var orchestrator = host.Host.Services.GetRequiredService<IRunOrchestrator>();

        await using var a = await ConnectNode(host, fixture.ProducerOrdersJobPath, "A", new EdgeBinding("main", EdgeDirection.Outbound));
        await using var b = await ConnectNode(host, fixture.ProducerCustomersJobPath, "B", new EdgeBinding("main", EdgeDirection.Outbound));
        await using var c = await ConnectNode(host, fixture.ConsumerJobPath, "C",
            new EdgeBinding("orders", EdgeDirection.Inbound), new EdgeBinding("customers", EdgeDirection.Inbound));

        var spec = new RunSpec("run-join-kill", [new FragmentPin("A"), new FragmentPin("B"), new FragmentPin("C")],
            [new RunEdge("A", "main", "C", "orders"), new RunEdge("B", "main", "C", "customers")]);

        var runTask = orchestrator.RunAsync(spec);

        var deadline = DateTime.UtcNow + Bound;
        while (!b.IsLaunched)
        {
            if (DateTime.UtcNow > deadline) throw new TimeoutException("B was never launched");
            await Task.Delay(10);
        }
        await Task.Delay(200); // let bytes actually cross both of C's inbound edges before killing B

        var rc = kill(b.ChildProcessId, SIGKILL);
        if (rc != 0) throw new InvalidOperationException($"kill returned {rc}, errno={Marshal.GetLastWin32Error()}");

        var sw = System.Diagnostics.Stopwatch.StartNew();
        var result = await runTask.WaitAsync(Bound);
        sw.Stop();

        Assert.Equal(RunOutcome.Failed, result.Outcome);
        Assert.Equal("B", result.Cause);
        // A joining consumer couples both producers: C's own child aborts the whole DAG once its
        // 'customers' edge breaks, which turns C's still-healthy 'orders' edge into a mismatch too
        // (measured: A sent more than C ever received on it) - so A fails alongside C, exactly as a
        // chain's own upstream neighbour does in CoordinatorDrivenTests.MiddleFragmentDies.
        Assert.Equal(["A", "C"], result.Consequences.Order(StringComparer.Ordinal));
        Assert.True(sw.Elapsed < TimeSpan.FromSeconds(10),
            $"cause attribution took {sw.Elapsed.TotalSeconds:F1}s - should be seconds, not the 60s stall window");
    }
}
