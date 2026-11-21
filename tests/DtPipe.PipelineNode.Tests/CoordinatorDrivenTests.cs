using System.Runtime.InteropServices;
using DtPipe.Coordinator;
using DtPipe.Coordinator.Tests.Infrastructure;
using DtPipe.PipelineNode.Tests.Infrastructure;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace DtPipe.PipelineNode.Tests;

/// <summary>
/// The barrier: a <see cref="RunOrchestrator"/> admits three registered fragments, launches them,
/// wires each declared edge once both endpoints report ready, and applies the outcome rule to what
/// they report back - exercised here against real replicas (<see cref="TestNode"/>) and real
/// <c>dtpipe</c> children, not fakes. <c>ReplicaAdmissionSubjectTests</c> covers admission alone.
/// </summary>
[Collection(ChildExitTimingCollection.Name)]
public class CoordinatorDrivenTests
{
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(30);

    [DllImport("libc", SetLastError = true)]
    private static extern int kill(int pid, int sig);

    // macOS/Darwin value (SDK sys/signal.h), same pattern as MemoryCeilingTests.
    private const int SIGKILL = 9;

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

    /// <summary>
    /// The barrier: with C never connected, <see cref="RunOrchestrator.RunAsync"/> refuses the run
    /// naming it, and neither A nor B - both announced - was launched while waiting.
    /// </summary>
    [Fact]
    public async Task RunAsync_WithAFragmentNeverRegistered_RefusesWithoutLaunchingTheOthers()
    {
        using var fixture = ChainFixture.Create(rowCount: 1_000);
        await using var host = await CoordinatorTestHost.StartAsync(
            o => o.Groups["test"] = new TransportR.Hub.GroupAccess { CanSendTo = ["test"] });
        var orchestrator = host.Host.Services.GetRequiredService<IRunOrchestrator>();

        await using var a = await ConnectNode(host, fixture.FragmentAJobPath, "A", new EdgeBinding("out", EdgeDirection.Outbound));
        await using var b = await ConnectNode(host, fixture.FragmentBJobPath, "B",
            new EdgeBinding("in", EdgeDirection.Inbound), new EdgeBinding("out", EdgeDirection.Outbound));
        // C is never connected.

        var ex = await Assert.ThrowsAsync<NoEligibleReplicaException>(
            () => orchestrator.RunAsync(ChainSpec("run-refused", a.Fragment, b.Fragment,
                TestNode.FragmentOf("C", fixture.FragmentCJobPath, new EdgeBinding("in", EdgeDirection.Inbound)))));

        Assert.Contains("C", ex.Reasons.Keys);
        Assert.False(a.IsLaunched);
        Assert.False(b.IsLaunched);
    }

    [Fact]
    public async Task HappyPath_ChainOfThree_CoordinatorDrivesTheWholeRun()
    {
        using var fixture = ChainFixture.Create(rowCount: 50_000);
        await using var host = await CoordinatorTestHost.StartAsync(
            o => o.Groups["test"] = new TransportR.Hub.GroupAccess { CanSendTo = ["test"] });
        var orchestrator = host.Host.Services.GetRequiredService<IRunOrchestrator>();

        await using var a = await ConnectNode(host, fixture.FragmentAJobPath, "A", new EdgeBinding("out", EdgeDirection.Outbound));
        await using var b = await ConnectNode(host, fixture.FragmentBJobPath, "B",
            new EdgeBinding("in", EdgeDirection.Inbound), new EdgeBinding("out", EdgeDirection.Outbound));
        await using var c = await ConnectNode(host, fixture.FragmentCJobPath, "C", new EdgeBinding("in", EdgeDirection.Inbound));

        var result = await orchestrator.RunAsync(ChainSpec("run-happy", a.Fragment, b.Fragment, c.Fragment)).WaitAsync(Bound);

        Assert.Equal(RunOutcome.Succeeded, result.Outcome);
        Assert.Null(result.Cause);
        Assert.Empty(result.Consequences);

        var witnessRows = File.ReadAllLines(fixture.WitnessCsvPath).Order(StringComparer.Ordinal);
        var splitRows = File.ReadAllLines(fixture.SplitCsvPath).Order(StringComparer.Ordinal);
        Assert.Equal(witnessRows, splitRows);
    }

    /// <summary>
    /// The run's result says, for each end of each edge, what that end's own client applied to the transfer. The test nodes announce
    /// nothing of the data plane, so the words in the result can be neither the announced level nor a setting: only what each client reports.
    /// </summary>
    [Theory]
    [InlineData(false, "none")]
    [InlineData(true, "encrypted-psk")]
    public async Task TheResult_NamesTheProtectionEachEndOfEachEdgeApplied(bool sealedWithAKey, string word)
    {
        using var fixture = ChainFixture.Create(rowCount: 5_000);
        await using var host = await CoordinatorTestHost.StartAsync(
            o => o.Groups["test"] = new TransportR.Hub.GroupAccess { CanSendTo = ["test"] });
        var orchestrator = host.Host.Services.GetRequiredService<IRunOrchestrator>();
        var key = System.Security.Cryptography.RandomNumberGenerator.GetBytes(32);
        Task<TestNode> Connect(string job, string name, params EdgeBinding[] edges) => TestNode.StartAsync(
            host, name, job, edges, client: new NodeClientOptions
            {
                HubUrl = host.Url,
                EndToEndEncryption = sealedWithAKey,
                EndToEndPreSharedKey = sealedWithAKey ? _ => key : null,
            });

        await using var a = await Connect(fixture.FragmentAJobPath, "A", new EdgeBinding("out", EdgeDirection.Outbound));
        await using var b = await Connect(fixture.FragmentBJobPath, "B",
            new EdgeBinding("in", EdgeDirection.Inbound), new EdgeBinding("out", EdgeDirection.Outbound));
        await using var c = await Connect(fixture.FragmentCJobPath, "C", new EdgeBinding("in", EdgeDirection.Inbound));

        var result = await orchestrator.RunAsync(ChainSpec("run-protection", a.Fragment, b.Fragment, c.Fragment)).WaitAsync(Bound);

        Assert.Equal(RunOutcome.Succeeded, result.Outcome);
        Assert.Equal(2, result.EdgeCounts.Count);
        Assert.All(result.EdgeCounts, edge => Assert.Equal((word, word), (edge.ProducerProtection, edge.ConsumerProtection)));
    }

    /// <summary>
    /// A chain whose middle seals its transfers while its two ends send in clear, which admission would have refused: these replicas
    /// announce nothing of the data plane, so they all read alike, while their clients apply what each was built with. The run fails,
    /// in seconds, with the first edge whose ends failed the encryption as its cause and every fragment a consequence, never a healthy
    /// fragment named for having seen the failure first.
    /// </summary>
    [Fact]
    public async Task AChainWhoseMiddleAloneSeals_FailsWithTheFirstEdgeAsCause_AndNoFragment()
    {
        using var fixture = ChainFixture.Create(rowCount: 5_000);
        await using var host = await CoordinatorTestHost.StartAsync(
            o => o.Groups["test"] = new TransportR.Hub.GroupAccess { CanSendTo = ["test"] });
        var orchestrator = host.Host.Services.GetRequiredService<IRunOrchestrator>();
        var key = System.Security.Cryptography.RandomNumberGenerator.GetBytes(32);
        Task<TestNode> Connect(string job, string name, bool seals, params EdgeBinding[] edges) => TestNode.StartAsync(
            host, name, job, edges, client: new NodeClientOptions
            {
                HubUrl = host.Url,
                EndToEndEncryption = seals,
                EndToEndPreSharedKey = seals ? _ => key : null,
            });

        await using var a = await Connect(fixture.FragmentAJobPath, "A", false, new EdgeBinding("out", EdgeDirection.Outbound));
        await using var b = await Connect(fixture.FragmentBJobPath, "B", true,
            new EdgeBinding("in", EdgeDirection.Inbound), new EdgeBinding("out", EdgeDirection.Outbound));
        await using var c = await Connect(fixture.FragmentCJobPath, "C", false, new EdgeBinding("in", EdgeDirection.Inbound));

        var sw = System.Diagnostics.Stopwatch.StartNew();
        var result = await orchestrator.RunAsync(ChainSpec("run-sealed-apart", a.Fragment, b.Fragment, c.Fragment)).WaitAsync(Bound);
        sw.Stop();

        Assert.Equal(RunOutcome.Failed, result.Outcome);
        Assert.Equal(("A -> B", CauseKinds.Edge, false), (result.Cause, result.CauseKind, result.CauseIsUnresponsive));
        Assert.Equal(["A", "B", "C"], result.Consequences.Order(StringComparer.Ordinal));
        Assert.Equal(Coordinator.Contracts.TransferFailures.EndToEndEncryptionFailed, result.Reports["B"].EdgeFailures!["in"]);
        Assert.True(sw.Elapsed < TimeSpan.FromSeconds(10), $"the verdict took {sw.Elapsed.TotalSeconds:F1}s");
    }

    /// <summary>
    /// A three-fragment chain, the middle fragment's child killed mid-flow (a join is not runnable
    /// yet - see <see cref="ChainFixture"/>): the run is reported failed, cause B, in seconds rather
    /// than the minute-plus a producer waiting out its own send window would otherwise take, and not
    /// by arrival order - a peer aborting its own transfer can fail a healthy fragment before the
    /// fragment that actually died has finished reporting its own <c>Exited</c>.
    /// </summary>
    [Fact]
    public async Task MiddleFragmentDies_RunFailsWithItAsCause_OthersAsConsequences()
    {
        // Throttled: A's own SendAsync/CompleteAsync can succeed for rows B's relay never actually
        // reads (TransportR acknowledges into B's receive buffer, not on B's own consumption of it),
        // so an unthrottled A can look done well before B - still alive, still relaying - has caught
        // up to what it was sent. At 100,000 rows/s, 1M rows take 10s: a wide margin against the
        // 200ms delay before killing B, so A is still mid-stream when the kill lands.
        using var fixture = ChainFixture.Create(rowCount: 1_000_000, "--throttle", "100000");
        await using var host = await CoordinatorTestHost.StartAsync(
            o => o.Groups["test"] = new TransportR.Hub.GroupAccess { CanSendTo = ["test"] });
        var orchestrator = host.Host.Services.GetRequiredService<IRunOrchestrator>();

        await using var a = await ConnectNode(host, fixture.FragmentAJobPath, "A", new EdgeBinding("out", EdgeDirection.Outbound));
        await using var b = await ConnectNode(host, fixture.FragmentBJobPath, "B",
            new EdgeBinding("in", EdgeDirection.Inbound), new EdgeBinding("out", EdgeDirection.Outbound));
        await using var c = await ConnectNode(host, fixture.FragmentCJobPath, "C", new EdgeBinding("in", EdgeDirection.Inbound));

        var runTask = orchestrator.RunAsync(ChainSpec("run-cause", a.Fragment, b.Fragment, c.Fragment));

        var deadline = DateTime.UtcNow + Bound;
        while (!b.IsLaunched)
        {
            if (DateTime.UtcNow > deadline) throw new TimeoutException("B was never launched");
            await Task.Delay(10);
        }
        await Task.Delay(200); // let bytes actually cross both of B's edges before killing it
        var rc = kill(b.ChildProcessId, SIGKILL);
        if (rc != 0) throw new InvalidOperationException($"kill returned {rc}, errno={Marshal.GetLastWin32Error()}");

        var sw = System.Diagnostics.Stopwatch.StartNew();
        var result = await runTask.WaitAsync(Bound);
        sw.Stop();

        Assert.Equal(RunOutcome.Failed, result.Outcome);
        Assert.Equal("B", result.Cause);
        Assert.Equal(["A", "C"], result.Consequences.Order(StringComparer.Ordinal));
        Assert.True(sw.Elapsed < TimeSpan.FromSeconds(10),
            $"cause attribution took {sw.Elapsed.TotalSeconds:F1}s - should be seconds, not the 60s stall window");
        // Fully qualified: DtPipe.PipelineNode.FaultOrigin is also in scope (the enclosing
        // namespace), colliding with the coordinator's own wire-side enum of the same name.
        Assert.Equal(Coordinator.FaultOrigin.Local, result.Reports["B"].Origin);
        Assert.Equal(Coordinator.FaultOrigin.Remote, result.Reports["A"].Origin);
        Assert.Equal(Coordinator.FaultOrigin.Remote, result.Reports["C"].Origin);
    }
}
