using DtPipe.Coordinator;
using DtPipe.Coordinator.Tests.Infrastructure;
using Microsoft.AspNetCore.SignalR.Client;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace DtPipe.PipelineNode.Tests;

/// <summary>
/// The barrier's abort path: a run the requester cancels, a fragment that never becomes ready, and a
/// fragment that vanishes outright (its whole process, not just its child) rather than reporting a
/// fault of its own. <see cref="CoordinatorDrivenTests"/> covers the mid-flow child death that this
/// file does not - C0bis's <c>Abort</c> already suffices there.
/// </summary>
public class CoordinatorAbortTests
{
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(30);

    private static async Task<PipelineNode> ConnectNode(
        CoordinatorTestHost host, string jobPath, string fragmentName, params EdgeBinding[] edges) =>
        await PipelineNode.ConnectAsync(new PipelineNodeOptions
        {
            HubUrl = host.Url,
            DtPipeExecutable = DtPipeExecutableLocator.Path,
            FragmentJobPath = jobPath,
            Edges = edges,
        }, fragmentName);

    private static RunSpec ChainSpec(string runId) => new(
        runId,
        Fragments: ["A", "B", "C"],
        Edges:
        [
            new RunEdge("A", "out", "B", "in"),
            new RunEdge("B", "out", "C", "in"),
        ]);

    /// <summary>
    /// The requester's own cancellation firing during admission - before any fragment is even
    /// launched - is deterministic to reproduce (no real-process timing to race against) and
    /// exercises the same path <see cref="AdmissionGateTests.ARequesterCancellation_ThrowsOperationCanceled_NeverAdmissionRefused"/>
    /// covers in isolation: it must report <see cref="RunOutcome.Cancelled"/>, never
    /// <see cref="AdmissionRefusedException"/>, and never launch anything.
    /// </summary>
    [Fact]
    public async Task RequesterCancelsDuringAdmission_ReportsCancelled_LaunchesNothing()
    {
        using var fixture = ChainFixture.Create(rowCount: 1_000);
        await using var host = await CoordinatorTestHost.StartAsync(
            o => o.Groups["test"] = new TransportR.FlowControl.GroupAccess { CanSendTo = ["test"] });
        var orchestrator = host.Host.Services.GetRequiredService<IRunOrchestrator>();

        await using var a = await ConnectNode(host, fixture.FragmentAJobPath, "A", new EdgeBinding("out", EdgeDirection.Outbound));
        await using var b = await ConnectNode(host, fixture.FragmentBJobPath, "B",
            new EdgeBinding("in", EdgeDirection.Inbound), new EdgeBinding("out", EdgeDirection.Outbound));
        // C is never connected, so admission polls rather than resolving on its first check.

        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(50));
        var result = await orchestrator.RunAsync(ChainSpec("run-requester-cancel"), cts.Token).WaitAsync(Bound);

        Assert.Equal(RunOutcome.Cancelled, result.Outcome);
        Assert.Null(result.Cause);
        Assert.False(a.IsLaunched);
        Assert.False(b.IsLaunched);
    }

    /// <summary>
    /// C registers (passes admission) but never handles the coordinator's own <c>Launch</c> push - a
    /// deterministic stand-in for a fragment that stops responding right after admission, without
    /// racing real child-process startup. The <c>ReadyTimeout</c> must abort the run naming C rather
    /// than hang on it, and A and B - launched, neither ever wired - must each receive <c>Cancel</c>
    /// and reach their own end rather than sit on a child blocked on stdio nobody will ever service.
    /// </summary>
    [Fact]
    public async Task AFragmentThatNeverBecomesReady_AbortsTheRun_AndTellsLaunchedSurvivorsToCancel()
    {
        using var fixture = ChainFixture.Create(rowCount: 1_000);
        await using var host = await CoordinatorTestHost.StartAsync(
            o => o.Groups["test"] = new TransportR.FlowControl.GroupAccess { CanSendTo = ["test"] },
            services => services.AddSingleton(new RunOrchestratorOptions
            {
                ReadyTimeout = TimeSpan.FromSeconds(3),
                ExitTimeout = TimeSpan.FromSeconds(3),
            }));
        var orchestrator = host.Host.Services.GetRequiredService<IRunOrchestrator>();

        await using var a = await ConnectNode(host, fixture.FragmentAJobPath, "A", new EdgeBinding("out", EdgeDirection.Outbound));
        await using var b = await ConnectNode(host, fixture.FragmentBJobPath, "B",
            new EdgeBinding("in", EdgeDirection.Inbound), new EdgeBinding("out", EdgeDirection.Outbound));

        await using var c = host.CreateClient();
        await c.ConnectAsync();
        await c.ControlConnection.InvokeAsync("Register", "C");

        var result = await orchestrator.RunAsync(ChainSpec("run-ready-timeout")).WaitAsync(Bound);

        Assert.Equal(RunOutcome.Failed, result.Outcome);
        Assert.Equal("C", result.Cause);
        Assert.True(result.CauseIsUnresponsive);

        await Task.WhenAll(a.Completion, b.Completion).WaitAsync(Bound);
    }

    /// <summary>
    /// A fragment's whole process vanishing (simulated by disposing it outright, not just killing its
    /// child) once it is fully wired and actively relaying is the pair muet case the disconnect grace
    /// period exists for: no <c>Exited</c>, no <c>Fault</c>, nothing at all from that side. Modelled on
    /// <see cref="CoordinatorDrivenTests.MiddleFragmentDies_RunFailsWithItAsCause_OthersAsConsequences"/> -
    /// throttled so the kill lands mid-stream, not before either edge has carried anything.
    /// </summary>
    /// <remarks>
    /// Proves the run resolves quickly and correctly, naming B, and that A and C do not hang - it does
    /// <b>not</b> isolate <c>ITransferTerminator.TerminateAsync</c> as the reason: disposing
    /// <see cref="PipelineNode"/> in-process closes its own data-plane connections cleanly, which
    /// resolves A and C on its own (confirmed by temporarily removing the <c>TerminateAsync</c> call -
    /// the run still passed). An abruptly unresponsive peer - a network partition, a process that hangs
    /// rather than exits - is the case <c>TerminateAsync</c> exists for, and is not reproducible this
    /// way: <see cref="PipelineNode"/> runs in-process here, so there is no separate process left to
    /// kill without also going through its own clean disposal path.
    /// </remarks>
    [Fact]
    public async Task ANodeThatVanishesMidFlow_FailsTheRun_NamingIt_QuicklyAndSurvivorsDoNotHang()
    {
        using var fixture = ChainFixture.Create(rowCount: 1_000_000, "--throttle", "100000");
        await using var host = await CoordinatorTestHost.StartAsync(
            o => o.Groups["test"] = new TransportR.FlowControl.GroupAccess { CanSendTo = ["test"] },
            services => services.AddSingleton(new NodeRegistryOptions { DisconnectGracePeriod = TimeSpan.FromMilliseconds(300) }));
        var orchestrator = host.Host.Services.GetRequiredService<IRunOrchestrator>();

        var a = await ConnectNode(host, fixture.FragmentAJobPath, "A", new EdgeBinding("out", EdgeDirection.Outbound));
        var b = await ConnectNode(host, fixture.FragmentBJobPath, "B",
            new EdgeBinding("in", EdgeDirection.Inbound), new EdgeBinding("out", EdgeDirection.Outbound));
        await using var c = await ConnectNode(host, fixture.FragmentCJobPath, "C", new EdgeBinding("in", EdgeDirection.Inbound));

        var runTask = orchestrator.RunAsync(ChainSpec("run-vanish"));

        var deadline = DateTime.UtcNow + Bound;
        while (!b.IsLaunched)
        {
            if (DateTime.UtcNow > deadline) throw new TimeoutException("B was never launched");
            await Task.Delay(10);
        }
        await Task.Delay(200); // let bytes actually cross both of B's edges before it vanishes

        var sw = System.Diagnostics.Stopwatch.StartNew();
        await b.DisposeAsync(); // the whole node, connection included - never just its child

        var result = await runTask.WaitAsync(Bound);
        sw.Stop();

        Assert.Equal(RunOutcome.Failed, result.Outcome);
        Assert.Equal("B", result.Cause);
        Assert.True(sw.Elapsed < TimeSpan.FromSeconds(10),
            $"cause attribution took {sw.Elapsed.TotalSeconds:F1}s - should be bounded by the disconnect " +
            "grace period, seconds, not by a peer's own stall window (tens of seconds to minutes)");

        await Task.WhenAll(a.Completion, c.Completion).WaitAsync(Bound);
        await a.DisposeAsync();
    }
}
