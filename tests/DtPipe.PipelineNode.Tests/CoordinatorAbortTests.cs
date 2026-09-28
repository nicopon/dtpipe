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
        Fragments: [new FragmentPin("A"), new FragmentPin("B"), new FragmentPin("C")],
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
        await c.ControlConnection.InvokeAsync("Register", "C", "v-test");

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
    /// Proves that <c>FragmentLost</c> is what ends the execution-phase wait and resolves the run
    /// (confirmed by temporarily disabling the <c>abort.Cancel()</c> call it drives - the run then
    /// hangs until <c>Bound</c> and the test fails), and that the result names B correctly, quickly.
    /// It does <b>not</b> isolate <c>ITransferTerminator.TerminateAsync</c> as what unblocks A and C:
    /// disposing <see cref="PipelineNode"/> in-process closes its own data-plane connections cleanly,
    /// which resolves them on its own (confirmed the same way, temporarily disabling the
    /// <c>TerminateAsync</c> call instead - the run still passed). An abruptly unresponsive peer - a
    /// network partition, a process that hangs rather than exits - is the case <c>TerminateAsync</c>
    /// exists for, and is not reproducible this way: <see cref="PipelineNode"/> runs in-process here,
    /// so there is no separate process left to kill without also going through its own clean disposal
    /// path.
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

    /// <summary>
    /// A fully wired fragment whose child has nothing to write - here an aggregate over a slow
    /// source, which emits only once its input ends - never touches its torn-down outbound transfer,
    /// so that transfer alone cannot fault it. The requester's cancellation must still end the run
    /// within the grace period, with that fragment reporting a <see cref="FaultOrigin.Remote"/>
    /// consequence rather than the transfer's own disposal as a local fault.
    /// </summary>
    [Fact]
    public async Task RequesterCancelsWhileAFullyWiredChildIsSilent_EndsWithinTheGracePeriod()
    {
        var dir = Directory.CreateTempSubdirectory("pnode-silent-");
        try
        {
            var producerJob = Path.Combine(dir.FullName, "producer.yaml");
            File.WriteAllText(producerJob,
                "g:\n  input: generate:100000000\n  provider-options:\n    generate:\n      rows-per-second: 1000\n" +
                "agg:\n  from: g\n  output: arrow:-\n  provider-options:\n    sql:\n      query: SELECT count(*) AS n FROM g\n");
            var consumerJob = Path.Combine(dir.FullName, "consumer.yaml");
            File.WriteAllText(consumerJob, $"main:\n  input: arrow:-\n  output: csv:{Path.Combine(dir.FullName, "out.csv")}\n");

            await using var host = await CoordinatorTestHost.StartAsync(
                o => o.Groups["test"] = new TransportR.FlowControl.GroupAccess { CanSendTo = ["test"] });
            var orchestrator = host.Host.Services.GetRequiredService<IRunOrchestrator>();

            await using var a = await ConnectNode(host, producerJob, "A", new EdgeBinding("out", EdgeDirection.Outbound));
            await using var b = await ConnectNode(host, consumerJob, "B", new EdgeBinding("in", EdgeDirection.Inbound));

            using var cts = new CancellationTokenSource();
            var runTask = orchestrator.RunAsync(new RunSpec("run-silent-cancel",
                [new FragmentPin("A"), new FragmentPin("B")], [new RunEdge("A", "out", "B", "in")]), cts.Token);

            var deadline = DateTime.UtcNow + Bound;
            while (!(a.IsLaunched && b.IsLaunched))
            {
                if (DateTime.UtcNow > deadline) throw new TimeoutException("A and B were never launched");
                await Task.Delay(10);
            }
            await Task.Delay(1500); // wired, and A's child busy reading its slow source

            var sw = System.Diagnostics.Stopwatch.StartNew();
            cts.Cancel();
            var result = await runTask.WaitAsync(Bound);
            await a.Completion.WaitAsync(Bound);
            sw.Stop();

            Assert.Equal(RunOutcome.Cancelled, result.Outcome);
            Assert.True(sw.Elapsed < TimeSpan.FromSeconds(10),
                $"the silent fragment took {sw.Elapsed.TotalSeconds:F1}s to end - it should be bounded by the grace period");
            Assert.Equal(FaultOrigin.Remote, a.Origin);
        }
        finally
        {
            dir.Delete(recursive: true);
        }
    }

    /// <summary>
    /// Two live instances of "A" - the same job, hence the same version, so instance alignment
    /// passes and admission is free to pick either. <see cref="RunOrchestrator.OnFragmentLost"/>
    /// (untouched by this lot) already discriminates a <c>FragmentLost</c> report by comparing its
    /// ClientId against the one this run actually admitted; this proves that path for real, against a
    /// fragment name with more than one live instance - the shape the pre-lot <c>NodeRegistry</c>
    /// could not even represent.
    /// </summary>
    [Fact]
    public async Task ANonSelectedInstanceOfAnAlignedFragmentName_CanDropWithoutAffectingAnInFlightRun()
    {
        using var fixture = ChainFixture.Create(rowCount: 1_000);
        await using var host = await CoordinatorTestHost.StartAsync(
            o => o.Groups["test"] = new TransportR.FlowControl.GroupAccess { CanSendTo = ["test"] },
            services => services.AddSingleton(new NodeRegistryOptions { DisconnectGracePeriod = TimeSpan.FromMilliseconds(300) }));
        var orchestrator = host.Host.Services.GetRequiredService<IRunOrchestrator>();
        var registry = host.Host.Services.GetRequiredService<INodeRegistry>();

        var a1 = await ConnectNode(host, fixture.FragmentAJobPath, "A", new EdgeBinding("out", EdgeDirection.Outbound));
        var a2 = await ConnectNode(host, fixture.FragmentAJobPath, "A", new EdgeBinding("out", EdgeDirection.Outbound));
        await using var b = await ConnectNode(host, fixture.FragmentBJobPath, "B",
            new EdgeBinding("in", EdgeDirection.Inbound), new EdgeBinding("out", EdgeDirection.Outbound));
        await using var c = await ConnectNode(host, fixture.FragmentCJobPath, "C", new EdgeBinding("in", EdgeDirection.Inbound));

        var lost = new List<(string Fragment, Guid ClientId)>();
        registry.FragmentLost += (fragment, clientId) => lost.Add((fragment, clientId));

        var runTask = orchestrator.RunAsync(ChainSpec("run-redundant-a"));

        var deadline = DateTime.UtcNow + Bound;
        while (!a1.IsLaunched && !a2.IsLaunched)
        {
            if (DateTime.UtcNow > deadline) throw new TimeoutException("neither instance of A was ever launched");
            await Task.Delay(10);
        }
        // Whichever of the two admission happened to pick keeps running the actual chain; the other
        // never receives Launch at all, and is the one this test drops.
        var (selected, notSelected) = a1.IsLaunched ? (a1, a2) : (a2, a1);
        var notSelectedClientId = notSelected.ClientId;

        await notSelected.DisposeAsync(); // the whole non-selected instance vanishes - never wired, never launched
        await Task.Delay(500); // past the 300ms grace period configured above

        Assert.Contains(lost, e => e.Fragment == "A" && e.ClientId == notSelectedClientId);

        var result = await runTask.WaitAsync(Bound);

        Assert.Equal(RunOutcome.Succeeded, result.Outcome);
        Assert.Null(result.Cause);

        await selected.DisposeAsync();
    }
}
