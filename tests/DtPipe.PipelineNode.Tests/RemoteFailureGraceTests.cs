using System.Diagnostics;
using DtPipe.Coordinator;
using DtPipe.Coordinator.Tests.Infrastructure;
using Microsoft.Extensions.DependencyInjection;
using TransportR.FlowControl;
using TransportR.Interfaces;
using Xunit;

namespace DtPipe.PipelineNode.Tests;

/// <summary>
/// The one clock a run has: once a fragment has failed because its transfer failed under it, its peers
/// owe the run their own report within <see cref="RunOrchestratorOptions.RemoteFailureGrace"/>, or the
/// run is torn down naming the ones that did not report. A run in which nothing has failed has no clock.
/// </summary>
public class RemoteFailureGraceTests
{
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(60);

    private static async Task<PipelineNode> ConnectNode(
        CoordinatorTestHost host, string jobPath, string fragmentName, params EdgeBinding[] edges) =>
        await PipelineNode.ConnectAsync(new PipelineNodeOptions
        {
            HubUrl = host.Url,
            DtPipeExecutable = DtPipeExecutableLocator.Path,
            FragmentJobPath = jobPath,
            Edges = edges,
        }, fragmentName);

    private static Task<CoordinatorTestHost> StartHost(TimeSpan grace) =>
        CoordinatorTestHost.StartAsync(
            o => o.Groups["test"] = new GroupAccess { CanSendTo = ["test"] },
            services => services.AddSingleton(new RunOrchestratorOptions { RemoteFailureGrace = grace }));

    private static RunSpec PairSpec(string runId) => new(
        runId, [new FragmentPin("A"), new FragmentPin("B")], [new RunEdge("A", "out", "B", "in")]);

    private static async Task WaitLaunched(params PipelineNode[] nodes)
    {
        var deadline = DateTime.UtcNow + Bound;
        while (!nodes.All(n => n.IsLaunched))
        {
            if (DateTime.UtcNow > deadline) throw new TimeoutException("the fragments were never launched");
            await Task.Delay(10);
        }
    }

    private static async Task<string> WaitForTheTransfer(CoordinatorTestHost host)
    {
        var transfers = host.Host.Services.GetRequiredService<ITransferManager>();
        var deadline = DateTime.UtcNow + Bound;
        while (true)
        {
            var open = transfers.GetAllTransfers().FirstOrDefault();
            if (open is not null) return open.TransferId;
            if (DateTime.UtcNow > deadline) throw new TimeoutException("the run never opened its transfer");
            await Task.Delay(20);
        }
    }

    // Throttled on purpose: these tests only need data in flight, and a producer running flat out takes the CPU
    // from every timing-sensitive test running beside them.
    private const string Producer =
        "main:\n  input: generate:100000000\n  output: arrow:-\n  provider-options:\n    generate:\n      rows-per-second: 50000\n";

    /// <summary>
    /// A consumer whose sink never reads (its child blocks opening a FIFO nobody reads): its node reports
    /// nothing, whatever happens to its transfer. The transfer is torn down under the producer, which
    /// fails remotely and reports. Nothing else would end this run; after the grace it is torn down, and
    /// the verdict names the consumer, not the producer that failed because of it.
    /// </summary>
    [Fact]
    public async Task APeerThatStaysSilentAfterAFragmentFailsRemotely_IsNamedAndTornDownAfterTheGrace()
    {
        var grace = TimeSpan.FromSeconds(3);
        var dir = Directory.CreateTempSubdirectory("pnode-grace-");
        try
        {
            var producerJob = Path.Combine(dir.FullName, "producer.yaml");
            File.WriteAllText(producerJob, Producer);
            var fifo = Path.Combine(dir.FullName, "sink.fifo");
            using (var mkfifo = Process.Start("mkfifo", fifo)!) mkfifo.WaitForExit();
            var consumerJob = Path.Combine(dir.FullName, "consumer.yaml");
            File.WriteAllText(consumerJob, $"main:\n  input: arrow:-\n  output: csv:{fifo}\n");

            await using var host = await StartHost(grace);
            var orchestrator = host.Host.Services.GetRequiredService<IRunOrchestrator>();
            await using var a = await ConnectNode(host, producerJob, "A", new EdgeBinding("out", EdgeDirection.Outbound));
            await using var b = await ConnectNode(host, consumerJob, "B", new EdgeBinding("in", EdgeDirection.Inbound));

            var runTask = orchestrator.RunAsync(PairSpec("run-silent-peer"));
            await WaitLaunched(a, b);
            var transferId = await WaitForTheTransfer(host);
            await Task.Delay(1500); // data flowing, the consumer's sink blocked

            var sw = Stopwatch.StartNew();
            await host.Host.Services.GetRequiredService<ITransferTerminator>().TerminateAsync(transferId);
            var result = await runTask.WaitAsync(Bound);
            sw.Stop();

            Assert.Equal(RunOutcome.Failed, result.Outcome);
            Assert.Equal("B", result.Cause);
            Assert.True(result.CauseIsUnresponsive);
            Assert.Equal(["A"], result.Consequences);
            Assert.Equal(Coordinator.FaultOrigin.Remote, result.Reports["A"].Origin);
            Assert.True(sw.Elapsed >= grace - TimeSpan.FromSeconds(1), $"the run ended after {sw.Elapsed.TotalSeconds:F1}s, before its grace");
            Assert.True(sw.Elapsed < grace + TimeSpan.FromSeconds(25), $"the run took {sw.Elapsed.TotalSeconds:F1}s to end");

            // The silent fragment was told to cancel, not left behind: its child is gone and it reports.
            await b.Completion.WaitAsync(Bound);
            Assert.True(b.ChildHasExited);
        }
        finally
        {
            dir.Delete(recursive: true);
        }
    }

    /// <summary>
    /// Both peers fail remotely and report at once: the run ends as soon as the second report arrives, long
    /// before the grace, and no fragment is called unresponsive.
    /// </summary>
    [Fact]
    public async Task PeersThatReportInsideTheGrace_EndTheRunAtOnce_WithoutBeingNamedUnresponsive()
    {
        var grace = TimeSpan.FromSeconds(30);
        var dir = Directory.CreateTempSubdirectory("pnode-grace-");
        try
        {
            var producerJob = Path.Combine(dir.FullName, "producer.yaml");
            File.WriteAllText(producerJob, Producer);
            var consumerJob = Path.Combine(dir.FullName, "consumer.yaml");
            File.WriteAllText(consumerJob, $"main:\n  input: arrow:-\n  output: csv:{Path.Combine(dir.FullName, "out.csv")}\n");

            await using var host = await StartHost(grace);
            var orchestrator = host.Host.Services.GetRequiredService<IRunOrchestrator>();
            await using var a = await ConnectNode(host, producerJob, "A", new EdgeBinding("out", EdgeDirection.Outbound));
            await using var b = await ConnectNode(host, consumerJob, "B", new EdgeBinding("in", EdgeDirection.Inbound));

            var runTask = orchestrator.RunAsync(PairSpec("run-prompt-peers"));
            await WaitLaunched(a, b);
            var transferId = await WaitForTheTransfer(host);
            await Task.Delay(1500);

            var sw = Stopwatch.StartNew();
            await host.Host.Services.GetRequiredService<ITransferTerminator>().TerminateAsync(transferId);
            var result = await runTask.WaitAsync(Bound);
            sw.Stop();

            Assert.Equal(RunOutcome.Failed, result.Outcome);
            Assert.False(result.CauseIsUnresponsive);
            Assert.True(sw.Elapsed < TimeSpan.FromSeconds(15), $"the run took {sw.Elapsed.TotalSeconds:F1}s: it waited on the grace");
        }
        finally
        {
            dir.Delete(recursive: true);
        }
    }

    /// <summary>
    /// A transfer that simply takes longer than the grace is never reported failed for it: nothing has failed,
    /// so nothing is timed.
    /// </summary>
    [Fact]
    public async Task ARunInWhichNoFragmentFails_IsNeverBoundedByTheGrace()
    {
        var grace = TimeSpan.FromMilliseconds(500);
        var dir = Directory.CreateTempSubdirectory("pnode-grace-");
        try
        {
            var producerJob = Path.Combine(dir.FullName, "producer.yaml");
            File.WriteAllText(producerJob,
                "main:\n  input: generate:3000\n  output: arrow:-\n  provider-options:\n    generate:\n      rows-per-second: 1000\n");
            var consumerJob = Path.Combine(dir.FullName, "consumer.yaml");
            File.WriteAllText(consumerJob, $"main:\n  input: arrow:-\n  output: csv:{Path.Combine(dir.FullName, "out.csv")}\n");

            await using var host = await StartHost(grace);
            var orchestrator = host.Host.Services.GetRequiredService<IRunOrchestrator>();
            await using var a = await ConnectNode(host, producerJob, "A", new EdgeBinding("out", EdgeDirection.Outbound));
            await using var b = await ConnectNode(host, consumerJob, "B", new EdgeBinding("in", EdgeDirection.Inbound));

            var sw = Stopwatch.StartNew();
            var result = await orchestrator.RunAsync(PairSpec("run-no-failure")).WaitAsync(Bound);
            sw.Stop();

            Assert.Equal(RunOutcome.Succeeded, result.Outcome);
            Assert.True(sw.Elapsed > TimeSpan.FromSeconds(2), $"the run took {sw.Elapsed.TotalSeconds:F1}s: it should outlive the grace several times over");
        }
        finally
        {
            dir.Delete(recursive: true);
        }
    }
}
