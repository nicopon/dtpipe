using System.Runtime.InteropServices;
using DtPipe.Coordinator;
using DtPipe.Coordinator.Tests.Infrastructure;
using Microsoft.Extensions.DependencyInjection;
using TransportR.FlowControl;
using Xunit;

namespace DtPipe.PipelineNode.Tests;

/// <summary>
/// The verdict of a run that fails: how soon the coordinator gives it, and what it names. A local
/// fault ends the run at once whatever the peers are doing, and a transfer the hub refuses is named
/// as such - never as a fragment that went quiet.
/// </summary>
public class FaultVerdictTests
{
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(20);

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

    private static RunSpec PairSpec(string runId) => new(
        runId, [new FragmentPin("A"), new FragmentPin("B")], [new RunEdge("A", "out", "B", "in")]);

    /// <summary>
    /// Two producers feed a merging consumer. A produces nothing until its slow source ends (an
    /// aggregate), so no torn-down transfer ever reaches its child; B streams. B's child is killed
    /// once everything is wired: B reports a local fault, the consumer is left waiting on A, and only
    /// the coordinator can end A. The verdict must name B within seconds, not wait for A's source.
    /// </summary>
    [Fact]
    public async Task ALocalFault_EndsTheRun_EvenWhenAPeerChildHasNothingToWrite()
    {
        using var fixture = MergeFixture.Create(rowCountA: 10, rowCountB: 10);
        var dir = Directory.CreateTempSubdirectory("pnode-localfault-");
        try
        {
            var producerA = Path.Combine(dir.FullName, "producer-a.yaml");
            File.WriteAllText(producerA,
                "g:\n  input: generate:100000000\n  provider-options:\n    generate:\n      rows-per-second: 1000\n" +
                "agg:\n  from: g\n  output: arrow:-\n  provider-options:\n    sql:\n      query: SELECT count(*) AS n FROM g\n");
            var producerB = Path.Combine(dir.FullName, "producer-b.yaml");
            File.WriteAllText(producerB,
                "main:\n  input: generate:100000000\n  output: arrow:-\n  provider-options:\n    generate:\n      rows-per-second: 1000\n");

            await using var host = await CoordinatorTestHost.StartAsync(
                o => o.Groups["test"] = new GroupAccess { CanSendTo = ["test"] });
            var orchestrator = host.Host.Services.GetRequiredService<IRunOrchestrator>();

            await using var a = await ConnectNode(host, producerA, "A", new EdgeBinding("agg", EdgeDirection.Outbound));
            await using var b = await ConnectNode(host, producerB, "B", new EdgeBinding("main", EdgeDirection.Outbound));
            await using var c = await ConnectNode(host, fixture.ConsumerJobPath, "C",
                new EdgeBinding("a", EdgeDirection.Inbound), new EdgeBinding("b", EdgeDirection.Inbound));

            var runTask = orchestrator.RunAsync(new RunSpec("run-local-fault",
                [new FragmentPin("A"), new FragmentPin("B"), new FragmentPin("C")],
                [new RunEdge("A", "agg", "C", "a"), new RunEdge("B", "main", "C", "b")]));

            var deadline = DateTime.UtcNow + Bound;
            while (!(a.IsLaunched && b.IsLaunched && c.IsLaunched))
            {
                if (DateTime.UtcNow > deadline) throw new TimeoutException("the fragments were never launched");
                await Task.Delay(10);
            }
            await Task.Delay(1500); // wired, B streaming and A's child busy reading its slow source

            var rc = kill(b.ChildProcessId, SIGKILL);
            if (rc != 0) throw new InvalidOperationException($"kill returned {rc}, errno={Marshal.GetLastWin32Error()}");

            var sw = System.Diagnostics.Stopwatch.StartNew();
            var result = await runTask.WaitAsync(Bound);
            sw.Stop();

            Assert.Equal(RunOutcome.Failed, result.Outcome);
            Assert.Equal("B", result.Cause);
            Assert.True(sw.Elapsed < TimeSpan.FromSeconds(10), $"the verdict took {sw.Elapsed.TotalSeconds:F1}s");
        }
        finally
        {
            try { dir.Delete(recursive: true); } catch { /* best-effort scratch cleanup */ }
        }
    }

    /// <summary>
    /// A produces nothing until its slow source ends (an aggregate), so its consumer B has nothing to
    /// relay: B's child dies while B's inbound relay waits on a peer that says nothing. The node must
    /// notice its child's death on its own, report a local fault, and let the coordinator end A.
    /// </summary>
    [Fact]
    public async Task AChildThatDiesWhileItsRelayWaitsOnASilentPeer_IsReportedAtOnce()
    {
        var dir = Directory.CreateTempSubdirectory("pnode-childdeath-");
        try
        {
            var producerJob = Path.Combine(dir.FullName, "producer.yaml");
            File.WriteAllText(producerJob,
                "g:\n  input: generate:100000000\n  provider-options:\n    generate:\n      rows-per-second: 1000\n" +
                "agg:\n  from: g\n  output: arrow:-\n  provider-options:\n    sql:\n      query: SELECT count(*) AS n FROM g\n");
            var consumerJob = Path.Combine(dir.FullName, "consumer.yaml");
            File.WriteAllText(consumerJob, $"main:\n  input: arrow:-\n  output: csv:{Path.Combine(dir.FullName, "out.csv")}\n");

            await using var host = await CoordinatorTestHost.StartAsync(
                o => o.Groups["test"] = new GroupAccess { CanSendTo = ["test"] });
            var orchestrator = host.Host.Services.GetRequiredService<IRunOrchestrator>();

            await using var a = await ConnectNode(host, producerJob, "A", new EdgeBinding("agg", EdgeDirection.Outbound));
            await using var b = await ConnectNode(host, consumerJob, "B", new EdgeBinding("in", EdgeDirection.Inbound));

            var runTask = orchestrator.RunAsync(new RunSpec("run-child-death",
                [new FragmentPin("A"), new FragmentPin("B")], [new RunEdge("A", "agg", "B", "in")]));

            var deadline = DateTime.UtcNow + Bound;
            while (!(a.IsLaunched && b.IsLaunched))
            {
                if (DateTime.UtcNow > deadline) throw new TimeoutException("A and B were never launched");
                await Task.Delay(10);
            }
            await Task.Delay(1500); // wired, A's child busy reading its slow source

            var rc = kill(b.ChildProcessId, SIGKILL);
            if (rc != 0) throw new InvalidOperationException($"kill returned {rc}, errno={Marshal.GetLastWin32Error()}");

            var sw = System.Diagnostics.Stopwatch.StartNew();
            var result = await runTask.WaitAsync(Bound);
            sw.Stop();

            Assert.Equal(RunOutcome.Failed, result.Outcome);
            Assert.Equal("B", result.Cause);
            Assert.True(sw.Elapsed < TimeSpan.FromSeconds(10), $"the verdict took {sw.Elapsed.TotalSeconds:F1}s");
        }
        finally
        {
            try { dir.Delete(recursive: true); } catch { /* best-effort scratch cleanup */ }
        }
    }

    /// <summary>
    /// The matrix is revoked after the nodes registered, the way an operator changes rights under a
    /// deployment: the hub refuses the transfer when the run opens it. The verdict says a transfer was
    /// refused on that edge, names no fragment as unresponsive, and gives no detail of the rights
    /// involved - the groups, the direction, the rule - to whoever reads it.
    /// </summary>
    [Fact]
    public async Task ARefusedTransfer_IsTheVerdict_NamingTheEdge_NotAFragmentThatWentQuiet()
    {
        using var fixture = ChainFixture.Create(rowCount: 1_000);
        await using var host = await CoordinatorTestHost.StartAsync(
            o => o.Groups["test"] = new GroupAccess { CanSendTo = ["test"] });
        var orchestrator = host.Host.Services.GetRequiredService<IRunOrchestrator>();

        await using var a = await ConnectNode(host, fixture.FragmentAJobPath, "A", new EdgeBinding("out", EdgeDirection.Outbound));
        await using var b = await ConnectNode(host, fixture.FragmentCJobPath, "B", new EdgeBinding("in", EdgeDirection.Inbound));

        host.Host.Services.GetRequiredService<FlowControlOptions>().Groups["test"] = new GroupAccess { CanSendTo = [] };

        var result = await orchestrator.RunAsync(PairSpec("run-refused-transfer")).WaitAsync(Bound);

        Assert.Equal(RunOutcome.Failed, result.Outcome);
        Assert.False(result.CauseIsUnresponsive);
        Assert.Contains("A", result.Cause);
        Assert.Contains("B", result.Cause);
        Assert.DoesNotContain("test", result.Describe());
    }
}
