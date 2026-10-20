using System.Diagnostics;
using DtPipe.Coordinator;
using DtPipe.Coordinator.Contracts;
using DtPipe.Coordinator.Tests.Infrastructure;
using DtPipe.PipelineNode.Hosting;
using DtPipe.PipelineNode.Tests.Infrastructure;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace DtPipe.PipelineNode.Tests;

/// <summary>
/// The run protocol of one replica (<see cref="ReplicaRuns"/>): a handler returns as soon as it has the message, a run the
/// coordinator never wires is abandoned, and a report that could not be delivered is held for the next announcement.
/// </summary>
public class ReplicaRunsTests : IDisposable
{
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(30);
    private const string WaitsOnItsInput = "a:\n  input: arrow:-\n  output: csv:/dev/null\n";

    private readonly DirectoryInfo _dir = Directory.CreateTempSubdirectory("replica-runs-");

    /// <summary>The coordinator's node link, noting when it holds the replica's answer to a <c>Launch</c>.</summary>
    private sealed class LaunchAnswerSignal(INodeLink inner) : INodeLink
    {
        public TaskCompletionSource Answered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public async Task<LaunchResult> LaunchAsync(ReplicaKey replica, LaunchRequest request, CancellationToken ct)
        {
            try
            {
                var result = await inner.LaunchAsync(replica, request, ct);
                Answered.TrySetResult();
                return result;
            }
            catch (Exception ex)
            {
                Answered.TrySetException(ex);
                throw;
            }
        }

        public Task<bool> WireAsync(ReplicaKey replica, string runId, string fragment, string alias, string transferId, CancellationToken ct) =>
            inner.WireAsync(replica, runId, fragment, alias, transferId, ct);

        public Task<bool> CancelAsync(ReplicaKey replica, string runId, string fragment, CancellationToken ct) =>
            inner.CancelAsync(replica, runId, fragment, ct);

        public Task<FragmentCheck> CheckAsync(ReplicaKey replica, FragmentSpec spec, CancellationToken ct) => inner.CheckAsync(replica, spec, ct);

        public Task<int> KillAsync(ReplicaKey replica, string fragment, CancellationToken ct) => inner.KillAsync(replica, fragment, ct);
    }

    public void Dispose() => _dir.Delete(recursive: true);

    private async Task<(CoordinatorTestHost Host, PipelineNode Node, ReplicaRuns Runs)> StartAsync(TimeSpan unwired)
    {
        var host = await CoordinatorTestHost.StartAsync(o => o.Groups["test"] = new TransportR.FlowControl.GroupAccess { CanSendTo = ["test"] });
        var node = await PipelineNode.ConnectAsync(new NodeClientOptions { HubUrl = host.Url });
        var runs = new ReplicaRuns(
            host.CreateNodeChannel(), node, "A", DtPipeExecutableLocator.Path, new Dictionary<string, string>(), _dir.FullName,
            spec => FragmentIntake.JobOf(spec, FragmentPolicy.AnyFragment, null, (_, _) => null, (_, _) => null, []),
            _ => NullLogger.Instance, NullLogger.Instance, unwired);
        return (host, node, runs);
    }

    private static LaunchRequest WaitingRun(string runId) =>
        new(runId, new FragmentSpec("A", "v", [new FragmentEdge("in", FragmentEdgeDirection.Inbound)], Yaml: WaitsOnItsInput), $"token-{runId}");

    /// <summary>
    /// A <c>Cancel</c> answers when it has faulted the run: it does not wait for the child to end, nor for another run
    /// of the replica. The node channel's client takes messages one after another, so a handler that waited would hold
    /// back every message behind it.
    /// </summary>
    [Fact]
    public async Task ACancel_IsAnsweredAtOnce_AndLeavesTheOtherRunsOfTheReplicaRunning()
    {
        var (host, node, runs) = await StartAsync(unwired: TimeSpan.FromMinutes(5));
        await using var _host = host;
        await using var _node = node;
        await using var _runs = runs;
        Assert.True((await runs.LaunchAsync(WaitingRun("run-1"))).Started);
        var other = WaitingRun("run-2");
        Assert.True((await runs.LaunchAsync(other)).Started);
        Assert.Equal(2, runs.Count);

        var sw = Stopwatch.StartNew();
        Assert.True(runs.Cancel("run-1", "A"));
        sw.Stop();

        Assert.True(sw.ElapsedMilliseconds < 500, $"the cancel took {sw.ElapsedMilliseconds}ms to answer");
        Assert.False(runs.Cancel("run-0", "A"));
        var deadline = DateTime.UtcNow + Bound;
        while (runs.Count > 1)
        {
            if (DateTime.UtcNow > deadline) throw new TimeoutException("the cancelled run never ended");
            await Task.Delay(50);
        }
        Assert.Equal(1, runs.Count);
        Assert.False(runs.Cancel("run-1", "A"));
        Assert.True(runs.Cancel("run-2", "A"));
    }

    [Fact]
    public async Task ARunTheCoordinatorNeverWires_IsAbandonedAfterTheUnwiredTimeout()
    {
        var (host, node, runs) = await StartAsync(unwired: TimeSpan.FromSeconds(1));
        await using var _host = host;
        await using var _node = node;
        await using var _runs = runs;
        Assert.True((await runs.LaunchAsync(WaitingRun("run-1"))).Started);

        var deadline = DateTime.UtcNow + Bound;
        while (runs.Count > 0)
        {
            if (DateTime.UtcNow > deadline) throw new TimeoutException("the unwired run was never abandoned");
            await Task.Delay(50);
        }
    }

    /// <summary>Once the coordinator has asked for every edge of the run, the run is its to finish: the bound that waits for a <c>Wire</c> is over.</summary>
    [Fact]
    public async Task ARunWhoseEdgesAreAllAskedFor_IsNotAbandonedForWantOfAWire()
    {
        var (host, node, runs) = await StartAsync(unwired: TimeSpan.FromSeconds(1));
        await using var _host = host;
        await using var _node = node;
        await using var _runs = runs;
        Assert.True((await runs.LaunchAsync(WaitingRun("run-1"))).Started);

        Assert.True(runs.Wire("run-1", "A", "in", "a-transfer-that-never-opens"));
        Assert.False(runs.Wire("run-1", "A", "no-such-edge", "t"));
        await Task.Delay(2500);

        Assert.Equal(1, runs.Count);
    }

    /// <summary>A replica that holds no data plane client launches nothing: the run protocol answers that the run did not start.</summary>
    [Fact]
    public async Task WithNoDataPlaneClient_ALaunchIsRefused()
    {
        var (host, node, runs) = await StartAsync(unwired: TimeSpan.FromMinutes(5));
        await using var _host = host;
        await using var _node = node;
        await using var _runs = runs;
        runs.UseNode(null);

        var result = await runs.LaunchAsync(WaitingRun("run-1"));

        Assert.False(result.Started);
        Assert.Equal(0, runs.Count);
    }

    /// <summary>A run asked to wire after its client was dropped fails at once, not after the wait for a transfer that cannot come.</summary>
    [Fact]
    public async Task ARunWiredAfterItsClientWasDropped_FailsAtOnce()
    {
        var (host, node, runs) = await StartAsync(unwired: TimeSpan.FromMinutes(5));
        await using var _host = host;
        await using var _node = node;
        await using var _runs = runs;
        Assert.True((await runs.LaunchAsync(WaitingRun("run-1"))).Started);
        runs.UseNode(null);

        Assert.True(runs.Wire("run-1", "A", "in", "a-transfer-that-never-opens"));

        var deadline = DateTime.UtcNow + Bound;
        while (runs.Count > 0)
        {
            if (DateTime.UtcNow > deadline) throw new TimeoutException("the run whose client was dropped never ended");
            await Task.Delay(50);
        }
    }

    /// <summary>
    /// The replica's channel drops while its fragment runs. The fragment ends and cannot say so; the coordinator keeps the
    /// replica for its grace period, and the report goes out once the same host announces itself again, so the run still
    /// ends on what the fragment said.
    /// </summary>
    [Fact]
    public async Task AReportThatCouldNotBeDelivered_IsHeld_AndSentAfterTheNextAnnouncement()
    {
        var job = Path.Combine(_dir.FullName, "job.yaml");
        File.WriteAllText(job, $"a:\n  input: generate:50\n  provider-options:\n    generate:\n      rows-per-second: 10\n  output: csv:{Path.Combine(_dir.FullName, "out.csv")}\n");
        await using var host = await CoordinatorTestHost.StartAsync(
            o => o.Groups["test"] = new TransportR.FlowControl.GroupAccess { CanSendTo = ["test"] },
            services => services.Replace(ServiceDescriptor.Singleton<INodeLink>(sp => new LaunchAnswerSignal(ActivatorUtilities.CreateInstance<HubNodeLink<TestNodeHub>>(sp)))));
        await using var node = await TestNode.StartAsync(host, "A", job, []);
        var orchestrator = host.Host.Services.GetRequiredService<IRunOrchestrator>();
        var launch = (LaunchAnswerSignal)host.Host.Services.GetRequiredService<INodeLink>();

        var run = orchestrator.RunAsync(new RunSpec("run-held", [node.Fragment], []));
        // The channel is cut once the coordinator holds the answer to its Launch: a replica whose child is up but whose answer is still
        // on its way would otherwise be a fragment that went quiet, and the run would end at once.
        await launch.Answered.Task.WaitAsync(Bound);
        await node.StopChannelAsync();

        await node.Completion.WaitAsync(Bound); // the fragment ended and tried to report
        await Task.Delay(300);
        Assert.False(run.IsCompleted, "the report reached the coordinator on a connection that was closed");

        await node.AnnounceAgainAsync();
        var result = await run.WaitAsync(Bound);
        Assert.Equal(RunOutcome.Succeeded, result.Outcome);
    }
}
