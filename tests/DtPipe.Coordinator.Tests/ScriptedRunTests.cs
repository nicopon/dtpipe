using System.Collections.Concurrent;
using DtPipe.Coordinator.Contracts;
using Microsoft.Extensions.Logging.Abstractions;
using TransportR.Interfaces;
using TransportR.Models;
using TransportR.Services;
using Xunit;

namespace DtPipe.Coordinator.Tests;

/// <summary>
/// The wiring, the grace after a remote failure and the teardown of a run, with a scripted replica link and scripted
/// transfers instead of a hub: the orchestrator is built directly, every message it sends is answered by the test, and a
/// replica's report is delivered by calling <see cref="IRunOrchestrator.OnExitedAsync"/> with the token of the launch.
/// Two fragments, <c>p</c> and <c>c</c>, joined by one edge <c>p.out -> c.in</c>.
/// </summary>
public class ScriptedRunTests
{
    private static readonly TimeSpan Wait = TimeSpan.FromSeconds(10);

    private sealed class ScriptedLink : INodeLink
    {
        public IRunOrchestrator Orchestrator { get; set; } = null!;
        public Dictionary<string, ReplicaKey> Replicas { get; } = new();
        public ConcurrentQueue<LaunchRequest> Launches { get; } = new();
        public ConcurrentQueue<(string Fragment, string Alias, string TransferId)> Wires { get; } = new();
        public ConcurrentQueue<string> Cancels { get; } = new();

        public Func<string, Task<bool>> OnWire { get; set; } = _ => Task.FromResult(true);
        public Func<string, Task<bool>> OnCancel { get; set; } = _ => Task.FromResult(true);

        public Task<LaunchResult> LaunchAsync(ReplicaKey replica, LaunchRequest request, CancellationToken ct)
        {
            Launches.Enqueue(request);
            return Task.FromResult(new LaunchResult(true));
        }

        public async Task<bool> WireAsync(ReplicaKey replica, string runId, string fragment, string alias, string transferId, CancellationToken ct)
        {
            Wires.Enqueue((fragment, alias, transferId));
            return await OnWire(fragment).WaitAsync(ct);
        }

        public async Task<bool> CancelAsync(ReplicaKey replica, string runId, string fragment, CancellationToken ct)
        {
            Cancels.Enqueue(fragment);
            return await OnCancel(fragment);
        }

        public Task<FragmentCheck> CheckAsync(ReplicaKey replica, FragmentSpec spec, CancellationToken ct) => throw new NotSupportedException();
        public Task<int> KillAsync(ReplicaKey replica, string fragment, CancellationToken ct) => throw new NotSupportedException();

        /// <summary>A replica reports how a fragment ended, under the token of the launch it answered.</summary>
        public Task<ExitedAck> ExitAsync(string fragment, int exitCode, string origin = "Local", IReadOnlyDictionary<string, long>? rowCounts = null)
        {
            var launch = Launches.Last(l => l.Spec.Fragment == fragment);
            return Orchestrator.OnExitedAsync(Replicas[fragment], new FragmentExit(
                launch.RunId, fragment, exitCode, origin, exitCode == 0 ? null : $"{fragment} failed", rowCounts ?? new Dictionary<string, long>(), launch.LaunchToken));
        }
    }

    private sealed class ScriptedTransfers : ITransferInitiator, ITransferTerminator
    {
        private int _next;
        public Func<Task<string>>? OnInit { get; set; }
        public ConcurrentQueue<string> Opened { get; } = new();
        public ConcurrentQueue<string> Terminated { get; } = new();

        public async Task<string> InitTransferAsync(Guid producerClientId, Guid consumerClientId, int batchSize, int timeoutMs, CancellationToken ct)
        {
            var id = OnInit is null ? $"transfer-{Interlocked.Increment(ref _next)}" : await OnInit();
            Opened.Enqueue(id);
            return id;
        }

        public Task TerminateAsync(string transferId, CancellationToken ct = default)
        {
            Terminated.Enqueue(transferId);
            return Task.CompletedTask;
        }

        // The hub's data server retires a transfer that ended well; the orchestrator never does.
        public Task RetireAsync(string transferId) => throw new NotSupportedException();
    }

    private sealed class Harness
    {
        private readonly ReplicaRegistry _registry = new();
        private readonly InMemoryStateStore _state = new();

        public Harness()
        {
            var admission = new ReplicaAdmission(_registry, _state, new AdmissionOptions { AdmitUnboundFragments = true });
            Orchestrator = new RunOrchestrator(admission, _registry, Link, Transfers, Transfers, NullLogger<RunOrchestrator>.Instance, new RunOrchestratorOptions
            {
                AdmissionTimeout = TimeSpan.FromSeconds(2),
                AdmissionPollInterval = TimeSpan.FromMilliseconds(20),
                ReadyTimeout = TimeSpan.FromSeconds(2),
                ExitTimeout = TimeSpan.FromMilliseconds(400),
                RemoteFailureGrace = TimeSpan.FromMilliseconds(400),
                LinkTimeout = TimeSpan.FromMilliseconds(400),
            });
            Link.Orchestrator = Orchestrator;
        }

        public ScriptedLink Link { get; } = new();
        public ScriptedTransfers Transfers { get; } = new();
        public RunOrchestrator Orchestrator { get; }

        /// <summary>One replica per node, its TransportR client registered like a node's.</summary>
        public async Task<Harness> WithNodesAsync(params string[] nodes)
        {
            foreach (var node in nodes)
            {
                var client = Guid.NewGuid();
                await _state.SaveClientAsync(new ClientRegistration(client, "data-" + node, ["test"], null, null, DateTime.UtcNow, Subject: node));
                var announcement = new NodeAnnouncement(
                    node, "", [], NodeRole.Runner, [], InstanceId: Guid.NewGuid(), DataPlaneClientId: client);
                Link.Replicas[node] = _registry.Announce("connection-" + node, node, announcement, ["test"], () => { });
            }
            return this;
        }

        /// <summary>The run <c>p.out -> c.in</c>: two fragments of a job written as such, one on each node of the same name.</summary>
        public static RunSpec Pair(string runId = "run-1") => new(
            runId,
            [Fragment("p"), Fragment("c")],
            [new RunEdge("p", "out", "c", "in")]);

        private static RunFragment Fragment(string name) =>
            new(new FragmentSpec(name, "v1", [], Yaml: "x:\n  input: generate:1\n"), name);

        public async Task UntilAsync(Func<bool> condition, string what)
        {
            var deadline = DateTime.UtcNow + Wait;
            while (!condition())
            {
                if (DateTime.UtcNow > deadline) throw new TimeoutException(what);
                await Task.Delay(10);
            }
        }
    }

    /// <summary>
    /// A fragment whose every edge is wired reports the consequence of a torn-down transfer after a cancel, and that
    /// consequence is part of the verdict; a fragment not fully wired would be left out of it.
    /// </summary>
    [Fact]
    public async Task FullyWiredFragments_AreTheConsequencesOfACancel_NotExcludedFromTheVerdict()
    {
        var harness = await new Harness().WithNodesAsync("p", "c");
        harness.Link.OnCancel = fragment =>
        {
            _ = Task.Run(() => harness.Link.ExitAsync(fragment, 1, "Remote"));
            return Task.FromResult(true);
        };
        using var cancel = new CancellationTokenSource();
        var run = harness.Orchestrator.RunAsync(Harness.Pair(), cancel.Token);
        await harness.UntilAsync(() => harness.Link.Wires.Count == 2, "both ends of the edge to be wired");

        cancel.Cancel();
        var result = await run.WaitAsync(Wait);

        Assert.Equal(RunOutcome.Cancelled, result.Outcome);
        Assert.Equal(["c", "p"], result.Consequences.Order());
        Assert.Equal(harness.Transfers.Opened.ToArray(), harness.Transfers.Terminated.ToArray());
    }

    /// <summary>The replica answers that it knows no such edge: the edge is the cause, as a message, and nobody is named unresponsive.</summary>
    [Fact]
    public async Task AReplicaThatKnowsNoSuchEdge_MakesTheEdgeTheCause()
    {
        var harness = await new Harness().WithNodesAsync("p", "c");
        harness.Link.OnWire = fragment => Task.FromResult(fragment != "c");

        var result = await harness.Orchestrator.RunAsync(Harness.Pair()).WaitAsync(Wait);

        Assert.Equal(RunOutcome.Failed, result.Outcome);
        Assert.Equal("transfer p -> c could not be opened by the hub", result.Cause);
        Assert.False(result.CauseIsUnresponsive);
        Assert.Equal(harness.Transfers.Opened.ToArray(), harness.Transfers.Terminated.ToArray());
    }

    /// <summary>The hub cannot open the transfer: the same cause.</summary>
    [Fact]
    public async Task ATransferTheHubCannotOpen_MakesTheEdgeTheCause()
    {
        var harness = await new Harness().WithNodesAsync("p", "c");
        harness.Transfers.OnInit = () => throw new InvalidOperationException("unauthorized");

        var result = await harness.Orchestrator.RunAsync(Harness.Pair()).WaitAsync(Wait);

        Assert.Equal("transfer p -> c could not be opened by the hub", result.Cause);
        Assert.False(result.CauseIsUnresponsive);
    }

    /// <summary>A replica that cannot be reached while its edge is wired is the cause, gone quiet, and is not told to cancel.</summary>
    [Fact]
    public async Task AReplicaUnreachableWhileWiring_IsTheCauseAsUnresponsive_AndIsNotToldToCancel()
    {
        var harness = await new Harness().WithNodesAsync("p", "c");
        harness.Link.OnWire = fragment => fragment == "c" ? throw new ReplicaUnreachableException(harness.Link.Replicas["c"]) : Task.FromResult(true);

        var result = await harness.Orchestrator.RunAsync(Harness.Pair()).WaitAsync(Wait);

        Assert.Equal(RunOutcome.Failed, result.Outcome);
        Assert.Equal("c", result.Cause);
        Assert.True(result.CauseIsUnresponsive);
        Assert.Contains("p", harness.Link.Cancels);
        Assert.DoesNotContain("c", harness.Link.Cancels);
    }

    /// <summary>A replica that does not answer <c>Wire</c> within the link bound is a failed edge, not a hang.</summary>
    [Fact]
    public async Task AReplicaThatDoesNotAnswerWire_IsAFailedEdgeAfterTheLinkBound()
    {
        var harness = await new Harness().WithNodesAsync("p", "c");
        harness.Link.OnWire = _ => new TaskCompletionSource<bool>().Task;

        var result = await harness.Orchestrator.RunAsync(Harness.Pair()).WaitAsync(Wait);

        Assert.Equal("transfer p -> c could not be opened by the hub", result.Cause);
    }

    /// <summary>
    /// A fragment fails remotely and its peer never reports: once the grace ran out the run is torn down, the peer is told
    /// to cancel, and the verdict names the peer, not the fragment that failed because of it, even though the peer then reports.
    /// </summary>
    [Fact]
    public async Task APeerSilentAfterARemoteFailure_IsToldToCancelAfterTheGrace_AndNamedNotTheFragmentThatFailedBecauseOfIt()
    {
        var harness = await new Harness().WithNodesAsync("p", "c");
        harness.Link.OnCancel = fragment =>
        {
            _ = Task.Run(() => harness.Link.ExitAsync(fragment, 1, "Remote"));
            return Task.FromResult(true);
        };
        var run = harness.Orchestrator.RunAsync(Harness.Pair());
        await harness.UntilAsync(() => harness.Link.Wires.Count == 2, "both ends of the edge to be wired");

        await harness.Link.ExitAsync("p", 1, "Remote");
        var result = await run.WaitAsync(Wait);

        Assert.Equal(RunOutcome.Failed, result.Outcome);
        Assert.Equal("c", result.Cause);
        Assert.True(result.CauseIsUnresponsive);
        Assert.Contains("c", harness.Link.Cancels);
    }

    /// <summary>
    /// The origin word a failed fragment reports: only the exact word <c>Remote</c> makes its fault remote, any other
    /// word a fault of the fragment itself, and a fragment that exited 0 has no origin whatever it says.
    /// </summary>
    [Theory]
    [InlineData("Local", FaultOrigin.Local)]
    [InlineData("Remote", FaultOrigin.Remote)]
    [InlineData("remote", FaultOrigin.Local)]
    [InlineData("unknown", FaultOrigin.Local)]
    [InlineData("", FaultOrigin.Local)]
    public async Task AnOriginWord_ReadsAsRemoteOnlyWhenItIsTheWordRemote(string word, FaultOrigin expected)
    {
        var harness = await new Harness().WithNodesAsync("p", "c");
        var run = harness.Orchestrator.RunAsync(Harness.Pair());
        await harness.UntilAsync(() => harness.Link.Wires.Count == 2, "both ends of the edge to be wired");

        await harness.Link.ExitAsync("p", 1, word);
        await harness.Link.ExitAsync("c", 0, "Remote");
        var result = await run.WaitAsync(Wait);

        Assert.Equal(expected, result.Reports["p"].Origin);
        Assert.Null(result.Reports["c"].Origin);
    }

    /// <summary>
    /// A word that is not exactly <c>Remote</c> is a local fault, a number and a word with blanks around it included: the
    /// origin is read from the two words the wire has, never parsed as the name of an enumeration.
    /// </summary>
    [Theory]
    [InlineData("1")]
    [InlineData("2")]
    [InlineData(" Remote")]
    [InlineData("Local, Remote")]
    public async Task AnOriginWordThatIsNotExactlyRemote_ReadsAsLocal(string word)
    {
        var harness = await new Harness().WithNodesAsync("p", "c");
        var run = harness.Orchestrator.RunAsync(Harness.Pair());
        await harness.UntilAsync(() => harness.Link.Wires.Count == 2, "both ends of the edge to be wired");

        await harness.Link.ExitAsync("p", 1, word);
        await harness.Link.ExitAsync("c", 0);
        var result = await run.WaitAsync(Wait);

        Assert.Equal(FaultOrigin.Local, result.Reports["p"].Origin);
    }

    /// <summary>A run in which nothing failed has no clock: a peer that takes longer than the grace is never torn down for it.</summary>
    [Fact]
    public async Task ARunInWhichNothingFailed_IsNotBoundedByTheGrace()
    {
        var harness = await new Harness().WithNodesAsync("p", "c");
        var run = harness.Orchestrator.RunAsync(Harness.Pair());
        await harness.UntilAsync(() => harness.Link.Wires.Count == 2, "both ends of the edge to be wired");

        await Task.Delay(1200);
        Assert.False(run.IsCompleted);
        Assert.Empty(harness.Link.Cancels);

        await harness.Link.ExitAsync("p", 0, rowCounts: new Dictionary<string, long> { ["out"] = 5 });
        await harness.Link.ExitAsync("c", 0, rowCounts: new Dictionary<string, long> { ["in"] = 5 });
        Assert.Equal(RunOutcome.Succeeded, (await run.WaitAsync(Wait)).Outcome);
    }

    /// <summary>The requester cancels while the run waits for a replica: the run is cancelled and nothing was launched.</summary>
    [Fact]
    public async Task ACancelDuringAdmission_EndsCancelled_WithNothingLaunched()
    {
        var harness = await new Harness().WithNodesAsync("p");
        using var cancel = new CancellationTokenSource();
        var run = harness.Orchestrator.RunAsync(Harness.Pair(), cancel.Token);
        await Task.Delay(150);

        cancel.Cancel();
        var result = await run.WaitAsync(Wait);

        Assert.Equal(RunOutcome.Cancelled, result.Outcome);
        Assert.Null(result.Cause);
        Assert.Empty(harness.Link.Launches);
    }
}
