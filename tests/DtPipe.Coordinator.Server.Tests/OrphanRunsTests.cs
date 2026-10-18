using System.Collections.Concurrent;
using DtPipe.Coordinator.Contracts;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace DtPipe.Coordinator.Server.Tests;

public sealed class OrphanRunsTests : IDisposable
{
    /// <summary>A link that records the abandonments asked of it; the replicas in <see cref="Holders"/> hold their run, the others answer that they know none.</summary>
    internal sealed class Abandonments : INodeLink
    {
        public ConcurrentQueue<(ReplicaKey Replica, string RunId, string Fragment)> Asked { get; } = new();
        public HashSet<ReplicaKey> Holders { get; } = [];
        public int Failures;

        public Task<bool> CancelAsync(ReplicaKey replica, string runId, string fragment, CancellationToken ct)
        {
            Asked.Enqueue((replica, runId, fragment));
            if (Interlocked.Decrement(ref Failures) >= 0) throw new ReplicaUnreachableException(replica);
            return Task.FromResult(Holders.Contains(replica));
        }

        public Task<LaunchResult> LaunchAsync(ReplicaKey replica, LaunchRequest request, CancellationToken ct) => throw new NotSupportedException();
        public Task<bool> WireAsync(ReplicaKey replica, string runId, string fragment, string alias, string transferId, CancellationToken ct) => throw new NotSupportedException();
        public Task<FragmentCheck> CheckAsync(ReplicaKey replica, FragmentSpec spec, CancellationToken ct) => throw new NotSupportedException();
        public Task<int> KillAsync(ReplicaKey replica, string fragment, CancellationToken ct) => throw new NotSupportedException();
    }

    private readonly string _dir = Path.Combine(Path.GetTempPath(), "orphans-" + Guid.NewGuid().ToString("N"));
    private readonly Abandonments _link = new();

    public void Dispose() => Directory.Delete(_dir, recursive: true);

    private OrphanRuns Restarted(params (string RunId, string Fragment, string Node)[] runs)
    {
        var options = new CoordinatorServerOptions { StateDir = _dir, Authority = "http://idp", Audience = "test", UiClientId = "ui", UiClientSecret = "secret" };
        var before = new RunJournal(options, NullLogger<RunJournal>.Instance);
        foreach (var (runId, fragment, node) in runs)
            before.Record(new RunView(runId, "plan", RunState.Running, DateTimeOffset.UtcNow, null, null, null, null, "p", DateTimeOffset.UtcNow,
                new RunPlan("plan", null, [new RunPlanFragment(fragment, node, "v1")], [])));
        return new OrphanRuns(new RunJournal(options, NullLogger<RunJournal>.Instance), _link, NullLogger<OrphanRuns>.Instance);
    }

    private static async Task Settled(Func<bool> condition)
    {
        for (var i = 0; i < 200 && !condition(); i++) await Task.Delay(25);
    }

    private static ReplicaKey Replica(string node, string instance) => new(node, instance);

    [Fact]
    public async Task Every_Replica_Of_The_Node_Is_Asked_Once_For_Each_Fragment_The_Restart_Left_There()
    {
        var orphans = Restarted(("run-1", "p@n", "n"), ("run-2", "q@other", "other"));
        var one = Replica("sub-n", "a");
        var two = Replica("sub-n", "b");

        orphans.OnAnnounced(one, "n");
        orphans.OnAnnounced(one, "n");
        orphans.OnAnnounced(two, "n");
        await Settled(() => _link.Asked.Count >= 2);
        await Task.Delay(100);

        Assert.Equal([(one, "run-1", "p@n"), (two, "run-1", "p@n")], _link.Asked.OrderBy(a => a.Replica.Instance));
    }

    [Fact]
    public async Task A_Fragment_A_Replica_Abandoned_Is_Asked_Of_No_Other()
    {
        var orphans = Restarted(("run-1", "p@n", "n"));
        var holder = Replica("sub-n", "a");
        _link.Holders.Add(holder);

        orphans.OnAnnounced(holder, "n");
        await Settled(() => _link.Asked.Count == 1);
        await Task.Delay(100);
        orphans.OnAnnounced(Replica("sub-n", "b"), "n");
        await Task.Delay(100);

        Assert.Single(_link.Asked);
    }

    [Fact]
    public async Task A_Message_That_Did_Not_Arrive_Is_Sent_Again_At_The_Next_Announcement()
    {
        var orphans = Restarted(("run-1", "p@n", "n"));
        var replica = Replica("sub-n", "a");
        _link.Failures = 1;

        orphans.OnAnnounced(replica, "n");
        await Settled(() => _link.Asked.Count == 1);
        await Task.Delay(100);
        orphans.OnAnnounced(replica, "n");
        await Settled(() => _link.Asked.Count == 2);

        Assert.Equal(2, _link.Asked.Count);
    }

    [Fact]
    public async Task A_Coordinator_With_Nothing_Interrupted_Asks_Nobody()
    {
        var orphans = Restarted();

        orphans.OnAnnounced(Replica("sub-n", "a"), "n");
        await Task.Delay(100);

        Assert.Empty(_link.Asked);
    }
}
