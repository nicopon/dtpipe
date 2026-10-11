using DtPipe.Coordinator;
using DtPipe.Coordinator.Contracts;
using DtPipe.Coordinator.Tests.Infrastructure;
using DtPipe.PipelineNode.Hosting;
using Microsoft.AspNetCore.SignalR.Client;
using Microsoft.Extensions.Logging.Abstractions;

namespace DtPipe.PipelineNode.Tests.Infrastructure;

/// <summary>
/// One replica of a node, as a host runs it: a TransportR client, the run protocol (<see cref="ReplicaRuns"/>, the
/// class the host's agent uses) answering on the node channel, and an announcement with the client it moves bytes with.
/// It is a runner, so it runs the job a run's fragment carries. It names the fragment it was made for, which a test
/// reads while the fragment runs; once the fragment is reported it is gone from the replica, and <see cref="Completion"/>
/// resolves.
/// </summary>
internal sealed class TestNode : IAsyncDisposable
{
    private readonly string _dir = Directory.CreateTempSubdirectory("test-node-").FullName;
    private readonly HubConnection _channel;
    private readonly ReplicaRuns _runs;
    private readonly TaskCompletionSource _completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private FragmentRun? _last;

    private TestNode(string name, PipelineNode node, HubConnection channel, ReplicaRuns runs, Guid instance, RunFragment fragment)
    {
        (Name, Node, _channel, _runs, InstanceId, Fragment) = (name, node, channel, runs, instance, fragment);
        runs.Idle += () => _completion.TrySetResult();
    }

    public string Name { get; }
    public PipelineNode Node { get; }
    public Guid InstanceId { get; }

    /// <summary>The fragment this node is the replica of, as a run names it.</summary>
    public RunFragment Fragment { get; }

    public ReplicaKey Key => new(Name, InstanceId.ToString("N"));

    /// <summary>Resolves once the fragment has been reported and is gone from the replica.</summary>
    public Task Completion => _completion.Task;

    private FragmentRun? Run => _last = _runs.Find(Name) ?? _last;

    public bool IsLaunched => Run is { IsLaunched: true };
    public int ChildProcessId => Run!.ChildProcessId;

    /// <summary>True once the child is gone; a run the replica has thrown away took its child with it.</summary>
    public bool ChildHasExited
    {
        get
        {
            try { return Run!.ChildHasExited; }
            catch (InvalidOperationException) { return true; }
        }
    }

    public Guid ClientId => Node.ClientId;
    public FaultOrigin? Origin => Run?.Origin;

    /// <summary>A fragment for the node <paramref name="name"/>: the job at <paramref name="jobPath"/> and the edges it declares.</summary>
    public static RunFragment FragmentOf(string name, string jobPath, params EdgeBinding[] edges)
    {
        var yaml = File.ReadAllText(jobPath);
        return new RunFragment(
            new FragmentSpec(name, FragmentJob.VersionOf(yaml),
                edges.Select(e => new FragmentEdge(e.Alias, e.Direction == EdgeDirection.Inbound ? FragmentEdgeDirection.Inbound : FragmentEdgeDirection.Outbound)).ToList(),
                Yaml: yaml),
            Node: name);
    }

    public static async Task<TestNode> StartAsync(
        CoordinatorTestHost host, string name, string jobPath, EdgeBinding[] edges, TimeSpan? unwiredTimeout = null, Guid? instance = null)
    {
        var node = await PipelineNode.ConnectAsync(new NodeClientOptions { HubUrl = host.Url });
        var channel = host.CreateNodeChannel();
        var id = instance ?? Guid.NewGuid();
        var dir = Directory.CreateTempSubdirectory("test-node-runs-").FullName;
        var runs = new ReplicaRuns(
            channel, node, name, DtPipeExecutableLocator.Path, new Dictionary<string, string>(), dir,
            spec => FragmentIntake.JobOf(spec, FragmentPolicy.AnyFragment, null, (_, _) => null, []),
            _ => NullLogger.Instance, NullLogger.Instance, unwiredTimeout ?? TimeSpan.FromMinutes(5));
        runs.Bind();
        var test = new TestNode(name, node, channel, runs, id, FragmentOf(name, jobPath, edges));
        await channel.StartAsync();
        await channel.InvokeAsync(NodeChannel.Announce, new NodeAnnouncement(
            name, "", [], NodeRole.Runner, [], Capabilities: [NodeAnnouncement.LaunchToken], InstanceId: id, Origin: "test", DataPlaneClientId: node.ClientId));
        await runs.OnAnnouncedAsync();
        return test;
    }

    /// <summary>The replica's node channel drops, without a word: the coordinator keeps the replica for its grace period.</summary>
    public Task StopChannelAsync() => _channel.StopAsync();

    /// <summary>The same host connects its channel again and announces itself, as an agent does after a cut.</summary>
    public async Task AnnounceAgainAsync()
    {
        await _channel.StartAsync();
        await _channel.InvokeAsync(NodeChannel.Announce, new NodeAnnouncement(
            Name, "", [], NodeRole.Runner, [], Capabilities: [NodeAnnouncement.LaunchToken], InstanceId: InstanceId, Origin: "test", DataPlaneClientId: Node.ClientId));
        await _runs.OnAnnouncedAsync();
    }

    /// <summary>The whole replica goes: it says so, its runs are thrown away, and its connections close.</summary>
    public async ValueTask DisposeAsync()
    {
        try
        {
            using var bounded = new CancellationTokenSource(TimeSpan.FromSeconds(2));
            if (_channel.State == HubConnectionState.Connected) await _channel.InvokeAsync(NodeChannel.Leave, bounded.Token);
        }
        catch (Exception) { /* the coordinator covers a replica that cannot say so */ }
        await _runs.DisposeAsync();
        await Node.DisposeAsync();
        try { await _channel.DisposeAsync(); } catch (Exception) { /* closed */ }
        try { Directory.Delete(_dir, recursive: true); } catch (Exception) { /* best-effort */ }
    }
}
