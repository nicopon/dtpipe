using System.Collections.Concurrent;
using DtPipe.Coordinator.Contracts;
using Microsoft.AspNetCore.SignalR.Client;
using TransportR.Client.SignalR.Services;

namespace DtPipe.Coordinator.Tests.Infrastructure;

/// <summary>What a <see cref="FakeNode"/> announces, and how it behaves.</summary>
public sealed class FakeNodeOptions
{
    /// <summary>The bearer token a production-mode host requires; none in dev mode, where the node is keyed by its name.</summary>
    public string? Token { get; init; }
    public NodeRole Role { get; init; } = NodeRole.Runner;
    public IReadOnlyList<BrickInfo> Bricks { get; init; } = [];
    public Guid? InstanceId { get; init; }

    /// <summary>The TransportR client id announced, when it is not the node's own (a replica claiming another's).</summary>
    public Guid? AnnouncedDataPlaneClientId { get; init; }

    /// <summary>False: no TransportR client is connected, so there is none to announce.</summary>
    public bool ConnectDataPlane { get; init; } = true;

    /// <summary>False: the node connects its channel and announces nothing until the test says so.</summary>
    public bool Announce { get; init; } = true;

    /// <summary>A real node answers a <c>Cancel</c> before it is fully wired with a local exit of its killed child.</summary>
    public bool ExitOnCancel { get; init; } = true;

    /// <summary>How long after a <c>Cancel</c> the node reports that exit; none by default.</summary>
    public TimeSpan ExitOnCancelAfter { get; init; } = TimeSpan.Zero;

    /// <summary>False: the node answers a <c>Cancel</c> that it knows no such run.</summary>
    public bool KnowsTheRun { get; init; } = true;

    /// <summary>False: the node announces itself as a version that does not return a launch's token.</summary>
    public bool AnnounceLaunchToken { get; init; } = true;
}

/// <summary>
/// A replica that speaks the node channel and runs no child: it answers <c>Launch</c> as the test tells it to, notes
/// every <c>Cancel</c>, and reports <c>Exited</c> when asked. The TransportR client it announces is real, registered
/// with the coordinator's hub like a node's.
/// </summary>
public sealed class FakeNode : IAsyncDisposable
{
    private readonly FakeNodeOptions _options;

    private FakeNode(string name, HubConnection channel, SignalRDataClient<string>? dataPlane, FakeNodeOptions options)
    {
        (Name, Channel, DataPlane, _options) = (name, channel, dataPlane, options);
        InstanceId = options.InstanceId ?? Guid.NewGuid();
    }

    public string Name { get; }
    public Guid InstanceId { get; }
    public HubConnection Channel { get; }
    public SignalRDataClient<string>? DataPlane { get; }
    public TaskCompletionSource<LaunchRequest> Launched { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    public TaskCompletionSource Cancelled { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    public ConcurrentQueue<LaunchRequest> Launches { get; } = new();

    /// <summary>How the node answers a <c>Launch</c>: started, by default.</summary>
    public Func<LaunchRequest, Task<LaunchResult>> OnLaunch { get; set; } = _ => Task.FromResult(new LaunchResult(true));

    /// <summary>The run, and the fragment, of the first launch this node started.</summary>
    public string RunId => Launched.Task.Result.RunId;
    public string FragmentName => Launched.Task.Result.Spec.Fragment;

    public static async Task<FakeNode> StartAsync(CoordinatorTestHost host, string name, FakeNodeOptions? options = null)
    {
        options ??= new FakeNodeOptions();
        SignalRDataClient<string>? dataPlane = null;
        if (options.ConnectDataPlane)
        {
            dataPlane = host.CreateClient(options.Token);
            await dataPlane.ConnectAsync();
        }
        var node = new FakeNode(name, host.CreateNodeChannel(options.Token), dataPlane, options);
        node.Bind();
        await node.Channel.StartAsync();
        if (options.Announce) await node.AnnounceAsync();
        return node;
    }

    private void Bind()
    {
        Channel.On<LaunchRequest, LaunchResult>(NodeChannel.Launch, async request =>
        {
            Launches.Enqueue(request);
            var result = await OnLaunch(request);
            if (result.Started) Launched.TrySetResult(request);
            return result;
        });
        Channel.On<string, string, string, string, bool>(NodeChannel.Wire, (_, _, _, _) => Task.FromResult(true));
        Channel.On<string, string, bool>(NodeChannel.Cancel, (runId, fragment) =>
        {
            Cancelled.TrySetResult();
            if (_options.ExitOnCancel) _ = ExitOnCancelAsync(runId, fragment);
            return Task.FromResult(_options.KnowsTheRun);
        });
        Channel.On<FragmentSpec, FragmentCheck>(NodeChannel.Check, _ => Task.FromResult(new FragmentCheck(true)));
    }

    private async Task ExitOnCancelAsync(string runId, string fragment)
    {
        if (_options.ExitOnCancelAfter > TimeSpan.Zero) await Task.Delay(_options.ExitOnCancelAfter);
        await ExitAsync(1, runId: runId, fragment: fragment);
    }

    public Task AnnounceAsync() => Channel.InvokeAsync(NodeChannel.Announce, new NodeAnnouncement(
        Name, "", [], _options.Role, _options.Bricks,
        Capabilities: _options.AnnounceLaunchToken ? [NodeAnnouncement.LaunchToken] : null, DtpipeVersion: null,
        InstanceId: InstanceId, Origin: "test", DataPlaneClientId: _options.AnnouncedDataPlaneClientId ?? DataPlane?.ClientId));

    /// <summary>The token of the launch of <paramref name="fragment"/> in <paramref name="runId"/> this node received, null when it received none.</summary>
    public string? LaunchTokenOf(string runId, string fragment) =>
        Launches.FirstOrDefault(l => l.RunId == runId && l.Spec.Fragment == fragment)?.LaunchToken;

    /// <summary>
    /// Reports how the fragment of <paramref name="runId"/> ended; the answer is the coordinator's. The report carries the token
    /// of this node's own launch of that fragment, as a real node's does; <paramref name="launchToken"/> sends another, and
    /// <paramref name="withoutToken"/> none.
    /// </summary>
    public Task<ExitedAck> ExitAsync(int exitCode, string origin = "Local", string? runId = null, string? fragment = null,
        IReadOnlyDictionary<string, long>? rowCounts = null, string? launchToken = null, bool withoutToken = false)
    {
        var run = runId ?? RunId;
        var name = fragment ?? FragmentName;
        return Channel.InvokeAsync<ExitedAck>(NodeChannel.Exited, new FragmentExit(
            run, name, exitCode, origin, exitCode == 0 ? null : $"{name} failed", rowCounts ?? new Dictionary<string, long>(),
            withoutToken ? null : launchToken ?? LaunchTokenOf(run, name)));
    }

    /// <summary>The replica says it is leaving for good.</summary>
    public Task LeaveAsync() => Channel.InvokeAsync(NodeChannel.Leave);

    public async ValueTask DisposeAsync()
    {
        try { await Channel.DisposeAsync(); } catch (Exception) { /* closed */ }
        if (DataPlane is not null) await DataPlane.DisposeAsync();
    }
}

/// <summary>The fragments tests ask the orchestrator to run.</summary>
public static class TestFragments
{
    /// <summary>A fragment with no edge, written as a job, for the node of the same name.</summary>
    public static RunFragment Edgeless(string name, string? node = null, IReadOnlyList<string>? groups = null) =>
        new(new FragmentSpec(name, "v1", [], Yaml: "x:\n  input: generate:1\n"), node ?? name, groups);
}
